"""Повторы временных сбоев LLM и переход к резервной модели."""

from types import SimpleNamespace

import httpx2
import pytest
import requests
from google import genai

import llm


class FakeResponse:
    def __init__(self, body, status_code=200):
        self.body = body
        self.status_code = status_code
        self.text = "error"

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)

    def json(self):
        return self.body


class FakeGeminiError(Exception):
    def __init__(self, code):
        super().__init__(f"HTTP {code}")
        self.code = code


class FakeGeminiClient:
    def __init__(self, models):
        self.models = models

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception, traceback):
        return False


def _response(model, content):
    return FakeResponse({"model": model, "choices": [{"finish_reason": "stop",
                         "message": {"content": content}}]})


def test_openrouter_retries_empty_content_twice_then_uses_next_model(monkeypatch, capsys):
    calls = []
    used_models = []
    responses = iter([_response("routed/reasoning", None) for _ in range(3)]
                     + [_response("fallback/model", '{"vacancies":[]}')])

    def fake_post(url, headers, json, timeout):
        calls.append(json["model"])
        return next(responses)

    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")
    monkeypatch.setenv("OPENROUTER_MODELS", "openrouter/auto,fallback/model")
    monkeypatch.setattr(llm.requests, "post", fake_post)
    monkeypatch.setattr(llm.time, "sleep", lambda seconds: None)

    answer = llm._chat_openrouter([{"role": "user", "content": "test"}], 0.0, 10,
                                  used_models.append)

    assert answer == '{"vacancies":[]}'
    assert calls == ["openrouter/auto"] * 3 + ["fallback/model"]
    assert used_models == ["OpenRouter: fallback/model"]
    assert "OpenRouter выбрал routed/reasoning; finish_reason=stop" in capsys.readouterr().out


def test_openrouter_only_empty_content_raises_after_three_attempts(monkeypatch):
    calls = []

    def fake_post(url, headers, json, timeout):
        calls.append(json["model"])
        return _response("routed/reasoning", None)

    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")
    monkeypatch.setenv("OPENROUTER_MODELS", "openrouter/auto")
    monkeypatch.setattr(llm.requests, "post", fake_post)
    monkeypatch.setattr(llm.time, "sleep", lambda seconds: None)

    with pytest.raises(RuntimeError, match="пустой текст от routed/reasoning"):
        llm._chat_openrouter([{"role": "user", "content": "test"}], 0.0, 10)

    assert calls == ["openrouter/auto"] * 3


def test_openrouter_permanent_400_skips_retries(monkeypatch):
    calls = []
    responses = iter([FakeResponse({}, status_code=400), _response("fallback/model", "OK")])

    def fake_post(url, headers, json, timeout):
        calls.append(json["model"])
        return next(responses)

    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")
    monkeypatch.setenv("OPENROUTER_MODELS", "openrouter/auto,fallback/model")
    monkeypatch.setattr(llm.requests, "post", fake_post)
    monkeypatch.setattr(llm.time, "sleep", lambda seconds: None)

    assert llm._chat_openrouter([{"role": "user", "content": "test"}], 0.0, 10) == "OK"
    assert calls == ["openrouter/auto", "fallback/model"]


def test_gemini_retries_503_twice_then_succeeds(monkeypatch):
    calls = []

    class FakeModels:
        def generate_content(self, model, contents, config):
            calls.append(model)
            if len(calls) < 3:
                raise FakeGeminiError(503)
            return SimpleNamespace(text="OK")

    monkeypatch.setenv("GEMINI_MODELS", "gemini/test")
    monkeypatch.setattr(genai, "Client", lambda **kwargs: FakeGeminiClient(FakeModels()))
    monkeypatch.setattr(llm.time, "sleep", lambda seconds: None)

    assert llm._chat_gemini([{"role": "user", "content": "test"}], 0.0) == "OK"
    assert calls == ["gemini/test"] * 3


def test_gemini_permanent_400_moves_to_next_model(monkeypatch):
    calls = []

    class FakeModels:
        def generate_content(self, model, contents, config):
            calls.append(model)
            if model == "gemini/blocked":
                raise FakeGeminiError(400)
            return SimpleNamespace(text="OK")

    monkeypatch.setenv("GEMINI_MODELS", "gemini/blocked,gemini/working")
    monkeypatch.setattr(genai, "Client", lambda **kwargs: FakeGeminiClient(FakeModels()))
    monkeypatch.setattr(llm.time, "sleep", lambda seconds: None)

    assert llm._chat_gemini([{"role": "user", "content": "test"}], 0.0) == "OK"
    assert calls == ["gemini/blocked", "gemini/working"]


def test_gemini_ipv4_transport_is_scoped_to_client(monkeypatch):
    client_options = []

    class FakeModels:
        def generate_content(self, model, contents, config):
            return SimpleNamespace(text="OK")

    def fake_client(**kwargs):
        client_options.append(kwargs.get("http_options"))
        return FakeGeminiClient(FakeModels())

    monkeypatch.setenv("GEMINI_MODELS", "gemini/test")
    monkeypatch.setenv("GEMINI_FORCE_IPV4", "1")
    monkeypatch.setattr(genai, "Client", fake_client)

    assert llm._chat_gemini([{"role": "user", "content": "test"}], 0.0) == "OK"
    assert len(client_options) == 1
    assert isinstance(client_options[0].httpx_client, httpx2.Client)
    assert client_options[0].httpx_client.is_closed
