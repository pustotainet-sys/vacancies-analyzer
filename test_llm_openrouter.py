"""Обработка пустых текстовых ответов OpenRouter."""

import pytest

import llm


class FakeResponse:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


def test_openrouter_empty_content_uses_next_model(monkeypatch, capsys):
    calls = []
    bodies = iter([
        {"model": "routed/reasoning", "choices": [{"finish_reason": "length",
         "message": {"content": None, "reasoning": "thoughts"}}]},
        {"model": "fallback/model", "choices": [{"finish_reason": "stop",
         "message": {"content": '{"vacancies":[]}'}}]},
    ])

    def fake_post(url, headers, json, timeout):
        calls.append(json["model"])
        return FakeResponse(next(bodies))

    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")
    monkeypatch.setenv("OPENROUTER_MODELS", "openrouter/auto,fallback/model")
    monkeypatch.setattr(llm.requests, "post", fake_post)

    answer = llm._chat_openrouter([{"role": "user", "content": "test"}], 0.0, 10)

    assert answer == '{"vacancies":[]}'
    assert calls == ["openrouter/auto", "fallback/model"]
    assert "OpenRouter выбрал routed/reasoning; finish_reason=length" in capsys.readouterr().out


def test_openrouter_only_empty_content_raises(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")
    monkeypatch.setenv("OPENROUTER_MODELS", "openrouter/auto")
    monkeypatch.setattr(
        llm.requests, "post",
        lambda *args, **kwargs: FakeResponse({
            "model": "routed/reasoning",
            "choices": [{"finish_reason": "stop", "message": {"content": None}}],
        }),
    )

    with pytest.raises(RuntimeError, match="пустой текст от routed/reasoning"):
        llm._chat_openrouter([{"role": "user", "content": "test"}], 0.0, 10)
