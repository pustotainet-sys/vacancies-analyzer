"""Извлечение технологий из вакансий (без LLM).

Основной сигнал — структурные key_skills и полный текст description из детали
вакансии; сниппет из поиска используется как fallback. Метрика спроса —
document frequency (в скольких вакансиях встретилась технология).
"""

import math
import re
from collections import Counter
from html.parser import HTMLParser
from itertools import combinations

import skills_dict


# --- очистка HTML ------------------------------------------------------------
class _TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self._chunks = []

    def handle_data(self, data):
        self._chunks.append(data)

    def text(self):
        return " ".join(self._chunks)


def strip_html(s):
    """Убрать теги (включая <highlighttext>) и схлопнуть пробелы."""
    if not s:
        return ""
    parser = _TextExtractor()
    try:
        parser.feed(s)
        parser.close()
        text = parser.text()
    except Exception:
        text = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", text).strip()


# --- матчер ------------------------------------------------------------------
class Matcher:
    """Скомпилированный gazetteer: alnum-альтернация + отдельные спецрегэкспы."""

    def __init__(self, normal_re, special, alias_map):
        self.normal_re = normal_re      # re.Pattern | None
        self.special = special          # list[(re.Pattern, canonical)]
        self.alias_map = alias_map      # alias_lower -> canonical


def build_matcher(skill_map=None):
    skill_map = skill_map if skill_map is not None else skills_dict.SEED_SKILLS
    alias_map = skills_dict.alias_to_canonical(skill_map)

    normal_aliases, special = [], []
    for alias, canon in alias_map.items():
        low = alias.lower()
        if low in skills_dict.AMBIGUOUS:
            continue  # неоднозначные — только через key_skills, не по тексту
        if low in skills_dict.SPECIAL_CHAR_ALIASES or not re.fullmatch(r"[\w\s]+", alias):
            rx = re.compile(r"(?<![\w+#.])" + re.escape(low) + r"(?![\w+#.])")
            special.append((rx, canon))
        else:
            normal_aliases.append(low)

    # длинные алиасы раньше — иначе "node" перекроет "node js"
    normal_aliases.sort(key=len, reverse=True)
    normal_re = None
    if normal_aliases:
        # класс границ включает + # . — иначе "js" ловится внутри "node.js"
        pattern = (r"(?<![\w+#.])(?:" + "|".join(re.escape(a) for a in normal_aliases)
                   + r")(?![\w+#.])")
        normal_re = re.compile(pattern)
    # спец — тоже длинные раньше (для читаемости; на set-результат не влияет)
    special.sort(key=lambda t: len(t[0].pattern), reverse=True)
    return Matcher(normal_re, special, alias_map)


def match_text(matcher, text):
    """Множество канонических технологий, найденных в свободном тексте."""
    if not text:
        return set()
    low = text.lower()
    found = set()
    if matcher.normal_re:
        for m in matcher.normal_re.finditer(low):
            canon = matcher.alias_map.get(m.group(0))
            if canon:
                found.add(canon)
    for rx, canon in matcher.special:
        if rx.search(low):
            found.add(canon)
    return found


def match_key_skills(items, alias_map, matcher=None):
    """Технологии из структурных тегов key_skills.

    Точное совпадение тега резолвит неоднозначные (Go/R/C). Дополнительно прогоняем
    имена тегов через matcher, чтобы поймать составные вроде "Python 3".
    """
    found, names = set(), []
    for it in items or []:
        name = it.get("name") if isinstance(it, dict) else it
        if not isinstance(name, str) or not name.strip():
            continue
        names.append(name)
        canon = alias_map.get(name.strip().lower())
        if canon:
            found.add(canon)
    if matcher and names:
        found |= match_text(matcher, " . ".join(names))
    return found


def techs_for_vacancy(matcher, alias_map, v):
    """(skill_techs, text_techs) для одной вакансии.

    text-источник = description (если есть) иначе сниппет из поиска.
    """
    skill_techs, text_techs = set(), set()
    ks = v.get("key_skills")
    if ks:
        skill_techs |= match_key_skills(ks, alias_map, matcher)

    desc = v.get("description")
    if desc:
        text_techs |= match_text(matcher, strip_html(desc))
    else:
        sn = v.get("snippet") or {}
        txt = " ".join(x for x in (sn.get("requirement"), sn.get("responsibility")) if x)
        text_techs |= match_text(matcher, strip_html(txt))
    return skill_techs, text_techs


def _source_str(sources):
    if sources == {"skill", "text"}:
        return "both"
    return "skill" if sources == {"skill"} else "text"


def aggregate(vacancies, matcher, alias_map, cooccur_top_k=40):
    """Пройти корпус: DF, источники, co-occurrence, сырые key_skills.

    Возвращает dict: {n, df, source, cooccur, raw_skills}.
    """
    df = Counter()
    src = {}
    raw_skills = Counter()
    per_vac = []

    for v in vacancies:
        st, tt = techs_for_vacancy(matcher, alias_map, v)
        allt = st | tt
        df.update(allt)
        for t in allt:
            s = src.setdefault(t, set())
            if t in st:
                s.add("skill")
            if t in tt:
                s.add("text")
        per_vac.append(allt)
        for it in v.get("key_skills") or []:
            name = it.get("name") if isinstance(it, dict) else it
            if isinstance(name, str) and name.strip():
                raw_skills[name.strip()] += 1

    top = {t for t, _ in df.most_common(cooccur_top_k)}
    cooccur = Counter()
    for s in per_vac:
        for a, b in combinations(sorted(s & top), 2):
            cooccur[(a, b)] += 1

    return {
        "n": len(vacancies),
        "df": dict(df),
        "source": {t: _source_str(src[t]) for t in df},
        "cooccur": dict(cooccur),
        "raw_skills": dict(raw_skills),
    }


# --- discovery: TF-IDF по тексту (только для пополнения словаря) ---------------
STOPWORDS = {
    # доменная «вода»
    "требования", "обязанности", "компания", "команда", "опыт", "работа",
    "работы", "задачи", "условия", "график", "предлагаем", "ищем", "будет",
    "знание", "знания", "умение", "навыки", "разработка", "разработки",
    "проект", "проекта", "проектов", "продукт", "система", "системы",
    "плюсом", "будущего", "офис", "офисе", "также", "если", "который",
    "нашей", "наших", "вами", "вас", "нас", "для", "или", "как", "что",
    # общий английский
    "the", "and", "for", "with", "you", "our", "your", "will", "are", "have",
    "team", "work", "experience", "requirements", "skills", "development",
    "developer", "engineer", "job", "role", "responsibilities", "we", "to",
}
_TOKEN_RE = re.compile(r"[a-zA-Zа-яёА-ЯЁ][\w+#.\-]{1,}")


def tfidf_candidates(texts, known_aliases, top=40):
    """Кандидаты в словарь: термы с высоким tf-idf, которых нет в known_aliases.

    texts — список уже очищенных от HTML строк (по одной на вакансию).
    known_aliases — множество известных форм (нижний регистр).
    """
    docs = []
    dfreq = Counter()
    for t in texts:
        terms = {w.lower().strip(".") for w in _TOKEN_RE.findall(t or "")}
        terms = {w for w in terms if w and w not in STOPWORDS
                 and not w.isdigit() and w not in known_aliases}
        docs.append(terms)
        dfreq.update(terms)

    n = len(docs) or 1
    score = Counter()
    for terms in docs:
        for w in terms:
            score[w] += math.log(n / (1 + dfreq[w]))
    return score.most_common(top)
