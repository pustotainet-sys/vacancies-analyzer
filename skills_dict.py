"""Канонический словарь технологий: синоним → каноническое имя.

Источник истины — курируемый SEED_SKILLS. Рост по данным идёт через таблицу
learned_aliases в storage: наблюдённые навыки апсертятся как 'pending', человек
ревьюит и переносит сюда. merged_with_learned() накладывает промоутнутые алиасы
поверх сида, чтобы они действовали сразу.
"""

# каноническое имя (в нужном регистре) -> список алиасов (форм для матчинга, нижний регистр)
SEED_SKILLS = {
    # --- языки ---
    "Python": ["python", "питон", "пайтон"],
    "JavaScript": ["javascript", "js", "джаваскрипт"],
    "TypeScript": ["typescript", "ts", "тайпскрипт"],
    "Java": ["java", "джава"],
    "Kotlin": ["kotlin", "котлин"],
    "Go": ["golang", "go"],
    "Rust": ["rust", "раст"],
    "C++": ["c++", "cpp", "си++", "с++"],
    "C#": ["c#", "c sharp", "csharp", "си шарп"],
    "C": ["c"],
    "PHP": ["php", "пхп"],
    "Ruby": ["ruby", "руби"],
    "Swift": ["swift", "свифт"],
    "Objective-C": ["objective-c", "objective c", "objc"],
    "Scala": ["scala", "скала"],
    "R": ["r"],
    "Perl": ["perl", "перл"],
    "Elixir": ["elixir"],
    "Dart": ["dart"],
    "Bash": ["bash", "shell", "шелл"],
    "SQL": ["sql", "скуль"],
    "1С": ["1c", "1с", "1c-битрикс", "1с-битрикс"],
    # --- python-фреймворки/либы ---
    "Django": ["django", "джанго"],
    "Flask": ["flask", "фласк"],
    "FastAPI": ["fastapi", "фастапи"],
    "aiohttp": ["aiohttp"],
    "SQLAlchemy": ["sqlalchemy", "sqlalchemy", "алхимия"],
    "Celery": ["celery", "селери"],
    "Pandas": ["pandas", "пандас"],
    "NumPy": ["numpy", "нампай"],
    "PyTorch": ["pytorch", "торч"],
    "TensorFlow": ["tensorflow", "тензорфлоу"],
    "scikit-learn": ["scikit-learn", "sklearn", "scikit learn"],
    "Pytest": ["pytest", "пайтест"],
    "Pydantic": ["pydantic", "пайдантик"],
    "asyncio": ["asyncio", "асинкио"],
    "aiogram": ["aiogram", "аиограм"],
    # --- фронтенд ---
    "React": ["react", "react.js", "reactjs", "реакт"],
    "Vue.js": ["vue", "vue.js", "vuejs", "вью"],
    "Angular": ["angular", "ангуляр"],
    "Node.js": ["node.js", "nodejs", "node js", "нода", "node"],
    "Next.js": ["next.js", "nextjs", "next js"],
    "Redux": ["redux", "редакс"],
    "HTML": ["html"],
    "CSS": ["css"],
    "SASS": ["sass", "scss"],
    "Webpack": ["webpack", "вебпак"],
    # --- бэкенд/прочие фреймворки ---
    "Spring": ["spring", "spring boot", "спринг"],
    ".NET": [".net", "asp.net", "dotnet", "asp.net core", "дотнет"],
    "Laravel": ["laravel", "ларавел"],
    "Symfony": ["symfony", "симфони"],
    "Ruby on Rails": ["ruby on rails", "rails", "рельсы"],
    "GraphQL": ["graphql", "графкуэль"],
    "gRPC": ["grpc"],
    "REST": ["rest", "rest api", "restful"],
    # --- БД ---
    "PostgreSQL": ["postgresql", "postgres", "постгрес", "pg", "psql", "постгрескл"],
    "MySQL": ["mysql", "майскуль"],
    "SQLite": ["sqlite"],
    "MongoDB": ["mongodb", "mongo", "монго"],
    "Redis": ["redis", "редис"],
    "Elasticsearch": ["elasticsearch", "elastic", "эластик"],
    "ClickHouse": ["clickhouse", "кликхаус"],
    "Cassandra": ["cassandra", "кассандра"],
    "Oracle": ["oracle", "оракл"],
    "MS SQL": ["ms sql", "mssql", "sql server", "microsoft sql"],
    "Kafka": ["kafka", "кафка"],
    "RabbitMQ": ["rabbitmq", "rabbit", "кролик"],
    # --- devops/облака/инфра ---
    "Docker": ["docker", "докер"],
    "Kubernetes": ["kubernetes", "k8s", "кубер", "кубернетес"],
    "Linux": ["linux", "линукс"],
    "Nginx": ["nginx", "нжинкс", "энджинкс"],
    "Git": ["git", "гит"],
    "CI/CD": ["ci/cd", "cicd", "ci cd"],
    "GitLab": ["gitlab", "гитлаб"],
    "Jenkins": ["jenkins", "дженкинс"],
    "Ansible": ["ansible", "ансибл"],
    "Terraform": ["terraform", "терраформ"],
    "AWS": ["aws", "amazon web services"],
    "Kubernetes Helm": ["helm"],
    "Prometheus": ["prometheus", "прометей"],
    "Grafana": ["grafana", "графана"],
    # --- инструменты/практики ---
    "Airflow": ["airflow", "эйрфлоу"],
    "Spark": ["spark", "apache spark", "спарк"],
    "Hadoop": ["hadoop", "хадуп"],
    "Selenium": ["selenium", "селениум"],
    "OpenCV": ["opencv"],
}

# токены, слишком общие для матчинга по свободному тексту: их считаем только
# при точном совпадении со структурным тегом key_skills.
AMBIGUOUS = {"go", "r", "c", "с"}

# алиасы со спецсимволами, где обычная граница \b работает некорректно —
# для них build_matcher строит отдельные регэкспы с классом границ [\w+#.].
SPECIAL_CHAR_ALIASES = {
    "c++", "с++", "си++", "c#", "си шарп", ".net", "asp.net", "asp.net core",
    "node.js", "next.js", "react.js", "vue.js", "ci/cd", "objective-c",
    "scikit-learn", "ms sql",
}


def alias_to_canonical(skill_map=None):
    """Плоская обратная карта alias(lower) -> каноническое имя.

    При коллизии алиаса побеждает первый встреченный (сид упорядочен осмысленно).
    """
    skill_map = skill_map if skill_map is not None else SEED_SKILLS
    rev = {}
    for canon, aliases in skill_map.items():
        for a in aliases:
            rev.setdefault(a.lower(), canon)
    return rev


def all_canonical(skill_map=None):
    skill_map = skill_map if skill_map is not None else SEED_SKILLS
    return set(skill_map.keys())


def is_ambiguous(alias):
    return alias.lower() in AMBIGUOUS


def merged_with_learned(learned):
    """Наложить промоутнутые learned-алиасы (alias_lower -> canonical) на сид.

    Возвращает новую карту canonical -> [aliases]. Алиасы, чей canonical уже есть
    в сиде, дописываются к нему; новые канонические имена добавляются отдельно.
    """
    merged = {canon: list(aliases) for canon, aliases in SEED_SKILLS.items()}
    for alias, canon in (learned or {}).items():
        if not canon:
            continue
        merged.setdefault(canon, [])
        if alias.lower() not in (a.lower() for a in merged[canon]):
            merged[canon].append(alias.lower())
    return merged
