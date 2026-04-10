from __future__ import annotations

DEFAULT_ORIGINS = ("PEK", "TSN", "SJW")
DEFAULT_DESTINATIONS = ("NRT", "HND", "KIX", "ITM", "NGO", "FUK", "CTS", "OKA")
DEFAULT_SCAN_DAYS = 180
DEFAULT_QUALIFIED_THRESHOLD = 1200.0
DEFAULT_TIMEOUT = 20
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)
DEFAULT_BROWSER_HEADLESS = True
DEFAULT_BROWSER_EXECUTABLE_PATH = ""
DEFAULT_BROWSER_CHANNEL = ""

DEFAULT_REPORT_EXCLUDE_TODAY = True
DEFAULT_REPORT_TOP_N = 25
DEFAULT_REPORT_TITLE = ""
DEFAULT_REPORT_SCOPE_DESCRIPTION = ""
DEFAULT_REPORT_RULES_DESCRIPTION = ""
DEFAULT_REPORT_SHOW_CONNECTION_CANDIDATES = False
DEFAULT_SPRING_LIVE_WORKERS = 1
DEFAULT_PROBE_STEP_DAYS = 14
DEFAULT_SPRING_WINDOW_DAYS = 7
DEFAULT_SPRING_DATE_CLICK_THRESHOLD = 1200.0
DEFAULT_SPRING_MAX_CONSECUTIVE_EMPTY_WEEKS = 6

DEFAULT_SOURCE_FLAGS = {
    "spring_airlines": True,
    "spring_japan": False,
    "peach": False,
    "jetstar_japan": False,
}

DEFAULT_LOGGING_ENABLED = True
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_STAGE_SUMMARY = True
DEFAULT_EMAIL_ENABLED = False
DEFAULT_EMAIL_SMTP_HOST = ""
DEFAULT_EMAIL_SMTP_PORT = 587
DEFAULT_EMAIL_SMTP_USERNAME = ""
DEFAULT_EMAIL_SMTP_PASSWORD_ENV = "FARE_MONITOR_SMTP_PASSWORD"
DEFAULT_EMAIL_FROM_ADDRESS = ""
DEFAULT_EMAIL_TO_ADDRESSES: tuple[str, ...] = ()
DEFAULT_EMAIL_SUBJECT_PREFIX = "[Fare Monitor]"
DEFAULT_EMAIL_SEND_ON_SUCCESS = True
DEFAULT_EMAIL_SEND_ON_FAILURE = True
DEFAULT_EMAIL_ATTACH_REPORT_HTML = True
DEFAULT_EMAIL_ATTACH_QUALIFIED_CSV = True
DEFAULT_EMAIL_ATTACH_RUN_LOG_ON_FAILURE = True
DEFAULT_EMAIL_SMTP_USE_TLS = True
DEFAULT_EMAIL_SMTP_USE_SSL = False

NORTHERN_ORIGINS = ("PEK", "TSN", "SJW")
SHANGHAI_HUBS = ("SHA", "PVG")

AIRPORT_DISPLAY = {
    "PEK": "北京首都",
    "PKX": "北京大兴",
    "TSN": "天津滨海",
    "SJW": "石家庄正定",
    "SHA": "上海虹桥",
    "PVG": "上海浦东",
    "NRT": "东京成田",
    "HND": "东京羽田",
    "KIX": "大阪关西",
    "ITM": "大阪伊丹",
    "NGO": "名古屋中部",
    "FUK": "福冈",
    "CTS": "札幌新千岁",
    "OKA": "冲绳那霸",
    "TYO": "东京城市群",
    "OSA": "大阪城市群",
    "SPK": "札幌城市群",
}

AIRLINE_DISPLAY = {
    "spring_airlines": "春秋航空 Spring Airlines",
    "spring_japan": "SPRING JAPAN",
    "peach": "Peach Aviation",
    "jetstar_japan": "Jetstar Japan",
    "Spring Airlines": "春秋航空 Spring Airlines",
    "SPRING JAPAN": "SPRING JAPAN",
    "Peach Aviation": "Peach Aviation",
    "Jetstar Japan": "Jetstar Japan",
}

SOURCE_DISPLAY = {
    "spring_airlines": "Spring Airlines 官网",
    "spring_japan": "SPRING JAPAN 官网",
    "peach": "Peach 官网",
    "jetstar_japan": "Jetstar Japan 官网",
}

SOURCE_HOME_URL = {
    "spring_airlines": "https://en.ch.com/flights/Japan.html",
    "spring_japan": "https://jp.ch.com/",
    "peach": "https://www.flypeach.com/en/lm/st/routemap",
    "jetstar_japan": "https://www.jetstar.com/jp/en/flights/shanghai",
}

SOURCE_NOTES = {
    "spring_airlines": "Spring 国际站，当前主 live 来源。",
    "spring_japan": "SPRING JAPAN 官方入口，当前默认关闭，避免和主站重复。",
    "peach": "Peach 官方入口，已收录来源信息，但 live 验证尚未默认启用。",
    "jetstar_japan": "Jetstar Japan 官方入口，已收录来源信息，但 live 验证尚未默认启用。",
}

CITY_GROUP_BY_AIRPORT = {
    "NRT": "TYO",
    "HND": "TYO",
    "KIX": "OSA",
    "ITM": "OSA",
    "CTS": "SPK",
}

SAMPLE_SOURCE_ROUTE_KEYS = {
    "spring_airlines": (
        "PEK->KIX",
        "SHA->FUK",
        "SHA->KIX",
    ),
    "spring_japan": (
        "PEK->NRT",
        "TSN->NRT",
    ),
    "peach": (
        "PVG->KIX",
    ),
    "jetstar_japan": (
        "PVG->NRT",
        "PVG->KIX",
    ),
}

COLOR_PALETTE = (
    "#0b3954",
    "#087e8b",
    "#c81d25",
    "#f4b942",
    "#3d405b",
    "#81b29a",
    "#d1495b",
    "#4c956c",
)
