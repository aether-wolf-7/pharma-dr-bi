"""
Pharma DR · Apache Superset Configuration
==========================================
Production-ready Superset config with:
  - PostgreSQL metadata DB
  - Redis cache (5 min operational, 1 h historical)
  - Celery async queries
  - RBAC + Row-Level Security
  - Performance tuning
"""

import os
from datetime import timedelta
from cachelib.redis import RedisCache

# ── Core Settings ─────────────────────────────────────────────
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "CHANGE_ME_in_production_very_long_key")
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://pharma_admin:password@pgbouncer:6432/pharma_dr"
)

# ── Security ──────────────────────────────────────────────────
WTF_CSRF_ENABLED               = True
SESSION_COOKIE_HTTPONLY        = True
SESSION_COOKIE_SECURE          = True   # Requires HTTPS
SESSION_COOKIE_SAMESITE        = "Lax"
TALISMAN_ENABLED               = True
TALISMAN_CONFIG                = {
    "content_security_policy": {
        "default-src": ["'self'"],
        "img-src":     ["'self'", "data:", "https:"],
        "script-src":  ["'self'", "'unsafe-inline'", "'unsafe-eval'"],
        "style-src":   ["'self'", "'unsafe-inline'"],
        "font-src":    ["'self'", "data:"],
    }
}

# ── Authentication ────────────────────────────────────────────
AUTH_TYPE = 1   # AUTH_DB (username/password via Superset internal DB)
# For SSO: AUTH_TYPE = 3  # AUTH_OAUTH

# Session timeout: 8 hours
PERMANENT_SESSION_LIFETIME = timedelta(hours=8)

# ── Redis Cache ───────────────────────────────────────────────
REDIS_HOST     = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT     = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")

REDIS_URL = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}"

# Query result cache: 5 min for operational data, 1 h for historical
CACHE_CONFIG = {
    "CACHE_TYPE":              "RedisCache",
    "CACHE_DEFAULT_TIMEOUT":   300,      # 5 minutes default
    "CACHE_KEY_PREFIX":        "pharmadr_superset_",
    "CACHE_REDIS_URL":         f"{REDIS_URL}/0",
}

# Filter-state cache (used by native filter bar)
FILTER_STATE_CACHE_CONFIG = {
    "CACHE_TYPE":    "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 3600,
    "CACHE_KEY_PREFIX": "pharmadr_filter_",
    "CACHE_REDIS_URL":   f"{REDIS_URL}/3",
}

# Explore (chart) form data cache
EXPLORE_FORM_DATA_CACHE_CONFIG = {
    "CACHE_TYPE":    "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 3600,
    "CACHE_KEY_PREFIX": "pharmadr_explore_",
    "CACHE_REDIS_URL":   f"{REDIS_URL}/4",
}

# ── Celery (Async Queries + Alerts) ───────────────────────────
class CeleryConfig:
    broker_url          = f"{REDIS_URL}/1"
    result_backend      = f"{REDIS_URL}/2"
    worker_prefetch_multiplier = 1
    task_acks_late      = False
    beat_schedule       = {
        "reports.scheduler": {
            "task":     "superset.tasks.scheduler.scheduler",
            "schedule": 10,
        },
        "reports.prune_log": {
            "task":     "superset.tasks.scheduler.prune_log",
            "schedule": timedelta(days=1),
        },
    }

CELERY_CONFIG = CeleryConfig

# ── Feature Flags ─────────────────────────────────────────────
FEATURE_FLAGS = {
    "ALERT_REPORTS":           True,    # Email/Slack alerts
    "DASHBOARD_NATIVE_FILTERS":True,    # Native cross-filter
    "DASHBOARD_CROSS_FILTERS": True,    # Cross-filter drill-down
    "DRILL_TO_DETAIL":         True,    # Drill-down to record level
    "DRILL_BY":                True,    # Drill-by dimension
    "GLOBAL_ASYNC_QUERIES":    True,    # Async query execution
    "THUMBNAILS":              True,    # Dashboard thumbnails
    "LISTVIEWS_DEFAULT_CARD_VIEW": True,
    "ENABLE_TEMPLATE_PROCESSING": True, # Jinja2 in SQL queries (for RLS)
    "ROW_LEVEL_SECURITY":      True,    # Fine-grained RLS
    "TAGGING_SYSTEM":          True,
    "HORIZONTAL_FILTER_BAR":   False,   # Use vertical for more space
    "VERSIONED_EXPORT":        True,
}

# ── SQL Lab Settings ──────────────────────────────────────────
SUPERSET_WEBSERVER_PORT         = 8088
SQL_MAX_ROW                     = 100000
SQL_QUERY_MUTATE_AFTER_HOOKS    = []
DISPLAY_MAX_ROW                 = 10000
SQLALCHEMY_POOL_SIZE            = 5
SQLALCHEMY_POOL_TIMEOUT         = 300
SQLALCHEMY_MAX_OVERFLOW         = 25
SQLALCHEMY_POOL_PRE_PING        = True

# Max async query time (seconds)
SQLLAB_ASYNC_TIME_LIMIT_SEC     = 120
SQLLAB_TIMEOUT                  = 30

# ── Thumbnail & Export ────────────────────────────────────────
THUMBNAIL_SELENIUM_USER         = "admin"
THUMBNAIL_CACHE_CONFIG          = {
    "CACHE_TYPE":            "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,   # 24 hours
    "CACHE_KEY_PREFIX":      "pharmadr_thumb_",
    "CACHE_REDIS_URL":       f"{REDIS_URL}/5",
}

# ── Email (Alerts / Reports) ──────────────────────────────────
SMTP_HOST       = os.environ.get("SMTP_HOST", "smtp.company.com")
SMTP_PORT       = int(os.environ.get("SMTP_PORT", 587))
SMTP_USER       = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD   = os.environ.get("SMTP_PASSWORD", "")
SMTP_STARTTLS   = True
SMTP_SSL        = False
SMTP_MAIL_FROM  = "pharma-bi@company.com"
EMAIL_REPORTS_CRON_RESOLUTION = 5

# ── Map / Geo Settings ────────────────────────────────────────
# Mapbox token for choropleth maps (free tier available)
# MAPBOX_API_KEY = os.environ.get("MAPBOX_API_KEY", "")
# Alternative: use OpenStreetMap (no key required)

# ── Locale ───────────────────────────────────────────────────
BABEL_DEFAULT_LOCALE  = "es"
BABEL_DEFAULT_FOLDER  = "superset/translations"
DEFAULT_LOCALE        = "es"

# ── Custom CSS (Pharma DR Brand Colors) ───────────────────────
# Injected via Assets tab in Superset UI
# Primary: #1E3A5F (dark navy)
# Accent:  #2ECC71 (pharma green)
# Alert:   #E74C3C (red)

# ── Row-Level Security Configuration ─────────────────────────
# Applied via Settings → Security → Row Level Security in Superset UI
# Example RLS for zone managers:
# Table: fact_sales
# Clause: zone_key IN (SELECT zone_key FROM dw.dim_zone WHERE zone_code = '{{current_username()}}')
# Roles: Gerente_CAP, Gerente_NOR, etc.

# ── Additional Settings ───────────────────────────────────────
ENABLE_JAVASCRIPT_CONTROLS  = True
ENABLE_PROXY_FIX            = True   # Behind nginx reverse proxy
SUPERSET_LOG_LEVEL          = "WARNING"
ENABLE_ACCESS_REQUEST       = False

# Dashboard refresh intervals available to users
DASHBOARD_AUTO_REFRESH_INTERVALS = [
    [0,     "Don't refresh"],
    [10,    "10 seconds"],
    [30,    "30 seconds"],
    [60,    "1 minute"],
    [300,   "5 minutes"],
    [1800,  "30 minutes"],
    [3600,  "1 hour"],
    [21600, "6 hours"],
    [43200, "12 hours"],
    [86400, "24 hours"],
]

# Allowed query execution methods
QUERY_LOGGER = None  # Set to custom logger for full SQL audit trail
