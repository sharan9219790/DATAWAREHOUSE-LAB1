# webserver_config.py

from os import environ

# Use the env var you already set so the key is stable across restarts
SECRET_KEY = environ.get("AIRFLOW__WEBSERVER__SECRET_KEY") or "dev_only_do_not_use_in_prod"

# Keep CSRF on, but make cookies compatible with localhost (http)
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None  # no expiry during the session

# Cookies tuned for localhost
SESSION_COOKIE_SAMESITE = "Lax"   # works well on localhost
SESSION_COOKIE_SECURE = False     # because you’re on http://localhost
SESSION_COOKIE_HTTPONLY = True

# Optional: name isolation (avoids clashes with any other local FAB apps)
SESSION_COOKIE_NAME = "airflow_session"

