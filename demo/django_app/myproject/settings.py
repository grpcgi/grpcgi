SECRET_KEY = "grpcgi-demo-not-for-production"
DEBUG = True
ALLOWED_HOSTS = ["*"]
INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "hello",
]
DATABASES = {}
ROOT_URLCONF = "myproject.urls"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
