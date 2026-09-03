from django.urls import path
from hello import views

urlpatterns = [
    path("", views.index),
    path("hello/", views.hello),
    path("<path:_>", views.index),
]
