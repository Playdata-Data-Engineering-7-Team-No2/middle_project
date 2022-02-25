from django.urls import path
from . import views
urlpatterns = [
    path("", views.home, name="home"),
    path("hour/", views.hour, name="hour"),
    path("name/", views.name, name="name"),
    path("day/", views.day, name="day")    
    
    ]