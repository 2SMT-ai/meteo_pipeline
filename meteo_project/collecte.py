import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import pandas as pd

# Charger la clé depuis .env
load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")

# Paramètres
VILLE = "Dakar"
URL = "https://api.openweathermap.org/data/2.5/weather"

def get_meteo(ville):
    params = {
        "q": ville,
        "appid": API_KEY,
        "units": "metric",
        "lang": "fr"
    }
    reponse = requests.get(URL, params=params)
    if reponse.status_code == 200:
        print(f"✅ Connexion réussie pour : {ville}")
        return reponse.json()
    else:
        print(f"❌ Erreur {reponse.status_code} : {reponse.text}")
        return None

def extraire_champs(donnees):
    return {
        "ville"          : donnees["name"],
        "pays"           : donnees["sys"]["country"],
        "latitude"       : donnees["coord"]["lat"],
        "longitude"      : donnees["coord"]["lon"],
        "temperature"    : donnees["main"]["temp"],
        "ressenti"       : donnees["main"]["feels_like"],
        "temp_min"       : donnees["main"]["temp_min"],
        "temp_max"       : donnees["main"]["temp_max"],
        "humidite"       : donnees["main"]["humidity"],
        "pression"       : donnees["main"]["pressure"],
        "vent_vitesse"   : donnees["wind"]["speed"],
        "vent_direction" : donnees["wind"]["deg"],
        "vent_rafale"    : donnees["wind"].get("gust", None),
        "description"    : donnees["weather"][0]["description"],
        "collecte_le"    : datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    }

# Exécution
donnees = get_meteo(VILLE)
if donnees:
    meteo = extraire_champs(donnees)
    for cle, valeur in meteo.items():
        print(f"{cle:<20} : {valeur}")


