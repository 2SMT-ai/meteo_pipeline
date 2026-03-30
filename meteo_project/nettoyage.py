import pandas as pd
from collecte import get_meteo, extraire_champs

# Liste de villes
VILLES = ["Dakar", "Saint-Louis", "Ziguinchor", "Thiès", "Kaolack"]

# Collecte
donnees_brutes = []
for ville in VILLES:
    donnees = get_meteo(ville)
    if donnees:
        donnees_brutes.append(extraire_champs(donnees))

# Créer le DataFrame
df = pd.DataFrame(donnees_brutes)

print("--- Avant nettoyage ---")
print(df[["ville", "vent_rafale"]])

# ----- NETTOYAGE -----

# 1. Remplacer les NaN de vent_rafale par 0.0
df["vent_rafale"] = df["vent_rafale"].fillna(0.0)

# 2. Convertir collecte_le en datetime
df["collecte_le"] = pd.to_datetime(df["collecte_le"])

# 3. Mettre ville et description en minuscules normalisés
df["ville"]       = df["ville"].str.strip().str.title()
df["description"] = df["description"].str.strip().str.lower()

# ----- VERIFICATION -----
print("\n--- Après nettoyage ---")
print(df[["ville", "vent_rafale", "collecte_le"]])

print("\n--- Valeurs manquantes ---")
print(df.isnull().sum())

print("\n--- Types des colonnes ---")
print(df.dtypes)

print("\n--- Données finales prêtes pour SQL ---")
print(df.to_string(index=False))


def nettoyer(donnees_brutes):
    df = pd.DataFrame(donnees_brutes)
    df["vent_rafale"]  = df["vent_rafale"].fillna(0.0)
    df["collecte_le"]  = pd.to_datetime(df["collecte_le"])
    df["ville"]        = df["ville"].str.strip().str.title()
    df["description"]  = df["description"].str.strip().str.lower()
    return df