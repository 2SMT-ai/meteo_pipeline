import sqlite3
import os
from collecte import get_meteo, extraire_champs
from nettoyage import nettoyer

DB_PATH = os.path.join(os.path.dirname(__file__), "meteo.db")

VILLES = ["Dakar", "Saint-Louis", "Ziguinchor", "Thiès", "Kaolack"]

def inserer_ville(curseur, meteo):
    curseur.execute("""
        INSERT OR IGNORE INTO villes (nom, pays, latitude, longitude)
        VALUES (?, ?, ?, ?)
    """, (meteo["ville"], meteo["pays"], meteo["latitude"], meteo["longitude"]))

    curseur.execute("SELECT id FROM villes WHERE nom = ?", (meteo["ville"],))
    return curseur.fetchone()[0]

def inserer_mesure(curseur, ville_id, meteo):
    curseur.execute("""
        INSERT INTO mesures_meteo (
            ville_id, temperature, ressenti, temp_min, temp_max,
            humidite, pression, vent_vitesse, vent_direction,
            vent_rafale, description, collecte_le
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        ville_id,
        meteo["temperature"],
        meteo["ressenti"],
        meteo["temp_min"],
        meteo["temp_max"],
        meteo["humidite"],
        meteo["pression"],
        meteo["vent_vitesse"],
        meteo["vent_direction"],
        meteo["vent_rafale"],
        meteo["description"],
        meteo["collecte_le"]
    ))

def pipeline():
    connexion = sqlite3.connect(DB_PATH)
    curseur = connexion.cursor()
    curseur.execute("PRAGMA foreign_keys = ON")

    print("--- Démarrage du pipeline ---\n")
    succes = 0

    for ville in VILLES:
        donnees = get_meteo(ville)
        if donnees:
            meteo = extraire_champs(donnees)

            # Nettoyage manuel (vent_rafale)
            if meteo["vent_rafale"] is None:
                meteo["vent_rafale"] = 0.0

            ville_id = inserer_ville(curseur, meteo)
            inserer_mesure(curseur, ville_id, meteo)
            print(f"✅ {ville:<15} insérée (ville_id={ville_id})")
            succes += 1

    connexion.commit()
    connexion.close()
    print(f"\n--- {succes}/{len(VILLES)} villes insérées avec succès ---")

def verifier_insertion():
    connexion = sqlite3.connect(DB_PATH)
    curseur = connexion.cursor()

    print("\n--- Contenu de la table villes ---")
    curseur.execute("SELECT * FROM villes")
    for row in curseur.fetchall():
        print(row)

    print("\n--- Contenu de la table mesures_meteo ---")
    curseur.execute("""
        SELECT v.nom, m.temperature, m.humidite,
               m.vent_vitesse, m.description, m.collecte_le
        FROM mesures_meteo m
        JOIN villes v ON m.ville_id = v.id
    """)
    for row in curseur.fetchall():
        print(row)

    connexion.close()

if __name__ == "__main__":
    pipeline()
    verifier_insertion()