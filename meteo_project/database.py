import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "meteo.db")

def creer_base():
    connexion = sqlite3.connect(DB_PATH)
    curseur = connexion.cursor()

    # Activer les clés étrangères
    curseur.execute("PRAGMA foreign_keys = ON")

    # Table villes
    curseur.execute("""
        CREATE TABLE IF NOT EXISTS villes (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            nom       TEXT    NOT NULL UNIQUE,
            pays      TEXT    NOT NULL,
            latitude  REAL    NOT NULL,
            longitude REAL    NOT NULL
        )
    """)

    # Table mesures_meteo
    curseur.execute("""
        CREATE TABLE IF NOT EXISTS mesures_meteo (
            id             INTEGER  PRIMARY KEY AUTOINCREMENT,
            ville_id       INTEGER  NOT NULL,
            temperature    REAL,
            ressenti       REAL,
            temp_min       REAL,
            temp_max       REAL,
            humidite       INTEGER,
            pression       INTEGER,
            vent_vitesse   REAL,
            vent_direction INTEGER,
            vent_rafale    REAL,
            description    TEXT,
            collecte_le    DATETIME,
            FOREIGN KEY (ville_id) REFERENCES villes(id)
        )
    """)

    connexion.commit()
    connexion.close()
    print("✅ Base meteo.db créée avec succès")
    print("✅ Table 'villes' créée")
    print("✅ Table 'mesures_meteo' créée")

def verifier_base():
    connexion = sqlite3.connect(DB_PATH)
    curseur = connexion.cursor()

    # Lister les tables créées
    curseur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = curseur.fetchall()
    print("\n--- Tables dans la base ---")
    for table in tables:
        print(f"  {table[0]}")

        # Afficher les colonnes de chaque table
        curseur.execute(f"PRAGMA table_info({table[0]})")
        colonnes = curseur.fetchall()
        for col in colonnes:
            print(f"    └── {col[1]} ({col[2]})")

    connexion.close()

if __name__ == "__main__":
    creer_base()
    verifier_base()