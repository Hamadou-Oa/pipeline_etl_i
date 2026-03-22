# 🗄️ Guide : Configurer PostgreSQL Supabase (Gratuit)

## Pourquoi Supabase ?
- ✅ PostgreSQL 100% gratuit (500MB, suffisant pour le projet)
- ✅ Accessible depuis partout (cloud, Airflow, local)
- ✅ Interface web pour visualiser les données
- ✅ SSL inclus (obligatoire pour la sécurité)

---

## Étape 1 : Créer le compte

1. Va sur **https://supabase.com**
2. Clique **"Start your project"** → Sign up avec GitHub ou email
3. Crée un nouveau projet :
   - **Name** : `etl-pipeline`
   - **Database Password** : note-le bien (tu en auras besoin)
   - **Region** : West EU (Ireland) — le plus proche d'Afrique
4. Attends ~2 minutes que le projet démarre

---

## Étape 2 : Créer la table customers

Dans Supabase → **SQL Editor** → colle et exécute :

```sql
-- Table principale : 3ème source de données ETL
CREATE TABLE IF NOT EXISTS customers (
    id          SERIAL PRIMARY KEY,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    email       VARCHAR(255) UNIQUE NOT NULL,
    country     VARCHAR(100),
    city        VARCHAR(100),
    age         INTEGER,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Données de test (20 clients)
INSERT INTO customers (first_name, last_name, email, country, city, age) VALUES
('Amadou',   'Diallo',   'amadou@example.com',   'Sénégal',  'Dakar',      28),
('Fatima',   'Koné',     'fatima@example.com',    'Mali',     'Bamako',     32),
('Ibrahim',  'Traoré',   'ibrahim@example.com',   'BF',       'Ouagadougou',25),
('Mariama',  'Bah',      'mariama@example.com',   'Guinée',   'Conakry',    30),
('Moussa',   'Coulibaly','moussa@example.com',    'CI',       'Abidjan',    27),
('Aïcha',    'Sawadogo', 'aicha@example.com',     'BF',       'Bobo',       24),
('Seydou',   'Keita',    'seydou@example.com',    'Mali',     'Sikasso',    35),
('Kadiatou', 'Barry',    'kadiatou@example.com',  'Guinée',   'Labé',       29),
('Oumar',    'Ndiaye',   'oumar@example.com',     'Sénégal',  'Thiès',      31),
('Hawa',     'Touré',    'hawa@example.com',      'CI',       'Bouaké',     26),
('Cheikh',   'Fall',     'cheikh@example.com',    'Sénégal',  'Saint-Louis',33),
('Ramatou',  'Cissé',    'ramatou@example.com',   'Mali',     'Ségou',      28),
('Alassane', 'Ouédraogo','alassane@example.com',  'BF',       'Koudougou',  30),
('Mariam',   'Sidibé',   'mariam@example.com',    'Mali',     'Mopti',      22),
('Souleymane','Diabaté', 'souleymane@example.com','CI',       'Yamoussoukro',38),
('Aminata',  'Kouyaté',  'aminata@example.com',   'Guinée',   'N''Zérékoré',27),
('Boubacar', 'Diarra',   'boubacar@example.com',  'Mali',     'Kayes',      34),
('Rokhaya',  'Mbaye',    'rokhaya@example.com',   'Sénégal',  'Ziguinchor', 29),
('Hamidou',  'Zongo',    'hamidou@example.com',   'BF',       'Fada',       26),
('Kadija',   'Camara',   'kadija@example.com',    'Guinée',   'Kindia',     31);
```

---

## Étape 3 : Récupérer l'URL de connexion

Dans Supabase → **Settings** → **Database** → **Connection string**

Sélectionne **"URI"** — tu obtiendras quelque chose comme :
```
postgresql://postgres:[MOT_DE_PASSE]@db.abcdefghijkl.supabase.co:5432/postgres
```

---

## Étape 4 : Configurer dans le projet

Crée un fichier `.env` à la racine du projet :

```bash
# .env
DATABASE_URL=postgresql://postgres:TON_MOT_DE_PASSE@db.TON_ID.supabase.co:5432/postgres
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=datalake
CSV_URL=https://drive.google.com/file/d/1s-x76gQ-eoM5sqT2Hhcfn087Aw5D__hD/view?usp=sharing
```

---

## Étape 5 : Tester la connexion

```python
# test rapide depuis ton terminal
python -c "
from src.extraction.sql_extractor import SQLExtractor
import os
from dotenv import load_dotenv

load_dotenv()
extractor = SQLExtractor(
    db_url=os.getenv('DATABASE_URL'),
    query='SELECT COUNT(*) as total FROM customers'
)
df = extractor.extract()
print(df)
"
```

Résultat attendu :
```
   total source
0     20    sql
```

---

## ⚠️ Problèmes fréquents

| Erreur | Cause | Solution |
|--------|-------|---------|
| `SSL required` | Connexion sans SSL | Déjà géré dans `sql_extractor.py` |
| `password authentication failed` | Mauvais mot de passe | Vérifie le `.env` |
| `connection refused` | Mauvais host | Copie l'URL exacte depuis Supabase |
| `relation does not exist` | Table non créée | Exécute le SQL de l'étape 2 |
