"""
Inspection du fichier téléchargé pour comprendre le vrai format.
"""

import os
from pathlib import Path

FILE_PATH = Path("data/raw/dataset.csv")

print("=" * 55)
print("  Inspection du fichier téléchargé")
print("=" * 55)

if not FILE_PATH.exists():
    print(f" Fichier introuvable : {FILE_PATH}")
    exit(1)

size = FILE_PATH.stat().st_size
print(f"\n[1] Taille : {size:,} octets ({size/1024/1024:.2f} MB)")

with open(FILE_PATH, "rb") as f:
    header = f.read(32)

print(f"[2] Premiers octets (hex) : {header.hex()}")
print(f"[3] Premiers octets (raw) : {header}")

magic = header[:4].hex()
print(f"\n[4] Détection du format :")
if header[:2] == b'PK':
    print(" C'est un fichier ZIP ! (pas un CSV)")
    print(" Il faut décompresser avant de lire")
elif header[:3] == b'\xef\xbb\xbf' or header[:3] == b'sep':
    print("Probablement un CSV UTF-8 avec BOM")
elif header[:2] in [b'\xff\xfe', b'\xfe\xff']:
    print("Probablement un CSV UTF-16")
elif b'<html' in header.lower() or b'<!doc' in header.lower():
    print(" C'est une page HTML ! (Google Drive a renvoyé une page web)")
    print(" Le lien de téléchargement a expiré ou est incorrect")
elif header[:2] == b'\x1f\x8b':
    print("C'est un fichier GZIP compressé")
else:
    print(f" Format inconnu (magic bytes: {magic})")

print(f"\n[5] Premières lignes du fichier :")
try:
    with open(FILE_PATH, encoding="latin-1") as f:
        for i, line in enumerate(f):
            print(f"    Ligne {i+1}: {repr(line[:120])}")
            if i >= 4:
                break
except Exception as e:
    print(f"    Erreur lecture : {e}")

print("\n" + "=" * 55)