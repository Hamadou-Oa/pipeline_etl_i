"""
diagnostic_reseau.py
Diagnostique le problème de connexion réseau à Supabase.
"""

import socket
import subprocess
import sys

HOST = "db.bqaznvlbzgckvmofsiqk.supabase.co"

print("=" * 55)
print("  Diagnostic réseau — Connexion Supabase")
print("=" * 55)

print("\n[1] Résolution DNS...")
try:
    ip = socket.gethostbyname(HOST)
    print(f" DNS  {HOST} = {ip}")
    dns_ok = True
except socket.gaierror as e:
    print(f" DNS échoue : {e}")
    dns_ok = False

print("\n[2] Connexion TCP sur le port 5432...")
try:
    sock = socket.create_connection((HOST, 5432), timeout=5)
    sock.close()
    print(f"Port 5432 accessible !")
    tcp_ok = True
except Exception as e:
    print(f"Port 5432 inaccessible : {e}")
    tcp_ok = False

print("\n[3] Connexion internet générale (google.com)...")
try:
    ip = socket.gethostbyname("www.google.com")
    print(f"Internet google.com = {ip}")
    internet_ok = True
except Exception as e:
    print(f" Pas d'internet : {e}")
    internet_ok = False

print("\n" + "=" * 55)
print("  Analyse")
print("=" * 55)

if not internet_ok:
    print("""
            SANS CONNEXION INTERNET
    """)

elif not dns_ok:
    print("""
            Change le DNS Windows en DNS Google :
            """)

elif not tcp_ok:
    print("""
          Database → Connection Pooling → Port 6543 connexion avec supabase.co:6543
        """)

else:
    print("""
            Réseau OK le problème vient d'ailleurs.
            """)

print("=" * 55)