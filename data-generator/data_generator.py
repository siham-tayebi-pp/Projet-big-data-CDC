#!/usr/bin/env python3
"""
Simulateur de transactions opérationnelles.

Ce script génère des opérations INSERT / UPDATE / DELETE en continu
sur la base PostgreSQL "retail", simulant un vrai système e-commerce.

Chaque opération déclenche un événement CDC capturé par Debezium.

Opérations simulées :
  - Nouveaux clients (INSERT customers)          → CDC op='c'
  - Nouvelles commandes (INSERT orders)           → CDC op='c'
  - Changement de statut (UPDATE orders.status)  → CDC op='u'
  - Paiements (INSERT payments)                  → CDC op='c'
  - Annulations (UPDATE orders → 'cancelled')    → CDC op='u'
  - Retours (INSERT returns)                     → CDC op='c'
  - Mise à jour prix (UPDATE products.price)     → CDC op='u'
  - Suppression brouillon (DELETE orders)        → CDC op='d'
"""

import os
import random
import time
import logging
import argparse
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("generator")

DB = {
    "host":     os.environ.get("PG_HOST", "localhost"),
    "port":     int(os.environ.get("PG_PORT", "5432")),
    "user":     os.environ.get("PG_USER", "admin"),
    "password": os.environ.get("PG_PASSWORD", "admin"),
    "dbname":   os.environ.get("PG_DBNAME", "retail"),
}

RATE_MS        = int(os.environ.get("SIMULATION_RATE_MS", "500"))
SEED_CUSTOMERS = int(os.environ.get("SEED_CUSTOMERS", "200"))
SEED_PRODUCTS  = int(os.environ.get("SEED_PRODUCTS", "100"))

FIRST_NAMES = ["Alice","Bob","Charlie","Diana","Eve","Frank","Grace","Hector",
               "Iris","Jules","Karima","Lamine","Maria","Nabil","Olivia","Pierre"]
LAST_NAMES  = ["Dupont","Martin","Bernard","Thomas","Robert","Simon","Laurent",
               "Lefebvre","Michel","Garcia","Moreau","David"]
CITIES      = ["Paris","Lyon","Marseille","Toulouse","Nice","Nantes","Casablanca",
               "Rabat","Tunis","Dakar","Alger","Bruxelles","Genève"]
SEGMENTS    = ["Standard","Premium","B2B","B2C","VIP"]
CATEGORIES  = ["Electronics","Clothing","Home","Books","Sports"]
BRANDS      = ["TechCo","FashionX","HomeStyle","BookWorld","SportLife"]
CHANNELS    = ["web","mobile","store","phone"]
METHODS     = ["card","cash","transfer","wallet"]
REASONS     = ["defective","wrong_item","changed_mind","damaged"]


def connect():
    for i in range(30):
        try:
            c = psycopg2.connect(**DB)
            c.autocommit = False
            log.info("Connecté à PostgreSQL retail@%s", DB["host"])
            return c
        except psycopg2.OperationalError as e:
            log.warning("Attente PostgreSQL (%d/30) : %s", i+1, e)
            time.sleep(3)
    raise RuntimeError("PostgreSQL non disponible")


def seed(conn):
    """Insère les données initiales si la base est vide."""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM customers")
    if cur.fetchone()[0] >= SEED_CUSTOMERS:
        log.info("Seed déjà fait, skip.")
        cur.close()
        return

    log.info("Seed : %d clients + %d produits", SEED_CUSTOMERS, SEED_PRODUCTS)

    customers = [
        (
            f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
            f"user{i}_{random.randint(100,999)}@mail.com",
            random.choice(CITIES),
            "France",
            random.choice(SEGMENTS),
            (datetime.now() - timedelta(days=random.randint(0, 365))).date(),
        )
        for i in range(SEED_CUSTOMERS)
    ]
    execute_values(cur, """
        INSERT INTO customers(name,email,city,country,segment,registration_date)
        VALUES %s ON CONFLICT(email) DO NOTHING
    """, customers)

    products = [
        (
            f"Produit {i+1}",
            random.choice(CATEGORIES),
            random.choice(BRANDS),
            round(random.uniform(5, 1500), 2),
            random.randint(0, 500),
        )
        for i in range(SEED_PRODUCTS)
    ]
    execute_values(cur, """
        INSERT INTO products(name,category,brand,price,stock_qty) VALUES %s
    """, products)

    conn.commit()
    cur.close()
    log.info("Seed terminé.")


# ── Opérations CDC ────────────────────────────────────────

def op_insert_customer(cur):
    """INSERT → CDC op='c' sur customers"""
    n = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
    cur.execute("""
        INSERT INTO customers(name,email,city,country,segment)
        VALUES(%s,%s,%s,'France',%s) ON CONFLICT(email) DO NOTHING
    """, (n, f"{n.replace(' ','.')}_{random.randint(1000,9999)}@test.com",
          random.choice(CITIES), random.choice(SEGMENTS)))
    return "INSERT customer"


def op_insert_order(cur):
    """INSERT → CDC op='c' sur orders + order_items"""
    cur.execute("SELECT customer_id FROM customers WHERE is_active ORDER BY RANDOM() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return "SKIP"
    cid = row[0]

    cur.execute("SELECT product_id, price FROM products WHERE is_active ORDER BY RANDOM() LIMIT %s",
                (random.randint(1, 4),))
    items = cur.fetchall()
    if not items:
        return "SKIP"

    total = sum(float(p) * random.randint(1, 3) for _, p in items)
    cur.execute("""
        INSERT INTO orders(customer_id,status,channel,total_amount)
        VALUES(%s,'pending',%s,%s) RETURNING order_id
    """, (cid, random.choice(CHANNELS), round(total, 2)))
    oid = cur.fetchone()[0]

    execute_values(cur, """
        INSERT INTO order_items(order_id,product_id,quantity,unit_price) VALUES %s
    """, [(oid, pid, random.randint(1,3), float(price)) for pid, price in items])

    return f"INSERT order {oid}"


def op_update_order_status(cur):
    """UPDATE → CDC op='u' sur orders"""
    cur.execute("""
        SELECT order_id, status FROM orders
        WHERE status IN ('pending','confirmed','shipped')
        ORDER BY RANDOM() LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        return "SKIP"
    oid, status = row
    next_s = {"pending": "confirmed", "confirmed": "shipped", "shipped": "delivered"}.get(status, "delivered")
    cur.execute("UPDATE orders SET status=%s, updated_at=now() WHERE order_id=%s", (next_s, oid))
    return f"UPDATE order {oid}: {status}→{next_s}"


def op_insert_payment(cur):
    """INSERT → CDC op='c' sur payments"""
    cur.execute("""
        SELECT o.order_id, o.total_amount FROM orders o
        LEFT JOIN payments p ON o.order_id=p.order_id
        WHERE p.payment_id IS NULL AND o.status IN ('confirmed','shipped','delivered')
        ORDER BY RANDOM() LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        return "SKIP"
    oid, amount = row
    cur.execute("""
        INSERT INTO payments(order_id,method,amount,status)
        VALUES(%s,%s,%s,'completed')
    """, (oid, random.choice(METHODS), round(float(amount or 50), 2)))
    return f"INSERT payment order {oid}"


def op_cancel_order(cur):
    """UPDATE → CDC op='u' (annulation commande)"""
    cur.execute("SELECT order_id FROM orders WHERE status='pending' ORDER BY RANDOM() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return "SKIP"
    cur.execute("UPDATE orders SET status='cancelled', updated_at=now() WHERE order_id=%s", (row[0],))
    return f"UPDATE order {row[0]}: CANCELLED"


def op_insert_return(cur):
    """INSERT → CDC op='c' sur returns"""
    cur.execute("""
        SELECT o.order_id, o.total_amount FROM orders o
        LEFT JOIN returns r ON o.order_id=r.order_id
        WHERE o.status='delivered' AND r.return_id IS NULL
        ORDER BY RANDOM() LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        return "SKIP"
    oid, total = row
    cur.execute("""
        INSERT INTO returns(order_id,reason,status,refund_amount)
        VALUES(%s,%s,'pending',%s)
    """, (oid, random.choice(REASONS), round(float(total or 30) * 0.8, 2)))
    return f"INSERT return order {oid}"


def op_update_price(cur):
    """UPDATE → CDC op='u' sur products"""
    cur.execute("SELECT product_id, price FROM products ORDER BY RANDOM() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return "SKIP"
    pid, price = row
    new_price = round(float(price) * random.uniform(0.9, 1.1), 2)
    cur.execute("UPDATE products SET price=%s, updated_at=now() WHERE product_id=%s", (new_price, pid))
    return f"UPDATE product {pid} price: {price}→{new_price}"


def op_delete_draft(cur):
    """DELETE → CDC op='d' sur orders (supprime les vieux brouillons)"""
    cur.execute("""
        DELETE FROM orders
        WHERE status='pending' AND created_at < now() - INTERVAL '5 minutes'
        RETURNING order_id
    """)
    deleted = cur.fetchall()
    return f"DELETE {len(deleted)} drafts" if deleted else "SKIP"


# Pondération des opérations (simule la réalité d'un e-commerce)
OPS = [
    (op_insert_order,        35),
    (op_update_order_status, 20),
    (op_insert_payment,      15),
    (op_insert_customer,     12),
    (op_cancel_order,         7),
    (op_insert_return,        5),
    (op_update_price,         4),
    (op_delete_draft,         2),
]


def run_cycle(conn):
    ops, weights = zip(*OPS)
    fn = random.choices(ops, weights=weights, k=1)[0]
    cur = conn.cursor()
    try:
        result = fn(cur)
        conn.commit()
        log.info("✅ %s", result)
    except Exception as e:
        conn.rollback()
        log.warning("⚠️ %s : %s", fn.__name__, e)
    finally:
        cur.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed-only", action="store_true")
    parser.add_argument("--once",      action="store_true")
    args = parser.parse_args()

    conn = connect()
    seed(conn)

    if args.seed_only:
        conn.close()
        return

    log.info("Simulation démarrée (toutes les %dms). Ctrl+C pour arrêter.", RATE_MS)
    n = 0
    try:
        while True:
            run_cycle(conn)
            n += 1
            if n % 100 == 0:
                log.info("─── %d opérations effectuées ───", n)
            if args.once:
                break
            time.sleep(RATE_MS / 1000.0)
    except KeyboardInterrupt:
        log.info("Arrêté après %d opérations.", n)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
