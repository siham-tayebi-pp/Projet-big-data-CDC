#!/bin/bash
# Ce script s'exécute automatiquement au premier démarrage de PostgreSQL.
# Docker exécute tous les fichiers de /docker-entrypoint-initdb.d/
# dans l'ordre alphabétique.
#
# Ce script fait 4 choses :
#  1. Crée la base "retail" (nos données métier)
#  2. Crée le rôle "cdc_reader" avec le droit REPLICATION
#  3. Crée les 6 tables métier (customers, products, orders, ...)
#  4. Crée la publication CDC (liste les tables à surveiller par Debezium)

set -euo pipefail

PGUSER="${POSTGRES_USER:-admin}"
PGDB="${POSTGRES_DB:-metastore}"
CDC_USER="${POSTGRES_CDC_USER:-cdc_reader}"
CDC_PASSWORD="${POSTGRES_CDC_PASSWORD:-cdc_reader_pwd}"

echo "=== [1/4] Création de la base retail ==="
psql -U "$PGUSER" -d "$PGDB" -v ON_ERROR_STOP=1 -c "
  SELECT 'CREATE DATABASE retail' WHERE NOT EXISTS
    (SELECT FROM pg_database WHERE datname = 'retail')
  \gexec
"

echo "=== [2/4] Création du rôle CDC ==="
psql -U "$PGUSER" -d "$PGDB" -v ON_ERROR_STOP=1 <<EOSQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${CDC_USER}') THEN
    CREATE ROLE "${CDC_USER}" WITH LOGIN PASSWORD '${CDC_PASSWORD}' REPLICATION;
  ELSE
    ALTER ROLE "${CDC_USER}" WITH LOGIN PASSWORD '${CDC_PASSWORD}' REPLICATION;
  END IF;
END
\$\$;
EOSQL

echo "=== [3/4] Création des tables métier ==="
psql -U "$PGUSER" -d retail -v ON_ERROR_STOP=1 -v cdc_user="$CDC_USER" <<'EOSQL'

-- TABLE 1 : Clients
CREATE TABLE IF NOT EXISTS customers (
    customer_id       SERIAL PRIMARY KEY,
    name              VARCHAR(255) NOT NULL,
    email             VARCHAR(255) UNIQUE,
    city              VARCHAR(100),
    country           VARCHAR(100),
    segment           VARCHAR(50),        -- 'B2B', 'B2C', 'Premium', 'Standard'
    registration_date DATE DEFAULT CURRENT_DATE,
    is_active         BOOLEAN DEFAULT TRUE,
    created_at        TIMESTAMPTZ DEFAULT now(),
    updated_at        TIMESTAMPTZ DEFAULT now()
);

-- TABLE 2 : Produits
CREATE TABLE IF NOT EXISTS products (
    product_id  SERIAL PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    category    VARCHAR(100),             -- 'Electronics', 'Clothing', etc.
    brand       VARCHAR(100),
    price       NUMERIC(10,2) NOT NULL,
    stock_qty   INT DEFAULT 0,
    is_active   BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- TABLE 3 : Commandes
CREATE TABLE IF NOT EXISTS orders (
    order_id     SERIAL PRIMARY KEY,
    customer_id  INT NOT NULL REFERENCES customers(customer_id),
    order_date   TIMESTAMPTZ DEFAULT now(),
    status       VARCHAR(50) DEFAULT 'pending',
    channel      VARCHAR(50),             -- 'web', 'mobile', 'store', 'phone'
    total_amount NUMERIC(12,2),
    created_at   TIMESTAMPTZ DEFAULT now(),
    updated_at   TIMESTAMPTZ DEFAULT now()
);

-- TABLE 4 : Lignes de commande
CREATE TABLE IF NOT EXISTS order_items (
    item_id    SERIAL PRIMARY KEY,
    order_id   INT NOT NULL REFERENCES orders(order_id),
    product_id INT NOT NULL REFERENCES products(product_id),
    quantity   INT NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    subtotal   NUMERIC(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- TABLE 5 : Paiements
CREATE TABLE IF NOT EXISTS payments (
    payment_id   SERIAL PRIMARY KEY,
    order_id     INT NOT NULL REFERENCES orders(order_id),
    payment_date TIMESTAMPTZ DEFAULT now(),
    method       VARCHAR(50),             -- 'card', 'cash', 'transfer', 'wallet'
    amount       NUMERIC(12,2) NOT NULL,
    status       VARCHAR(50) DEFAULT 'completed',
    created_at   TIMESTAMPTZ DEFAULT now(),
    updated_at   TIMESTAMPTZ DEFAULT now()
);

-- TABLE 6 : Retours / Annulations
CREATE TABLE IF NOT EXISTS returns (
    return_id     SERIAL PRIMARY KEY,
    order_id      INT NOT NULL REFERENCES orders(order_id),
    return_date   TIMESTAMPTZ DEFAULT now(),
    reason        VARCHAR(255),
    status        VARCHAR(50) DEFAULT 'pending',
    refund_amount NUMERIC(12,2),
    created_at    TIMESTAMPTZ DEFAULT now(),
    updated_at    TIMESTAMPTZ DEFAULT now()
);

-- Droits pour le rôle CDC (lecture seule, mais REPLICATION)
GRANT CONNECT ON DATABASE retail TO :cdc_user;
GRANT USAGE ON SCHEMA public TO :cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO :cdc_user;

EOSQL

echo "=== [4/4] Création de la publication CDC ==="
psql -U "$PGUSER" -d retail -v ON_ERROR_STOP=1 <<'EOSQL'
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'cdc_publication') THEN
    -- Cette publication dit à PostgreSQL : "surveille toutes les tables
    -- du schéma public et rends leurs changements accessibles via le WAL"
    CREATE PUBLICATION cdc_publication FOR TABLES IN SCHEMA public;
  END IF;
END
$$;
EOSQL

echo "=== PostgreSQL initialisé avec succès ==="
