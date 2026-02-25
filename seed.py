import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
INVENTORY_CSV          = "data/inventory.csv"
SALES_CSV              = "data/sales_transactions.csv"
FINANCIAL_CSV          = "data/financial_tracking.csv"
engine = create_engine(DB_URL)
CREATE_SQL = """

CREATE TABLE IF NOT EXISTS inventory (
    batch_no                VARCHAR(50)  PRIMARY KEY,
    sku_id                  VARCHAR(50)  NOT NULL,
    sku_name                VARCHAR(200),
    category                VARCHAR(100),
    supplier_id             VARCHAR(50),
    supplier_name           VARCHAR(200),
    warehouse_id            VARCHAR(50),
    manufactured_date       DATE,
    expiry_date             DATE,
    shelf_life_days         INT,
    shelf_life_remaining_pct FLOAT,
    quantity_in_stock       INT,
    unit                    VARCHAR(20),
    cost_per_unit           DECIMAL(10,2),
    reorder_level           INT,
    reorder_quantity        INT,
    stock_status            VARCHAR(30),
    is_damaged              BOOLEAN,
    damaged_qty             INT,
    damage_reason           TEXT,
    financial_loss_damage   DECIMAL(12,2),
    inventory_turnover_rate FLOAT,
    last_updated            TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sales_transactions (
    order_id                VARCHAR(50)  PRIMARY KEY,
    order_date              DATE,
    order_time              TIME,
    order_hour              INT,
    sku_id                  VARCHAR(50),
    sku_name                VARCHAR(200),
    category                VARCHAR(100),
    channel                 VARCHAR(50),
    region                  VARCHAR(100),
    distributor_id          VARCHAR(50),
    qty_ordered             INT,
    qty_delivered           INT,
    fulfillment_rate_pct    FLOAT,
    is_partial_delivery     BOOLEAN,
    unit_cost               DECIMAL(10,2),
    selling_price_per_unit  DECIMAL(10,2),
    discount_pct            FLOAT,
    total_revenue           DECIMAL(12,2),
    gross_margin            DECIMAL(12,2),
    payment_status          VARCHAR(30),
    days_to_payment         INT,
    transaction_type        VARCHAR(20),
    return_reason           TEXT,
    original_order_id       VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS financial_tracking (
    transaction_id  VARCHAR(50)  PRIMARY KEY,
    date            DATE,
    sku_id          VARCHAR(50),
    sku_name        VARCHAR(200),
    category        VARCHAR(100),
    transaction_type VARCHAR(20),
    channel         VARCHAR(50),
    region          VARCHAR(100),
    quantity        INT,
    unit_cost       DECIMAL(10,2),
    selling_price   DECIMAL(10,2),
    total_value     DECIMAL(12,2),
    gross_margin    DECIMAL(12,2),
    discount_pct    FLOAT,
    payment_status  VARCHAR(30),
    days_to_payment INT,
    reference_id    VARCHAR(50),
    damage_reason   TEXT
);

CREATE TABLE IF NOT EXISTS exceptions (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    outlet_id        VARCHAR(50),
    exception_type   VARCHAR(50),
    amount           DECIMAL(10,2),
    authority_status VARCHAR(50),
    occurred_at      TIMESTAMPTZ DEFAULT NOW(),
    source           VARCHAR(20),
    shift            VARCHAR(20),
    day_type         VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS conversations (
    conv_id     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     VARCHAR(50),
    prompt      TEXT,
    response    TEXT,
    tokens_used INT,
    domain      VARCHAR(30),
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_inv_sku        ON inventory (sku_id);
CREATE INDEX IF NOT EXISTS idx_inv_expiry      ON inventory (expiry_date);
CREATE INDEX IF NOT EXISTS idx_sal_date_sku    ON sales_transactions (order_date, sku_id);
CREATE INDEX IF NOT EXISTS idx_sal_channel     ON sales_transactions (channel);
CREATE INDEX IF NOT EXISTS idx_fin_date        ON financial_tracking (date);
CREATE INDEX IF NOT EXISTS idx_exc_outlet_time ON exceptions (outlet_id, occurred_at DESC);
"""

def load_inventory():
    df = pd.read_csv(INVENTORY_CSV)
    df['manufactured_date'] = pd.to_datetime(df['manufactured_date'], errors='coerce')
    df['expiry_date']       = pd.to_datetime(df['expiry_date'],       errors='coerce')
    df['last_updated']      = pd.to_datetime(df['last_updated'],      errors='coerce')
    df['is_damaged']        = df['is_damaged'].astype(bool)
    df.to_sql('inventory', engine, if_exists='append', index=False)
    print(f" inventory       → {len(df)} rows")

def load_sales():
    df = pd.read_csv(SALES_CSV)
    df['order_date']          = pd.to_datetime(df['order_date'],  errors='coerce')
    df['is_partial_delivery'] = df['is_partial_delivery'].astype(bool)
    df['return_reason']       = df['return_reason'].fillna('')
    df['original_order_id']   = df['original_order_id'].fillna('')
    df.to_sql('sales_transactions', engine, if_exists='append', index=False)
    print(f" sales_transactions → {len(df)} rows")

def load_financial():
    df = pd.read_csv(FINANCIAL_CSV)
    df['date']          = pd.to_datetime(df['date'], errors='coerce')
    df['damage_reason'] = df['damage_reason'].fillna('')
    df['reference_id']  = df['reference_id'].fillna('')
    df.to_sql('financial_tracking', engine, if_exists='append', index=False)
    print(f" financial_tracking → {len(df)} rows")

if __name__ == "__main__":
    print("\n🔌 Connecting to Supabase...")
    with engine.connect() as conn:
        conn.execute(text(CREATE_SQL))
        conn.commit()
    print("Tables created\n")

    print("Loading CSVs...")
    load_inventory()
    load_sales()
    load_financial()

    print("\n Done! Open Supabase dashboard → Table Editor to verify.")
