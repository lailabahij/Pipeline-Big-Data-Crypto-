import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import glob


# =====================================
# CONFIG SNOWFLAKE
# =====================================

SNOWFLAKE_CONFIG = {
    "user": "laila",
    "password": "LailaDataAnalyst2026",
    "account": "pj74301.eu-west-3.aws",
    "warehouse": "COMPUTE_WH",
    "database": "CRYPTO_DB",
    "schema": "GOLD"
}


# =====================================
# CONNEXION
# =====================================

def get_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        account=SNOWFLAKE_CONFIG["account"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"]
    )


# =====================================
# CHARGEMENT TABLE
# =====================================

def load_table(conn, df, table_name):

    df = df.copy()

    # Colonnes en majuscules
    df.columns = df.columns.str.upper()


    # Nettoyage DIM_DATE
    if table_name == "DIM_DATE":

        if "TIMESTAMP" in df.columns:
            df = df.drop(columns=["TIMESTAMP"])


        if "DATE" in df.columns:
            df["DATE"] = pd.to_datetime(
                df["DATE"],
                errors="coerce"
            ).dt.date


    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name,
        auto_create_table=False
    )


    if success:
        print(f"✅ {table_name}: {nrows} lignes")
    else:
        raise Exception(f"Erreur chargement {table_name}")



# =====================================
# CREATION TABLES
# =====================================

def create_tables(conn):

    cur = conn.cursor()

    try:

        cur.execute("USE DATABASE CRYPTO_DB")
        cur.execute("USE SCHEMA GOLD")


        # Création des tables

        cur.execute("""
        CREATE TABLE IF NOT EXISTS DIM_CRYPTO(
            CRYPTO_KEY INTEGER,
            ID STRING,
            SYMBOL STRING,
            NAME STRING
        )
        """)


        cur.execute("""
        CREATE TABLE IF NOT EXISTS DIM_DATE(
            DATE_KEY INTEGER,
            DATE DATE,
            YEAR INTEGER,
            MONTH INTEGER,
            DAY INTEGER,
            HOUR INTEGER,
            MINUTE INTEGER,
            WEEK INTEGER,
            QUARTER INTEGER
        )
        """)


        cur.execute("""
        CREATE TABLE IF NOT EXISTS FACT_CRYPTO_SNAPSHOT(
            CRYPTO_KEY INTEGER,
            DATE_KEY INTEGER,
            CURRENT_PRICE FLOAT,
            MARKET_CAP FLOAT,
            MARKET_CAP_RANK INTEGER,
            TOTAL_VOLUME FLOAT,
            HIGH_24H FLOAT,
            LOW_24H FLOAT,
            PRICE_CHANGE_24H FLOAT,
            PRICE_CHANGE_PERCENTAGE_24H FLOAT,
            CIRCULATING_SUPPLY FLOAT,
            MAX_SUPPLY FLOAT,
            ATH FLOAT
        )
        """)


        conn.commit()

        print("✅ Tables créées")


        # =====================================
        # NETTOYAGE AVANT RECHARGEMENT
        # =====================================

        cur.execute("TRUNCATE TABLE DIM_CRYPTO")

        cur.execute("TRUNCATE TABLE DIM_DATE")

        cur.execute("TRUNCATE TABLE FACT_CRYPTO_SNAPSHOT")


        print("🧹 Tables vidées avant chargement")


    finally:
        cur.close()



# =====================================
# PIPELINE SNOWFLAKE
# =====================================

def snowflake_pipeline():

    print("🚀 START SNOWFLAKE LOAD")


    dim_crypto_file = sorted(
        glob.glob("../data/gold/*/*/*/dim_crypto.parquet")
    )[-1]


    dim_date_file = sorted(
        glob.glob("../data/gold/*/*/*/dim_date.parquet")
    )[-1]


    fact_file = sorted(
        glob.glob("../data/gold/*/*/*/fact_crypto_snapshot.parquet")
    )[-1]


    # Lecture parquet

    dim_crypto = pd.read_parquet(dim_crypto_file)

    dim_date = pd.read_parquet(dim_date_file)

    fact = pd.read_parquet(fact_file)



    if "timestamp" in dim_date.columns:
        dim_date = dim_date.drop(columns=["timestamp"])



    print("\nDATES DANS GOLD")
    print(sorted(dim_date["date"].unique()))


    print("\nSHAPES")

    print("DIM_CRYPTO :", dim_crypto.shape)

    print("DIM_DATE   :", dim_date.shape)

    print("FACT       :", fact.shape)



    conn = get_connection()


    try:


        create_tables(conn)


        load_table(conn, dim_crypto, "DIM_CRYPTO")

        load_table(conn, dim_date, "DIM_DATE")

        load_table(conn, fact, "FACT_CRYPTO_SNAPSHOT")



        cur = conn.cursor()



        print("\n📊 VALIDATION")


        cur.execute(
            "SELECT COUNT(*) FROM DIM_CRYPTO"
        )
        print(
            "DIM_CRYPTO :",
            cur.fetchone()[0]
        )


        cur.execute(
            "SELECT COUNT(*) FROM DIM_DATE"
        )
        print(
            "DIM_DATE :",
            cur.fetchone()[0]
        )


        cur.execute(
            "SELECT COUNT(*) FROM FACT_CRYPTO_SNAPSHOT"
        )
        print(
            "FACT :",
            cur.fetchone()[0]
        )



        cur.execute("""
        SELECT DISTINCT DATE
        FROM DIM_DATE
        ORDER BY DATE
        """)


        print("\n📅 DATES DANS SNOWFLAKE")


        for row in cur.fetchall():
            print(row[0])



        cur.close()


        print("\n✅ LOAD TERMINÉ")



    finally:

        conn.close()



# =====================================
# RUN
# =====================================

if __name__ == "__main__":

    snowflake_pipeline()