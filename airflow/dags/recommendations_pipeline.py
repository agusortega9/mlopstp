from datetime import datetime
import pandas as pd
import boto3
import psycopg2
from airflow.sdk import dag, task
from airflow.models import Variable
from io import StringIO


# ----------------------------
# Helper: leer CSV desde S3
# ----------------------------
def read_csv_from_s3(bucket: str, key: str):
    s3 = boto3.client("s3")

    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read().decode("utf-8")

    return pd.read_csv(StringIO(data))


# ----------------------------
# DAG
# ----------------------------
BUCKET = "grupo-5-2025"


@dag(
    dag_id="recommendations_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",   # corre todos los días a las 02:00
    catchup=False,
)
def recommendations_pipeline():

    # -------------------------
    # TAREA 1 — Filtrar Datos
    # -------------------------
    @task
    def filtrar_datos():

        df_active = read_csv_from_s3(BUCKET, "advertiser_ids")
        df_products = read_csv_from_s3(BUCKET, "product_views")
        df_ads = read_csv_from_s3(BUCKET, "ads_views")

        df_products = df_products[
            df_products["advertiser_id"].isin(df_active["advertiser_id"])
        ]

        df_ads = df_ads[
            df_ads["advertiser_id"].isin(df_active["advertiser_id"])
        ]

        return {
            "products": df_products.to_dict(orient="records"),
            "ads": df_ads.to_dict(orient="records")
        }

    # -------------------------
    # TAREA 2 — TopCTR
    # -------------------------
    @task
    def top_ctr(data: dict):

        df = pd.DataFrame(data["ads"])

        df["click"] = (df["type"] == "click").astype(int)
        df["imp"] = (df["type"] == "impression").astype(int)

        grouped = df.groupby(["advertiser_id", "product_id"]).agg(
            clicks=("click", "sum"),
            impressions=("imp", "sum"),
        )

        grouped["ctr"] = grouped["clicks"] / grouped["impressions"].replace(0, 1)

        top = (
            grouped.sort_values(["advertiser_id", "ctr"], ascending=[True, False])
            .groupby(level=0)
            .head(20)
            .reset_index()
        )

        # Return dict → {adv: [(product, ctr), ...], ...}
        result = {}
        for row in top.itertuples(index=False):
            adv = row.advertiser_id
            result.setdefault(adv, []).append((row.product_id, float(row.ctr)))

        return result

    # -------------------------
    # TAREA 3 — TopProduct
    # -------------------------
    @task
    def top_product(data: dict):

        df = pd.DataFrame(data["products"])

        top = (
            df.groupby(["advertiser_id", "product_id"])
            .size()
            .reset_index(name="views")
            .sort_values(["advertiser_id", "views"], ascending=[True, False])
            .groupby("advertiser_id")
            .head(20)
        )

        result = {}
        for row in top.itertuples(index=False):
            adv = row.advertiser_id
            result.setdefault(adv, []).append((row.product_id, int(row.views)))

        return result

    # -------------------------
    # TAREA 4 — Escribir en RDS
    # -------------------------
    @task
    def write_db(topctr: dict, topproduct: dict, **context):

        conn = psycopg2.connect(
            host=Variable.get("RDS_HOST"),
            port=5432,
            user=Variable.get("RDS_USER"),
            password=Variable.get("RDS_PASSWORD"),
            dbname=Variable.get("RDS_DBNAME"),
        )
        cursor = conn.cursor()

        insert_sql = """
        INSERT INTO recommendations (advertiser_id, model, product_id, score, date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (advertiser_id, model, product_id, date)
        DO UPDATE SET score = EXCLUDED.score;
        """

        today = context["ds"]

        # ---- Insert TopCTR
        for adv, recs in topctr.items():
            for product, score in recs:
                cursor.execute(insert_sql, (adv, "TopCTR", product, score, today))

        # ---- Insert TopProduct
        for adv, recs in topproduct.items():
            for product, score in recs:
                cursor.execute(insert_sql, (adv, "TopProduct", product, score, today))

        conn.commit()
        cursor.close()
        conn.close()

    # -------------------------
    # FLOW
    # -------------------------

    filtered = filtrar_datos()
    ctr = top_ctr(filtered)
    tp = top_product(filtered)
    write_db(ctr, tp)


recommendations_pipeline()
