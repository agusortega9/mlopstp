from datetime import datetime
import pandas as pd
from airflow.sdk import dag, task

# Ruta local donde pusiste los CSV
LOCAL_DATA_PATH = "/home/agust/MLOps_TP"   # <-- CAMBIAR A TU PATH REAL

@dag(
    dag_id="recommendations_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",   # corre todos los días a las 02:00
    catchup=False,
)
def recommendations_pipeline():

    # ------------------------------
    # TAREA 1: Filtrar Advertisers Activos
    # ------------------------------
    @task
    def filtrar_datos():
        active = pd.read_csv(f"{LOCAL_DATA_PATH}/advertiser_ids")
        df_products = pd.read_csv(f"{LOCAL_DATA_PATH}/product_views")
        df_ads = pd.read_csv(f"{LOCAL_DATA_PATH}/ads_views")

        df_products = df_products[df_products["advertiser_id"].isin(active["advertiser_id"])]
        df_ads = df_ads[df_ads["advertiser_id"].isin(active["advertiser_id"])]

        filtered_products_path = f"{LOCAL_DATA_PATH}/filtered_product_views.csv"
        filtered_ads_path = f"{LOCAL_DATA_PATH}/filtered_ads_views.csv"

        df_products.to_csv(filtered_products_path, index=False)
        df_ads.to_csv(filtered_ads_path, index=False)

        return {
            "products": filtered_products_path,
            "ads": filtered_ads_path
        }

    # ------------------------------
    # TAREA 2: Top CTR
    # ------------------------------
    @task
    def top_ctr(paths: dict):

        df = pd.read_csv(paths["ads"])

        # CTR = clicks / impressions
        df["click"] = (df["type"] == "click").astype(int)
        df["imp"] = (df["type"] == "impression").astype(int)

        grouped = df.groupby(["advertiser_id", "product_id"]).agg(
            clicks=("click", "sum"),
            impressions=("imp", "sum"),
        )

        grouped["ctr"] = grouped["clicks"] / grouped["impressions"].replace(0, 1)

        top_ctr = (
            grouped
            .sort_values(["advertiser_id", "ctr"], ascending=[True, False])
            .groupby(level=0)
            .head(20)
            .reset_index()
        )

        out_path = f"{LOCAL_DATA_PATH}/top_ctr.csv"
        top_ctr.to_csv(out_path, index=False)
        return out_path

    # ------------------------------
    # TAREA 3: Top Product (más vistos)
    # ------------------------------
    @task
    def top_product(paths: dict):

        df = pd.read_csv(paths["products"])

        top = (
            df.groupby(["advertiser_id", "product_id"])
            .size()
            .reset_index(name="views")
            .sort_values(["advertiser_id", "views"], ascending=[True, False])
            .groupby("advertiser_id")
            .head(20)
        )

        out_path = f"{LOCAL_DATA_PATH}/top_product.csv"
        top.to_csv(out_path, index=False)
        return out_path

    # ------------------------------
    # TAREA 4: DB Writing (aun NO escribe en RDS localmente)
    # ------------------------------
    @task
    def write_to_db(top_ctr_path, top_product_path):

        print("Simulación: escribiríamos en PostgreSQL RDS")
        print(f"TopCTR → {top_ctr_path}")
        print(f"TopProduct → {top_product_path}")
        # TODO: agregar psycopg2 más adelante cuando configures RDS

    # DAG FLOW
    filtered = filtrar_datos()
    ctr = top_ctr(filtered)
    top = top_product(filtered)
    write_to_db(ctr, top)


recommendations_pipeline()