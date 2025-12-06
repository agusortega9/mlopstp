from fastapi import FastAPI
import psycopg2
import os

app = FastAPI()

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        port=5432
    )

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/recommendations/{adv}/{model}")
def get_reco(adv: str, model: str):

    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT product_id, score 
        FROM recommendations 
        WHERE advertiser_id = %s AND model = %s
        ORDER BY score DESC
        LIMIT 20;
    """, (adv, model))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return {"advertiser_id": adv, "model": model, "recommendations": rows}