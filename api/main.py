from fastapi import FastAPI, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel
from typing import List
from datetime import date
import os

app = FastAPI(title="AdTech Recommendations API")

# --- DATABASE CONFIG ---
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = "5432"

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            port=DB_PORT,
            cursor_factory=RealDictCursor
        )
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")

# --- MODELOS ---
class Recommendation(BaseModel):
    advertiser_id: str
    product_id: str
    model: str
    score: float
    date: str

# --- ENDPOINTS ---

@app.get("/")
def health_check():
    return {"status": "ok", "service": "AdTech API", "version": "1.0.0"}

@app.get("/recommendations/{advertiser_id}/{model}", response_model=List[Recommendation])
def get_recommendations(advertiser_id: str, model: str):
    """
    Devuelve las recomendaciones DEL DÍA para un anunciante y modelo específico.
    Modelos esperados: 'TopCTR' o 'TopProduct'
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # IMPORTANTE: Filtramos por fecha actual (CURRENT_DATE) según consigna
    query = """
        SELECT advertiser_id, product_id, model, score, date::text
        FROM recommendations
        WHERE advertiser_id = %s 
          AND model = %s
          AND date = CURRENT_DATE
        ORDER BY score DESC
        LIMIT 20
    """
    cursor.execute(query, (advertiser_id, model))
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return results if results else []

@app.get("/history/{advertiser_id}", response_model=List[Recommendation])
def get_history(advertiser_id: str):
    """
    Devuelve el historial de recomendaciones de los ÚLTIMOS 7 DÍAS.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Filtro: date >= (HOY - 7 días)
    query = """
        SELECT advertiser_id, product_id, model, score, date::text
        FROM recommendations
        WHERE advertiser_id = %s
          AND date >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY date DESC, score DESC
        LIMIT 500
    """
    cursor.execute(query, (advertiser_id,))
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return results if results else []

@app.get("/stats")
def get_stats():
    """
    Estadísticas generales del sistema.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Cantidad total de recomendaciones
    cursor.execute("SELECT COUNT(*) as total FROM recommendations")
    total = cursor.fetchone()["total"]
    
    # 2. Cantidad de Anunciantes únicos
    cursor.execute("SELECT COUNT(DISTINCT advertiser_id) as advertisers FROM recommendations")
    advertisers = cursor.fetchone()["advertisers"]
    
    # 3. Distribución por Modelo
    cursor.execute("SELECT model, COUNT(*) as count FROM recommendations GROUP BY model")
    models_data = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return {
        "summary": "System Statistics",
        "total_recommendations": total,
        "unique_advertisers": advertisers,
        "models_breakdown": models_data
    }
