from fastapi import FastAPI
import pandas as pd
import numpy as np
import requests

app = FastAPI(title="TP MLOps - Sistema de Anuncios")

@app.get("/anuncios")
def obtener_anuncios(usuario_id: int = 1):
    # Simulación de recomendaciones de anuncios
    df = pd.DataFrame({
        "anuncio_id": [101, 102, 103],
        "score": np.random.rand(3)
    }).sort_values("score", ascending=False)

    return {
        "usuario": usuario_id,
        "recomendaciones": df.to_dict(orient="records")
    }
