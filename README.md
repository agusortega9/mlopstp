# TP MLOps - Servicio de Anuncios

Este proyecto expone un endpoint FastAPI que entrega anuncios recomendados.

## Cómo construir la imagen

```bash
docker build -t anuncios-mlops .
```

## Cómo ejecutar

```bash
docker run -p 8000:8000 anuncios-mlops
```

## Endpoint

- GET `/anuncios?usuario_id=123`
