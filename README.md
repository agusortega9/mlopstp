# TP MLOps - AdTech

## Objetivo
Sistema de recomendación de productos para ads. Dos modelos:
- TopProduct: productos más vistos en la web del advertiser.
- TopCTR: productos con mayor click-through-rate en ads.

## Componentes
1. Pipeline de datos en Airflow sobre EC2.
2. API en FastAPI dockerizada en App Runner.
3. Base PostgreSQL en RDS.

## Estructura
- data/: datos crudos y procesados (luego S3).
- src/: lógica de negocio del pipeline (TopProduct, TopCTR, filtrado, escritura DB).
- api/: implementación FastAPI (recommendations, stats, history).
- dags/: DAGs de Airflow.
- infra/: scripts o notas para configurar S3, RDS, EC2, App Runner.

