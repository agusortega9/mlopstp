# TP MLOps - Servicio de Anuncios

## API
## Despues de actualizar el repositorio en EC2:
## Cómo construir la imagen
```bash
docker build -t grupo-5-api:latest .
```

## Loguearse en ECR (cambiar el account ID si es necesario)
```bash
aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 396913735447.dkr.ecr.us-east-2.amazonaws.com
```

## Taggear la imagen 
```bash
docker tag grupo-5-api:latest \
396913735447.dkr.ecr.us-east-2.amazonaws.com/grupo-5-api:latest
```

## Pushear a ECR
```bash
docker push 396913735447.dkr.ecr.us-east-2.amazonaws.com/grupo-5-api:latest
```

## Actualizar el deploy de App Runner desde la UI de AWS 