import psycopg2
engine = psycopg2.connect(
   database="postgres",
   user="postgres",
   password="postgres",
   host="grupo-5-2025-database.cpomi0gaon83.us-east-2.rds.amazonaws.com",
   port='5432'
)
cursor = engine.cursor()

create_table_query = """
CREATE TABLE IF NOT EXISTS recommendations (
    advertiser_id VARCHAR(50),
    model VARCHAR(20),
    product_id VARCHAR(50),
    score FLOAT,
    date DATE,
    PRIMARY KEY (advertiser_id, model, product_id, date)
);
"""

cursor.execute(create_table_query)
engine.commit()