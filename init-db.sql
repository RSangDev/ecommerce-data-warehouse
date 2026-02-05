-- Database para o warehouse
CREATE DATABASE warehouse;

-- Database para Metabase
CREATE DATABASE metabase;

-- Conectar ao warehouse e criar schemas
\c warehouse;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA bronze TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA silver TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA gold TO airflow;