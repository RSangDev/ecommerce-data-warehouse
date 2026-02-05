from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import great_expectations as gx

default_args = {
    'owner': 'data-engineer',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def validate_data_quality():
    """Validar qualidade dos dados com Great Expectations"""
    import pandas as pd
    import sqlalchemy
    
    engine = sqlalchemy.create_engine(
        'postgresql://airflow:airflow@postgres:5432/warehouse'
    )
    
    # Ler dados
    df = pd.read_sql("SELECT * FROM bronze.orders LIMIT 1000", engine)
    
    # Criar suite de expectativas
    suite = gx.ExpectationSuite(expectation_suite_name="orders_suite")
    
    # Expectativas
    expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="order_status",
            value_set=["delivered", "shipped", "processing", "canceled"]
        ),
    ]
    
    for exp in expectations:
        suite.add_expectation(exp)
    
    # Validar
    context = gx.get_context()
    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[df],
        expectation_suite_name="orders_suite"
    )
    
    print(f"✅ Data Quality: {results}")

with DAG(
    'data_quality_checks',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['quality', 'monitoring'],
) as dag:
    
    quality_check = PythonOperator(
        task_id='validate_quality',
        python_callable=validate_data_quality,
    )