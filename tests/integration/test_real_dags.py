"""
Testes de Integração - DAGs Reais do Airflow
REQUISITO: Airflow instalado (pip install apache-airflow)

Executar com: pytest tests/integration/test_real_dags.py -v

NOTA: O conftest.py configura o Airflow antes destes imports
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import pandas as pd
import os

# Imports do Airflow - configuração feita no conftest.py
# Não usar try/except aqui para ver o erro real se houver
from airflow.models import DagBag

AIRFLOW_AVAILABLE = True


# ============================================================================
# FIXTURES
# ============================================================================


@pytest.fixture
def dagbag():
    """Carregar DAGs do Airflow"""
    dag_folder = os.path.join(os.path.dirname(__file__), "..", "..", "airflow", "dags")
    return DagBag(dag_folder=dag_folder, include_examples=False)


@pytest.fixture
def sample_dataframe():
    """DataFrame de exemplo para testes"""
    return pd.DataFrame(
        {
            "order_id": ["ORDER-001", "ORDER-002", "ORDER-003"],
            "customer_id": ["CUST-001", "CUST-002", "CUST-003"],
            "order_status": ["delivered", "shipped", "processing"],
            "order_date": pd.date_range("2024-01-01", periods=3),
            "total_amount": [100.50, 250.00, 75.25],
        }
    )


# ============================================================================
# TESTES DA DAG ecommerce_etl_pipeline
# ============================================================================


class TestEcommerceETLPipelineDAG:
    """Testes da DAG de ETL principal"""

    def test_dag_loaded(self, dagbag):
        """Verificar se a DAG foi carregada sem erros"""
        dag = dagbag.get_dag(dag_id="ecommerce_etl_pipeline")
        assert dag is not None, "DAG ecommerce_etl_pipeline não foi carregada"
        assert (
            len(dagbag.import_errors) == 0
        ), f"Erros de import: {dagbag.import_errors}"

    def test_dag_structure(self, dagbag):
        """Verificar estrutura da DAG"""
        dag = dagbag.get_dag(dag_id="ecommerce_etl_pipeline")

        # Verificar propriedades
        assert dag.dag_id == "ecommerce_etl_pipeline"
        assert dag.schedule_interval == "@daily"
        assert dag.catchup is False
        assert dag.max_active_runs == 1

    def test_dag_default_args(self, dagbag):
        """Verificar default_args da DAG"""
        dag = dagbag.get_dag(dag_id="ecommerce_etl_pipeline")

        assert dag.default_args["owner"] == "data-engineer"
        assert dag.default_args["retries"] == 2
        assert dag.default_args["retry_delay"] == timedelta(minutes=3)
        assert dag.default_args["depends_on_past"] is False

    def test_dag_tags(self, dagbag):
        """Verificar tags da DAG"""
        dag = dagbag.get_dag(dag_id="ecommerce_etl_pipeline")

        expected_tags = {"ecommerce", "etl", "bronze", "silver", "gold"}
        assert set(dag.tags) == expected_tags

    def test_dag_tasks_count(self, dagbag):
        """Verificar número de tasks"""
        dag = dagbag.get_dag(dag_id="ecommerce_etl_pipeline")
        assert len(dag.tasks) == 5

    def test_dag_task_ids(self, dagbag):
        """Verificar IDs das tasks"""
        dag = dagbag.get_dag(dag_id="ecommerce_etl_pipeline")

        expected_task_ids = {
            "download_kaggle_dataset",
            "load_to_bronze_layer",
            "validate_bronze_layer",
            "dbt_run",
            "dbt_test",
        }
        actual_task_ids = {task.task_id for task in dag.tasks}
        assert actual_task_ids == expected_task_ids

    def test_dag_task_dependencies(self, dagbag):
        """Verificar dependências entre tasks"""
        dag = dagbag.get_dag(dag_id="ecommerce_etl_pipeline")

        download_task = dag.get_task("download_kaggle_dataset")
        load_task = dag.get_task("load_to_bronze_layer")
        validate_task = dag.get_task("validate_bronze_layer")
        dbt_run_task = dag.get_task("dbt_run")
        dbt_test_task = dag.get_task("dbt_test")

        # Verificar dependências downstream
        assert load_task in download_task.downstream_list
        assert validate_task in load_task.downstream_list
        assert dbt_run_task in validate_task.downstream_list
        assert dbt_test_task in dbt_run_task.downstream_list

        # Verificar dependências upstream
        assert download_task in load_task.upstream_list
        assert load_task in validate_task.upstream_list
        assert validate_task in dbt_run_task.upstream_list
        assert dbt_run_task in dbt_test_task.upstream_list

    def test_task_types(self, dagbag):
        """Verificar tipos de operators"""
        from airflow.operators.python import PythonOperator
        from airflow.operators.bash import BashOperator

        dag = dagbag.get_dag(dag_id="ecommerce_etl_pipeline")

        # Python operators
        assert isinstance(dag.get_task("download_kaggle_dataset"), PythonOperator)
        assert isinstance(dag.get_task("load_to_bronze_layer"), PythonOperator)
        assert isinstance(dag.get_task("validate_bronze_layer"), PythonOperator)

        # Bash operators
        assert isinstance(dag.get_task("dbt_run"), BashOperator)
        assert isinstance(dag.get_task("dbt_test"), BashOperator)

    def test_task_timeouts(self, dagbag):
        """Verificar se tasks têm timeout configurado"""
        dag = dagbag.get_dag(dag_id="ecommerce_etl_pipeline")

        download_task = dag.get_task("download_kaggle_dataset")
        assert download_task.execution_timeout == timedelta(minutes=15)

        load_task = dag.get_task("load_to_bronze_layer")
        assert load_task.execution_timeout == timedelta(minutes=10)

        validate_task = dag.get_task("validate_bronze_layer")
        assert validate_task.execution_timeout == timedelta(minutes=5)


# ============================================================================
# TESTES DAS FUNÇÕES DA DAG ecommerce_etl
# ============================================================================


class TestEcommerceETLFunctions:
    """Testes das funções Python da DAG"""

    @patch("zipfile.ZipFile")
    @patch("os.remove")
    @patch("os.path.getsize")
    @patch("os.listdir")
    @patch("os.makedirs")
    @patch("shutil.rmtree")
    @patch("os.path.exists")
    @patch("json.load")
    @patch("builtins.open")
    def test_download_kaggle_dataset_success(
        self,
        mock_file,
        mock_json_load,
        mock_exists,
        mock_rmtree,
        mock_makedirs,
        mock_listdir,
        mock_getsize,
        mock_remove,
        mock_zipfile,
        dagbag,
    ):
        """Testar função de download com mocks"""
        # Importar a função da DAG carregada
        dag = dagbag.get_dag(dag_id="ecommerce_etl_pipeline")
        download_task = dag.get_task("download_kaggle_dataset")
        download_kaggle_dataset = download_task.python_callable

        with patch.dict("sys.modules", {"kaggle": MagicMock()}):
            # Setup mocks
            def exists_side_effect(path):
                if "kaggle.json" in path:
                    return True
                if path == "/opt/airflow/data/raw":
                    return True
                if "brazilian-ecommerce.zip" in path:
                    return True
                return False

            mock_exists.side_effect = exists_side_effect
            mock_json_load.return_value = {"username": "test", "key": "test123"}
            mock_listdir.return_value = ["file1.csv", "file2.csv", "file3.csv"]
            mock_getsize.return_value = 1024 * 1024

            mock_zip = MagicMock()
            mock_zip.namelist.return_value = ["file1.csv", "file2.csv", "file3.csv"]
            mock_zipfile.return_value.__enter__.return_value = mock_zip

            # Execute
            result = download_kaggle_dataset()

            # Verificar
            assert result["csv_count"] == 3
            assert isinstance(result["total_size_mb"], float)

    @patch("sqlalchemy.create_engine")
    @patch("pandas.read_csv")
    @patch("os.listdir")
    @patch("os.path.exists")
    def test_load_to_bronze_success(
        self,
        mock_exists,
        mock_listdir,
        mock_read_csv,
        mock_create_engine,
        dagbag,
        sample_dataframe,
    ):
        """Testar função de carga na Bronze"""
        # Importar a função da DAG carregada
        dag = dagbag.get_dag(dag_id="ecommerce_etl_pipeline")
        load_task = dag.get_task("load_to_bronze_layer")
        load_to_bronze = load_task.python_callable

        # Setup mocks
        engine = MagicMock()
        connection = MagicMock()
        engine.connect.return_value.__enter__.return_value = connection
        engine.begin.return_value.__enter__.return_value = connection
        mock_create_engine.return_value = engine

        mock_exists.return_value = True
        mock_listdir.return_value = ["olist_orders_dataset.csv"]
        mock_read_csv.return_value = sample_dataframe

        result_mock = MagicMock()
        result_mock.scalar.return_value = len(sample_dataframe)
        connection.execute.return_value = result_mock

        # Execute
        result = load_to_bronze()

        # Verificar
        assert result["loaded_tables"] >= 1
        assert result["total_records"] >= 0

    @patch("sqlalchemy.create_engine")
    def test_validate_bronze_layer_success(self, mock_create_engine, dagbag):
        """Testar função de validação"""
        # Importar a função da DAG carregada
        dag = dagbag.get_dag(dag_id="ecommerce_etl_pipeline")
        validate_task = dag.get_task("validate_bronze_layer")
        validate_bronze_layer = validate_task.python_callable

        # Setup mocks
        engine = MagicMock()
        connection = MagicMock()
        engine.connect.return_value.__enter__.return_value = connection
        mock_create_engine.return_value = engine

        count_result = MagicMock()
        count_result.scalar.return_value = 100

        col_result = MagicMock()
        col_result.scalar.return_value = 5

        connection.execute.side_effect = [count_result, col_result] * 7

        # Execute
        result = validate_bronze_layer()

        # Verificar
        assert result["tables_ok"] == 7
        assert result["total_tables"] == 7
        assert result["total_records"] == 700


# ============================================================================
# TESTES DA DAG data_quality_checks
# ============================================================================


class TestDataQualityChecksDAG:
    """Testes da DAG de qualidade de dados"""

    def test_dag_loaded(self, dagbag):
        """Verificar se a DAG foi carregada"""
        dag = dagbag.get_dag(dag_id="data_quality_checks")
        assert dag is not None, "DAG data_quality_checks não foi carregada"

    def test_dag_structure(self, dagbag):
        """Verificar estrutura da DAG"""
        dag = dagbag.get_dag(dag_id="data_quality_checks")

        assert dag.dag_id == "data_quality_checks"
        assert dag.schedule_interval == "@daily"
        assert dag.catchup is False

    def test_dag_tags(self, dagbag):
        """Verificar tags"""
        dag = dagbag.get_dag(dag_id="data_quality_checks")
        assert "quality" in dag.tags
        assert "monitoring" in dag.tags

    def test_dag_tasks(self, dagbag):
        """Verificar tasks"""
        dag = dagbag.get_dag(dag_id="data_quality_checks")
        assert len(dag.tasks) == 1

        task = dag.tasks[0]
        assert task.task_id == "validate_quality"

    def test_task_type(self, dagbag):
        """Verificar tipo de operator"""
        from airflow.operators.python import PythonOperator

        dag = dagbag.get_dag(dag_id="data_quality_checks")
        task = dag.get_task("validate_quality")

        assert isinstance(task, PythonOperator)

    @patch("great_expectations.get_context")
    @patch("pandas.read_sql")
    @patch("sqlalchemy.create_engine")
    def test_validate_data_quality_function(
        self,
        mock_create_engine,
        mock_read_sql,
        mock_gx_context,
        dagbag,
        sample_dataframe,
    ):
        """Testar função de validação de qualidade"""
        # Importar a função da DAG carregada
        dag = dagbag.get_dag(dag_id="data_quality_checks")
        quality_task = dag.get_task("validate_quality")
        validate_data_quality = quality_task.python_callable

        # Setup mocks
        engine = MagicMock()
        mock_create_engine.return_value = engine
        mock_read_sql.return_value = sample_dataframe

        mock_context = MagicMock()
        mock_context.run_validation_operator.return_value = {"success": True}
        mock_gx_context.return_value = mock_context

        # Execute (capturar print)
        with patch("builtins.print"):
            validate_data_quality()

        # Verificar que foi chamado
        mock_read_sql.assert_called_once()
        mock_gx_context.assert_called_once()


# ============================================================================
# TESTES DE IMPORT E SYNTAX
# ============================================================================


class TestDAGImports:
    """Testes de imports e syntax errors"""

    def test_no_import_errors(self, dagbag):
        """Verificar que não há erros de import"""
        assert (
            len(dagbag.import_errors) == 0
        ), f"Erros encontrados: {dagbag.import_errors}"

    def test_all_dags_loaded(self, dagbag):
        """Verificar que todas as DAGs esperadas foram carregadas"""
        expected_dags = {"ecommerce_etl_pipeline", "data_quality_checks"}
        loaded_dags = set(dagbag.dag_ids)

        assert expected_dags.issubset(
            loaded_dags
        ), f"DAGs faltando: {expected_dags - loaded_dags}"


# ============================================================================
# TESTES DE CICLOS E VALIDAÇÃO
# ============================================================================


class TestDAGValidation:
    """Testes de validação de DAGs"""

    def test_no_cycles_in_dag(self, dagbag):
        """Verificar que não há ciclos na DAG"""
        for dag_id in dagbag.dag_ids:
            dag = dagbag.get_dag(dag_id)
            # Se houver ciclo, isto levantará uma exceção
            dag.test_cycle()

    def test_dag_start_date(self, dagbag):
        """Verificar que start_date está definido"""
        dag = dagbag.get_dag(dag_id="ecommerce_etl_pipeline")
        assert dag.default_args["start_date"] == datetime(2024, 1, 1)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
