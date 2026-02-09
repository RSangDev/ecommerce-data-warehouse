"""
Testes para as DAGs do Airflow - E-commerce ETL Pipeline
Executar com: pytest tests/test_airflow_dags.py -v

NOTA: Estes testes NÃO precisam do Airflow instalado!
"""

import pytest
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
# Adicionar diretório tests ao path para importar etl_functions
#sys.path.insert(0, os.path.dirname(__file__))


# ============================================================================
# FIXTURES
# ============================================================================


@pytest.fixture
def mock_sqlalchemy_engine():
    """Mock da conexão SQLAlchemy"""
    engine = MagicMock()
    connection = MagicMock()

    # Mock para context manager
    engine.connect.return_value.__enter__.return_value = connection
    engine.begin.return_value.__enter__.return_value = connection

    return engine, connection


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
# TESTES DA FUNÇÃO download_kaggle_dataset
# ============================================================================


class TestDownloadKaggleDataset:
    """Testes da função de download do Kaggle"""

    @patch("etl_functions.zipfile.ZipFile")
    @patch("etl_functions.os.remove")
    @patch("etl_functions.os.path.getsize")
    @patch("etl_functions.os.listdir")
    @patch("etl_functions.os.makedirs")
    @patch("etl_functions.shutil.rmtree")
    @patch("etl_functions.os.path.exists")
    @patch("etl_functions.json.load")
    @patch("builtins.open", new_callable=mock_open)
    def test_download_success(
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
    ):
        """Teste de download bem-sucedido"""
        # Mock kaggle
        with patch.dict("sys.modules", {"kaggle": MagicMock()}):
            import etl_functions

            # Recarregar módulo com kaggle mockado
            import importlib

            importlib.reload(etl_functions)

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
            mock_json_load.return_value = {"username": "test_user", "key": "test_key"}

            # CORREÇÃO: Usar um valor específico para cada arquivo
            def getsize_side_effect(path):
                if path.endswith(".csv"):
                    return 1024 * 1024  # 1 MB por CSV
                return 10 * 1024 * 1024  # 10 MB para o ZIP

            mock_getsize.side_effect = getsize_side_effect

            # CORREÇÃO: listdir é chamado DUAS vezes
            # 1ª vez: para validar CSVs no final (linha 123 do etl_functions.py)
            # 2ª vez: dentro do loop para calcular tamanho (linha 126)
            mock_listdir.return_value = ["file1.csv", "file2.csv", "file3.csv"]

            # Mock do ZIP
            mock_zip = MagicMock()
            mock_zip.namelist.return_value = ["file1.csv", "file2.csv", "file3.csv"]
            mock_zipfile.return_value.__enter__.return_value = mock_zip

            # Execute
            result = etl_functions.download_kaggle_dataset()

            # Verificar
            assert result["csv_count"] == 3
            assert isinstance(result["total_size_mb"], float)
            assert result["total_size_mb"] > 0

    @patch("etl_functions.os.path.exists")
    def test_download_missing_credentials(self, mock_exists):
        """Teste de erro quando credenciais não existem"""
        with patch.dict("sys.modules", {"kaggle": MagicMock()}):
            import etl_functions
            import importlib

            importlib.reload(etl_functions)

            mock_exists.return_value = False

            with pytest.raises(FileNotFoundError) as exc_info:
                etl_functions.download_kaggle_dataset()

            assert "kaggle.json não encontrado" in str(exc_info.value)


# ============================================================================
# TESTES DA FUNÇÃO load_to_bronze
# ============================================================================


class TestLoadToBronze:
    """Testes da função de carga na camada Bronze"""

    @patch("etl_functions.sqlalchemy.create_engine")
    @patch("etl_functions.pd.read_csv")
    @patch("etl_functions.os.listdir")
    @patch("etl_functions.os.path.exists")
    def test_load_bronze_success(
        self,
        mock_exists,
        mock_listdir,
        mock_read_csv,
        mock_create_engine,
        mock_sqlalchemy_engine,
        sample_dataframe,
    ):
        """Teste de carga bem-sucedida"""
        import  etl_functions

        # Setup
        engine, connection = mock_sqlalchemy_engine
        mock_create_engine.return_value = engine

        mock_exists.return_value = True
        mock_listdir.return_value = [
            "olist_customers_dataset.csv",
            "olist_orders_dataset.csv",
        ]
        mock_read_csv.return_value = sample_dataframe

        # Mock para COUNT(*)
        result_mock = MagicMock()
        result_mock.scalar.return_value = len(sample_dataframe)
        connection.execute.return_value = result_mock

        # Execute
        result = etl_functions.load_to_bronze()

        # Verificar
        assert result["loaded_tables"] >= 1
        assert result["total_records"] >= 0

    @patch("etl_functions.sqlalchemy.create_engine")
    @patch("etl_functions.os.path.exists")
    def test_load_bronze_missing_directory(self, mock_exists, mock_create_engine):
        """Teste quando diretório não existe"""
        import etl_functions

        # Mock do engine para evitar erro de conexão
        engine = MagicMock()
        connection = MagicMock()
        engine.connect.return_value.__enter__.return_value = connection
        engine.begin.return_value.__enter__.return_value = connection
        mock_create_engine.return_value = engine

        # CORRIGIDO: Retornar False apenas para o diretório de dados
        mock_exists.return_value = False

        with pytest.raises(FileNotFoundError) as exc_info:
            etl_functions.load_to_bronze()

        assert "Diretório não existe" in str(exc_info.value)

    @patch("etl_functions.sqlalchemy.create_engine")
    @patch("etl_functions.os.listdir")
    @patch("etl_functions.os.path.exists")
    def test_load_bronze_no_csv_files(
        self, mock_exists, mock_listdir, mock_create_engine, mock_sqlalchemy_engine
    ):
        """Teste quando não há arquivos CSV"""
        import etl_functions

        engine, connection = mock_sqlalchemy_engine
        mock_create_engine.return_value = engine

        mock_exists.return_value = True
        mock_listdir.return_value = []

        with pytest.raises(FileNotFoundError) as exc_info:
            etl_functions.load_to_bronze()

        assert "Nenhum arquivo CSV" in str(exc_info.value)

    @patch("etl_functions.sqlalchemy.create_engine")
    def test_load_bronze_connection_error(self, mock_create_engine):
        """Teste de erro de conexão"""
        import etl_functions

        mock_create_engine.side_effect = Exception("Connection failed")

        with pytest.raises(Exception) as exc_info:
            etl_functions.load_to_bronze()

        assert "Erro ao conectar PostgreSQL" in str(exc_info.value)


# ============================================================================
# TESTES DA FUNÇÃO validate_bronze_layer
# ============================================================================


class TestValidateBronzeLayer:
    """Testes da validação da camada Bronze"""

    @patch("etl_functions.sqlalchemy.create_engine")
    def test_validate_success(self, mock_create_engine, mock_sqlalchemy_engine):
        """Teste de validação bem-sucedida"""
        import etl_functions

        engine, connection = mock_sqlalchemy_engine
        mock_create_engine.return_value = engine

        # Mock para COUNT(*) - retorna 100 registros por tabela
        count_result = MagicMock()
        count_result.scalar.return_value = 100

        # Mock para contagem de colunas - retorna 5 colunas
        col_result = MagicMock()
        col_result.scalar.return_value = 5

        # Alternar entre count e columns (14 calls = 7 tabelas x 2 queries cada)
        connection.execute.side_effect = [count_result, col_result] * 7

        # Execute
        result = etl_functions.validate_bronze_layer()

        # Verificar
        assert result["tables_ok"] == 7
        assert result["total_tables"] == 7
        assert result["total_records"] == 700  # 100 * 7

    @patch("etl_functions.sqlalchemy.create_engine")
    def test_validate_no_tables(self, mock_create_engine, mock_sqlalchemy_engine):
        """Teste quando não há tabelas"""
        import etl_functions

        engine, connection = mock_sqlalchemy_engine
        mock_create_engine.return_value = engine

        # Mock retornando 0 registros
        count_result = MagicMock()
        count_result.scalar.return_value = 0

        col_result = MagicMock()
        col_result.scalar.return_value = 0

        connection.execute.side_effect = [count_result, col_result] * 7

        # Execute e verificar
        with pytest.raises(ValueError) as exc_info:
            etl_functions.validate_bronze_layer()

        assert "Nenhuma tabela acessível" in str(exc_info.value)

    @patch("etl_functions.sqlalchemy.create_engine")
    def test_validate_connection_error(self, mock_create_engine):
        """Teste de erro de conexão"""
        import etl_functions

        mock_create_engine.side_effect = Exception("Connection timeout")

        with pytest.raises(Exception) as exc_info:
            etl_functions.validate_bronze_layer()

        assert "Erro ao conectar" in str(exc_info.value)


# ============================================================================
# TESTES DE ESTRUTURA DA DAG (sem importar Airflow)
# ============================================================================


class TestDAGStructure:
    """Testes de estrutura da DAG usando parsing de arquivo"""

    def test_dag_file_exists(self):
        """Verificar se o arquivo da DAG existe"""
        dag_path = os.path.join(PROJECT_ROOT, "airflow", "dags", "ecommerce_etl.py")
        assert os.path.exists(dag_path), f"Arquivo {dag_path} não encontrado"

    def test_dag_has_correct_imports(self):
        """Verificar imports necessários no arquivo da DAG"""
        dag_path = os.path.join(PROJECT_ROOT, "airflow", "dags", "ecommerce_etl.py")

        with open(dag_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Verificar imports essenciais
        assert "from airflow import DAG" in content
        assert "from airflow.operators.python import PythonOperator" in content
        assert "from airflow.operators.bash import BashOperator" in content

    def test_dag_has_functions(self):
        """Verificar se as funções principais existem"""
        dag_path = os.path.join(PROJECT_ROOT, "airflow", "dags", "ecommerce_etl.py")

        with open(dag_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Verificar funções
        assert "def download_kaggle_dataset():" in content
        assert "def load_to_bronze():" in content
        assert "def validate_bronze_layer():" in content

    def test_dag_default_args_present(self):
        """Verificar se default_args está definido"""
        dag_path = os.path.join(PROJECT_ROOT,"airflow", "dags", "ecommerce_etl.py")

        with open(dag_path, "r", encoding="utf-8") as f:
            content = f.read()

        assert "default_args = {" in content

        assert ('"owner":' in content or "'owner':" in content)
        assert ('"retries":' in content or "'retries':" in content)

    def test_dag_tasks_defined(self):
        """Verificar se as tasks estão definidas"""
        dag_path = os.path.join(PROJECT_ROOT, "airflow", "dags", "ecommerce_etl.py")

        with open(dag_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Verificar tasks
        assert "download_data = PythonOperator" in content
        assert "load_bronze = PythonOperator" in content
        assert "validate_bronze = PythonOperator" in content
        assert "dbt_run = BashOperator" in content
        assert "dbt_test = BashOperator" in content

    def test_dag_dependencies_defined(self):
        """Verificar se as dependências estão definidas"""
        dag_path = os.path.join(PROJECT_ROOT, "airflow", "dags", "ecommerce_etl.py")

        with open(dag_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Verificar que há dependências definidas
        assert (
            ">>" in content or "set_upstream" in content or "set_downstream" in content
        )

    def test_quality_dag_file_exists(self):
        """Verificar se o arquivo da DAG de qualidade existe"""
        dag_path = os.path.join(PROJECT_ROOT, "airflow", "dags", "data_quality_checks.py")
        assert os.path.exists(dag_path), f"Arquivo {dag_path} não encontrado"

    def test_quality_dag_has_function(self):
        """Verificar se a função de qualidade existe"""
        dag_path = os.path.join(PROJECT_ROOT, "airflow", "dags", "data_quality_checks.py")

        with open(dag_path, "r", encoding="utf-8") as f:
            content = f.read()

        assert "def validate_data_quality():" in content
        assert "great_expectations" in content


# ============================================================================
# TESTES DE EDGE CASES
# ============================================================================


class TestEdgeCases:
    """Testes de casos extremos"""

    @patch("etl_functions.sqlalchemy.create_engine")
    @patch("etl_functions.pd.read_csv")
    @patch("etl_functions.os.listdir")
    @patch("etl_functions.os.path.exists")
    def test_empty_dataframe(
        self,
        mock_exists,
        mock_listdir,
        mock_read_csv,
        mock_create_engine,
        mock_sqlalchemy_engine,
    ):
        """Teste com DataFrame vazio"""
        import etl_functions

        engine, connection = mock_sqlalchemy_engine
        mock_create_engine.return_value = engine

        # Setup
        mock_exists.return_value = True
        mock_listdir.return_value = ["olist_orders_dataset.csv"]
        mock_read_csv.return_value = pd.DataFrame()  # DataFrame vazio

        # Mock para COUNT(*)
        result_mock = MagicMock()
        result_mock.scalar.return_value = 0
        connection.execute.return_value = result_mock

        # Execute
        result = etl_functions.load_to_bronze()

        # Deve completar mas sem registros
        assert result["total_records"] == 0


# ============================================================================
# TESTES DE VALIDAÇÃO DE DADOS
# ============================================================================


class TestDataValidation:
    """Testes de validação de dados"""

    def test_sample_dataframe_structure(self, sample_dataframe):
        """Verificar estrutura do DataFrame de teste"""
        assert len(sample_dataframe) == 3
        assert "order_id" in sample_dataframe.columns
        assert "customer_id" in sample_dataframe.columns
        assert "order_status" in sample_dataframe.columns

    def test_sample_dataframe_values(self, sample_dataframe):
        """Verificar valores do DataFrame de teste"""
        assert (
            sample_dataframe["order_status"]
            .isin(["delivered", "shipped", "processing"])
            .all()
        )

    def test_sample_dataframe_not_empty(self, sample_dataframe):
        """Verificar que DataFrame não está vazio"""
        assert not sample_dataframe.empty
        assert len(sample_dataframe) > 0


# ============================================================================
# TESTES DE INTEGRAÇÃO
# ============================================================================


class TestIntegration:
    """Testes de integração (validação de workflow)"""

    def test_dag_workflow_sequence(self):
        """Verificar sequência lógica do workflow no código"""
        dag_path = os.path.join(PROJECT_ROOT, "airflow", "dags", "ecommerce_etl.py")

        with open(dag_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Encontrar posições das definições de tasks
        download_pos = content.find("download_data = PythonOperator")
        load_pos = content.find("load_bronze = PythonOperator")
        validate_pos = content.find("validate_bronze = PythonOperator")
        dbt_run_pos = content.find("dbt_run = BashOperator")
        dbt_test_pos = content.find("dbt_test = BashOperator")

        # Todas as tasks devem existir
        assert all(
            pos != -1
            for pos in [download_pos, load_pos, validate_pos, dbt_run_pos, dbt_test_pos]
        )

        # Verificar ordem de definição (não necessariamente ordem de execução,
        # mas é um indicador)
        assert download_pos < load_pos
        assert load_pos < validate_pos


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
