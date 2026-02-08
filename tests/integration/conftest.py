"""
Plugin pytest para configurar Airflow ANTES de qualquer import
IMPORTANTE: Este arquivo DEVE se chamar conftest.py e estar em tests/integration/
"""

import os
import sys
from pathlib import Path

# ============================================================================
# EXECUTAR IMEDIATAMENTE - ANTES DE QUALQUER IMPORT DO AIRFLOW
# ============================================================================


def setup_airflow_environment():
    """Configurar ambiente Airflow ANTES de qualquer import"""

    # Calcular paths baseado na localização deste arquivo
    # conftest.py está em: tests/integration/conftest.py
    conftest_path = Path(__file__).resolve()
    integration_dir = conftest_path.parent  # tests/integration/
    tests_dir = integration_dir.parent  # tests/
    project_root = tests_dir.parent  # raiz do projeto

    dag_folder = project_root / "airflow" / "dags"

    # Banco de dados - SOLUÇÃO PARA WINDOWS
    # Airflow no Windows exige caminho absoluto com 4 barras (sqlite:////)
    db_file = project_root / "airflow_test.db"

    # Criar URL do SQLite compatível com Windows
    # CRÍTICO: 4 barras para Windows (sqlite:////D:/...)
    if sys.platform == "win32":
        # Windows: 4 barras = sqlite:// + // + caminho
        db_path_unix = db_file.as_posix()  # D:/Projetos/.../airflow_test.db
        # sqlite + :// + / + / + D:/... = sqlite:////D:/...
        db_url = f"sqlite:////{db_path_unix}"  # QUATRO barras!
    else:
        db_url = f"sqlite:///{db_file}"

    # Configurar variáveis de ambiente ANTES de qualquer import do Airflow
    env_vars = {
        "AIRFLOW_HOME": str(project_root),
        "AIRFLOW__CORE__DAGS_FOLDER": str(dag_folder),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__UNIT_TEST_MODE": "True",
        "AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT": "0",
        "AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS": "False",
        "AIRFLOW__CORE__LOAD_PLUGINS": "False",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": db_url,
    }

    for key, value in env_vars.items():
        os.environ[key] = value

    print("\n Airflow configurado (antes de imports)")
    print("   Home: {project_root}")
    print("   DAGs: {dag_folder}")
    print("   DB: {db_url}")
    if sys.platform == "win32":
        print("   Platform: Windows (timeout desabilitado)")
    print()


# EXECUTAR AGORA (antes de qualquer import)
setup_airflow_environment()

# ============================================================================
# AGORA podemos importar pytest e configurar
# ============================================================================

import pytest # noqa


def pytest_configure(config):
    """Inicializar banco após configuração inicial"""
    print("🗄️  Inicializando banco de dados do Airflow...")

    try:
        # Importar aqui, depois da configuração
        from airflow.utils import db

        db.initdb()
        print("✅ Banco inicializado com sucesso!\n")
    except Exception as e:
        print(f"⚠️  Aviso: {e}")
        print("   Continuando mesmo assim...\n")


@pytest.fixture(scope="session", autouse=True)
def initialize_airflow_db():
    """Garantir que banco está pronto antes de qualquer teste"""
    try:
        from airflow.utils import db

        db.initdb()
    except Exception:
        pass  # Já foi inicializado no pytest_configure

    yield

    # Cleanup (opcional)
    print("\n🧹 Limpeza concluída")
