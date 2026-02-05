from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import logging

# Configurar logger
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

def download_kaggle_dataset():
    """Download dataset do Kaggle com validação robusta"""
    import json
    import zipfile
    import shutil
    
    try:
        import kaggle
    except ImportError:
        raise ImportError("Kaggle não está instalado. Execute: pip install kaggle")
    
    logger.info("="*70)
    logger.info("INICIANDO DOWNLOAD DO KAGGLE")
    logger.info("="*70)
    
    # Configurar credenciais
    kaggle_config_path = '/opt/airflow/.kaggle/kaggle.json'
    
    if not os.path.exists(kaggle_config_path):
        raise FileNotFoundError(
            f"❌ kaggle.json não encontrado em {kaggle_config_path}\n"
            f"Verifique se o volume está montado corretamente no docker-compose.yml"
        )
    
    try:
        with open(kaggle_config_path, 'r') as f:
            kaggle_creds = json.load(f)
            os.environ['KAGGLE_USERNAME'] = kaggle_creds['username']
            os.environ['KAGGLE_KEY'] = kaggle_creds['key']
        
        logger.info(f"✅ Credenciais carregadas: {kaggle_creds['username']}")
    except Exception as e:
        raise Exception(f"❌ Erro ao ler kaggle.json: {str(e)}")
    
    # Preparar diretórios
    data_dir = '/opt/airflow/data/raw'
    
    # Limpar diretório se existir (começar do zero)
    if os.path.exists(data_dir):
        logger.info(f"🗑️  Limpando diretório {data_dir}...")
        shutil.rmtree(data_dir)
    
    os.makedirs(data_dir, exist_ok=True)
    logger.info(f"📁 Diretório criado: {data_dir}")
    
    # Download do dataset
    dataset_name = 'olistbr/brazilian-ecommerce'
    zip_path = f"{data_dir}/brazilian-ecommerce.zip"
    
    try:
        logger.info(f"📥 Baixando dataset: {dataset_name}")
        logger.info("   (Isso pode demorar 2-5 minutos dependendo da conexão...)")
        
        kaggle.api.dataset_download_files(
            dataset_name,
            path=data_dir,
            unzip=False
        )
        
        logger.info("✅ Download concluído!")
        
    except Exception as e:
        raise Exception(f"❌ Erro ao baixar dataset: {str(e)}")
    
    # Verificar se ZIP foi baixado
    if not os.path.exists(zip_path):
        files_in_dir = os.listdir(data_dir)
        raise FileNotFoundError(
            f"❌ ZIP não encontrado em {zip_path}\n"
            f"Arquivos no diretório: {files_in_dir}"
        )
    
    zip_size_mb = os.path.getsize(zip_path) / 1024 / 1024
    logger.info(f"📦 ZIP encontrado: {zip_size_mb:.2f} MB")
    
    # Descompactar
    try:
        logger.info("📦 Descompactando arquivo...")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Listar arquivos no ZIP
            file_list = zip_ref.namelist()
            logger.info(f"   Arquivos no ZIP: {len(file_list)}")
            
            # Extrair tudo
            zip_ref.extractall(data_dir)
        
        logger.info("✅ Descompactação concluída!")
        
        # Remover ZIP
        os.remove(zip_path)
        logger.info("🗑️  Arquivo ZIP removido")
        
    except zipfile.BadZipFile:
        raise Exception(f"❌ Arquivo ZIP corrompido em {zip_path}")
    except Exception as e:
        raise Exception(f"❌ Erro ao descompactar: {str(e)}")
    
    # Validar arquivos extraídos
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    
    if len(csv_files) == 0:
        all_files = os.listdir(data_dir)
        raise Exception(
            f"❌ Nenhum arquivo CSV foi extraído!\n"
            f"Arquivos encontrados: {all_files}"
        )
    
    logger.info(f"\n✅ Dataset pronto! {len(csv_files)} arquivos CSV:")
    total_size_mb = 0
    for f in sorted(csv_files):
        size_mb = os.path.getsize(f"{data_dir}/{f}") / 1024 / 1024
        total_size_mb += size_mb
        logger.info(f"   📄 {f:45} {size_mb:>8.2f} MB")
    
    logger.info(f"\n📊 Tamanho total: {total_size_mb:.2f} MB")
    logger.info("="*70)
    
    return {
        'csv_count': len(csv_files),
        'total_size_mb': round(total_size_mb, 2)
    }


def load_to_bronze():
    """Carregar dados brutos para camada Bronze com validação robusta"""
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    
    logger.info("="*70)
    logger.info("INICIANDO CARGA NA CAMADA BRONZE")
    logger.info("="*70)
    
    # Conectar ao PostgreSQL
    try:
        engine = sqlalchemy.create_engine(
            'postgresql://airflow:airflow@postgres:5432/warehouse',
            pool_pre_ping=True,  # Verificar conexão antes de usar
            echo=False
        )
        
        # Testar conexão
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        logger.info("✅ Conexão PostgreSQL estabelecida")
        
    except Exception as e:
        raise Exception(f"❌ Erro ao conectar PostgreSQL: {str(e)}")
    
    # Criar schema
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        logger.info("✅ Schema bronze criado/verificado")
    except Exception as e:
        raise Exception(f"❌ Erro ao criar schema: {str(e)}")
    
    # Verificar arquivos
    data_path = '/opt/airflow/data/raw'
    
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"❌ Diretório não existe: {data_path}")
    
    csv_files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
    
    if len(csv_files) == 0:
        raise FileNotFoundError(
            f"❌ Nenhum arquivo CSV em {data_path}\n"
            f"Execute a task de download primeiro!"
        )
    
    logger.info(f"\n📁 Arquivos CSV disponíveis: {len(csv_files)}")
    
    # Mapear arquivos para tabelas
    tables = {
        'olist_customers_dataset.csv': 'customers',
        'olist_orders_dataset.csv': 'orders',
        'olist_order_items_dataset.csv': 'order_items',
        'olist_products_dataset.csv': 'products',
        'olist_sellers_dataset.csv': 'sellers',
        'olist_order_payments_dataset.csv': 'payments',
        'olist_order_reviews_dataset.csv': 'reviews',
    }
    
    loaded_tables = []
    failed_tables = []
    total_records = 0
    
    for csv_file, table_name in tables.items():
        file_path = f"{data_path}/{csv_file}"
        
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 TABELA: bronze.{table_name}")
        logger.info(f"   Arquivo: {csv_file}")
        
        if not os.path.exists(file_path):
            logger.warning(f"   ⚠️  Arquivo não encontrado, pulando...")
            failed_tables.append((table_name, "Arquivo não encontrado"))
            continue
        
        try:
            # Ler CSV
            logger.info(f"   📖 Lendo CSV...")
            df = pd.read_csv(file_path, low_memory=False)
            
            rows = len(df)
            cols = len(df.columns)
            logger.info(f"   ✅ Lido: {rows:,} linhas × {cols} colunas")
            
            # Adicionar metadados
            df['loaded_at'] = datetime.now()
            
            # Carregar no PostgreSQL
            logger.info(f"   💾 Inserindo em bronze.{table_name}...")
            
            start_time = datetime.now()
            
            df.to_sql(
                name=table_name,
                con=engine,
                schema='bronze',
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=5000
            )
            
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"   ✅ Inserção concluída em {elapsed:.1f}s")
            
            # Verificar
            with engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT COUNT(*) FROM bronze.{table_name}")
                )
                count = result.scalar()
                
                if count == rows:
                    logger.info(f"   ✅ Verificado: {count:,} registros")
                    loaded_tables.append(table_name)
                    total_records += count
                else:
                    logger.warning(
                        f"   ⚠️  Discrepância: esperava {rows:,}, "
                        f"encontrado {count:,}"
                    )
            
        except Exception as e:
            error_msg = str(e)[:100]
            logger.error(f"   ❌ ERRO: {error_msg}")
            failed_tables.append((table_name, error_msg))
            
            import traceback
            logger.error(traceback.format_exc())
    
    # Resumo
    logger.info(f"\n{'='*70}")
    logger.info("📊 RESUMO DA CARGA")
    logger.info(f"{'='*70}")
    logger.info(f"✅ Sucesso: {len(loaded_tables)}/{len(tables)} tabelas")
    logger.info(f"📈 Total: {total_records:,} registros carregados")
    
    if failed_tables:
        logger.warning(f"\n⚠️  Falhas: {len(failed_tables)}")
        for table, error in failed_tables:
            logger.warning(f"   - {table}: {error}")
    
    # Listar tabelas criadas
    logger.info(f"\n🔍 Tabelas no schema bronze:")
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT tablename FROM pg_tables "
            "WHERE schemaname = 'bronze' "
            "ORDER BY tablename"
        ))
        bronze_tables = [row[0] for row in result]
        
        for table in bronze_tables:
            count_result = conn.execute(
                text(f"SELECT COUNT(*) FROM bronze.{table}")
            )
            count = count_result.scalar()
            logger.info(f"   ✅ {table:20} {count:>10,} registros")
    
    logger.info("="*70)
    
    if len(loaded_tables) == 0:
        raise Exception("❌ CRÍTICO: Nenhuma tabela foi carregada!")
    
    return {
        'loaded_tables': len(loaded_tables),
        'total_records': total_records,
        'failed_tables': len(failed_tables)
    }


def validate_bronze_layer():
    """Validar camada Bronze com verificações robustas"""
    import sqlalchemy
    from sqlalchemy import text
    
    logger.info("="*70)
    logger.info("VALIDANDO CAMADA BRONZE")
    logger.info("="*70)
    
    try:
        engine = sqlalchemy.create_engine(
            'postgresql://airflow:airflow@postgres:5432/warehouse',
            pool_pre_ping=True
        )
    except Exception as e:
        raise Exception(f"❌ Erro ao conectar: {str(e)}")
    
    expected_tables = [
        'customers', 'orders', 'order_items',
        'products', 'sellers', 'payments', 'reviews'
    ]
    
    logger.info(f"\n📋 Verificando {len(expected_tables)} tabelas esperadas:\n")
    
    results = {}
    total_records = 0
    tables_ok = 0
    
    with engine.connect() as conn:
        for table in expected_tables:
            try:
                # Contar registros
                result = conn.execute(
                    text(f"SELECT COUNT(*) FROM bronze.{table}")
                )
                count = result.scalar()
                
                # Verificar colunas
                col_result = conn.execute(text(
                    f"SELECT COUNT(*) FROM information_schema.columns "
                    f"WHERE table_schema = 'bronze' AND table_name = '{table}'"
                ))
                col_count = col_result.scalar()
                
                status = "✅" if count > 0 else "⚠️"
                logger.info(
                    f"   {status} bronze.{table:20} "
                    f"{count:>10,} registros | {col_count:>2} colunas"
                )
                
                results[table] = {'count': count, 'columns': col_count}
                total_records += count
                
                if count > 0:
                    tables_ok += 1
                    
            except Exception as e:
                logger.error(f"   ❌ bronze.{table:20} ERRO: {str(e)[:50]}")
                results[table] = {'count': 0, 'error': str(e)}
    
    # Resumo final
    logger.info(f"\n{'='*70}")
    logger.info("📊 RESULTADO DA VALIDAÇÃO")
    logger.info(f"{'='*70}")
    logger.info(f"   Tabelas OK: {tables_ok}/{len(expected_tables)}")
    logger.info(f"   Total de registros: {total_records:,}")
    
    # Estatísticas por tabela
    logger.info(f"\n📈 Distribuição de dados:")
    for table, data in results.items():
        if 'count' in data and data['count'] > 0:
            percentage = (data['count'] / total_records) * 100
            logger.info(f"   {table:20} {percentage:>6.2f}%")
    
    logger.info("="*70)
    
    # Decisão final
    if tables_ok == 0:
        raise ValueError(
            "❌ CRÍTICO: Nenhuma tabela acessível no schema bronze!"
        )
    elif tables_ok < len(expected_tables):
        logger.warning(
            f"⚠️  ATENÇÃO: Apenas {tables_ok}/{len(expected_tables)} "
            f"tabelas disponíveis"
        )
        logger.info("   Prosseguindo mesmo assim...")
    else:
        logger.info("✅ VALIDAÇÃO COMPLETA: Todas as tabelas OK!")
    
    return {
        'tables_ok': tables_ok,
        'total_tables': len(expected_tables),
        'total_records': total_records
    }


# Definição da DAG
with DAG(
    'ecommerce_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL E-commerce com Airflow + DBT',
    schedule_interval='@daily',
    catchup=False,
    tags=['ecommerce', 'etl', 'bronze', 'silver', 'gold'],
    max_active_runs=1,  # Apenas 1 execução por vez
) as dag:

    # Task 1: Download do Kaggle
    download_data = PythonOperator(
        task_id='download_kaggle_dataset',
        python_callable=download_kaggle_dataset,
        execution_timeout=timedelta(minutes=15),  # Timeout de 15 min
    )

    # Task 2: Carregar para Bronze
    load_bronze = PythonOperator(
        task_id='load_to_bronze_layer',
        python_callable=load_to_bronze,
        execution_timeout=timedelta(minutes=10),
    )

    # Task 3: Validar Bronze
    validate_bronze = PythonOperator(
        task_id='validate_bronze_layer',
        python_callable=validate_bronze_layer,
        execution_timeout=timedelta(minutes=5),
    )

    # Task 4: Rodar DBT (transformações Silver/Gold)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir . --full-refresh',
        execution_timeout=timedelta(minutes=10),
    )

    # Task 5: Testes DBT
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_project && dbt test --profiles-dir .',
        execution_timeout=timedelta(minutes=5),
    )

    # Dependências
    download_data >> load_bronze >> validate_bronze >> dbt_run >> dbt_test