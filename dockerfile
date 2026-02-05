FROM apache/airflow:2.8.1-python3.11

USER airflow

# Instalar tudo de uma vez respeitando dependências
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

# Criar diretório para kaggle
RUN mkdir -p /opt/airflow/.kaggle