from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Defina os argumentos padrão
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Defina a DAG
with DAG(
    'dbt_postgres_connection_test',
    default_args=default_args,
    description='DAG simples para conectar ao PostgreSQL usando dbt',
    schedule_interval=None,  # DAG manual, sem agendamento automático
    catchup=False,
) as dag:

    # Tarefa para testar a conexão com o PostgreSQL usando o dbt
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command="""
        cd /usr/local/airflow/dags/dbt/rhmp-dbtproject-dbt &&
        dbt debug --profiles-dir /usr/local/airflow/dags/dbt
        """,
    )

    # Ordem de execução
    dbt_debug
