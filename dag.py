from datetime import datetime
import os

from airflow import DAG
from includes.scripts.python.on_callback_airflow import on_callback, on_callback_success
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from includes.scripts.python.functions.environment import get_environment_clickhouse, get_environment_greenplum
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator


dir_path = os.path.dirname(__file__)
_environment = Variable.get('airflow_environment').lower()
clickhouse_env = get_environment_clickhouse(_environment)
clickhouse_connection = BaseHook.get_connection(clickhouse_env['cluster_connection'])
greenplum_env = get_environment_greenplum(_environment)
greenplum__connection = BaseHook.get_connection(greenplum_env['connection_id'])

doc_md = """
    Данный DAG загружает данные таблицы из Clickhouse в Greenplum.

        {
            "src_query": "SELECT order_number, checkpoint_number, is_in_checkpoint, checkpoint_start_date, checkpoint_end_date, refresh_changed FROM bi.f_control_points_main_distributed WHERE is_in_checkpoint <> TRUE",
            "dst_table": "f_control_points_archive",
            "dst_schema": "temp"
        }
    
    Таблица-приемник должна существовать в GP и ее схема должна соответствовать запросу CH src_query.
"""

dag_default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'owner': 'bi',
    'catchup': False,
    'on_retry_callback': on_callback,
    'on_success_callback': on_callback_success,
}

with DAG(
    dag_id='upload_data_from_CH_to_GP_PG',
    description='Данный DAG загружает данные таблицы из Clickhouse в Greenplum',
    schedule_interval=None,
    max_active_runs=1,
    default_args=dag_default_args,
    tags=['clickhouse', 'greenplum'],
    doc_md=doc_md,
    on_failure_callback=on_callback,
    template_searchpath = [dir_path],
) as dag:

    start = EmptyOperator(task_id="start")

    def clear_export_dir(task_id):
        return SSHOperator(
            task_id=task_id,
            command=f'rm -rf {Variable.get("rdw_db_export_path")}/ch; mkdir {Variable.get("rdw_db_export_path")}/ch',
            ssh_conn_id='rdw_storage_server',
            cmd_timeout=None,
        )

    data_export = SSHOperator(
        task_id='data_export',
        command='export.sh',
        params={
            'host': clickhouse_connection.host,
            'login': clickhouse_connection.login,
            'password': clickhouse_connection.password,
            'base_dir': Variable.get("rdw_db_export_path"),
        },
        ssh_conn_id='rdw_storage_server',
        cmd_timeout=None,
    )

    data_import = SSHOperator(
        task_id='data_import',
        command=f'import.sh',
        params={
            'host': greenplum__connection.host,
            'port': greenplum__connection.port,
            'login': greenplum__connection.login,
            'password': greenplum__connection.password,
            'base_dir': Variable.get("rdw_db_export_path"),
        },
        ssh_conn_id='rdw_storage_server',
        cmd_timeout=None,
    )

    end = EmptyOperator(task_id="end")

    start >> clear_export_dir('clear_export_dir_before') >> data_export
    data_export >> data_import >> clear_export_dir('clear_export_dir_after') >> end