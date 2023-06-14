import datetime

import pandas as pd
import snowflake.connector

from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from snowflake.connector.pandas_tools import write_pandas, pd_writer
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

engine = create_engine(URL(
    user=Variable.get('USER'),
    password=Variable.get('PASSWORD'),
    account=Variable.get('ACCOUNT'),
    warehouse=Variable.get('WAREHOUSE'),
    database=Variable.get('DATABASE'),
    schema=Variable.get('SCHEMA'),
    region=Variable.get('REGION')
))

con = engine.connect()

AIRFLOW_HOME = "/opt/airflow"
DATA_INPUT = f"{AIRFLOW_HOME}/data/raw"
DATA_OUTPUT = f"{AIRFLOW_HOME}/data/proc"
FILENAME = "763K_plus_IOS_Apps_Info.csv"

DATABASE_NAME = "TASK_8_DB"
SCHEMA_NAME = "TASK_8_SCHEMA"

conn = snowflake.connector.connect(
    user=Variable.get('USER'),
    password=Variable.get('PASSWORD'),
    account=Variable.get('ACCOUNT'),
    warehouse=Variable.get('WAREHOUSE'),
    database=Variable.get('DATABASE'),
    schema=Variable.get('SCHEMA'),
    region=Variable.get('REGION')
)

cursor = conn.cursor()


@dag(
    dag_id="task_8_dag",
    start_date=datetime.datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    tags=["snowflake", "pandas"],
)
def task_8_dag():
    @task(task_id="get_columns_names")
    def get_columns_names(file, **kwargs):
        df = pd.read_csv(file, nrows=20)
        df.columns = map(lambda x: str(x).upper(), df.columns)
        columns_names_without_types = ''.join([f'{i} string, ' for i in df.columns])[:-2]
        # columns_names_without_types = ''.join([f'{i}, ' for i in df.columns])[:-2]
        # columns_names_with_types = ''
        # types = list(df.dtypes.replace('int64', 'int').replace('float64', 'float').astype(str).to_dict().values())
        # columns = list(df.dtypes.replace('int64', 'int').replace('float64', 'float').astype(str).to_dict().keys())
        # for i in range(0, len(columns)):
        #     columns_names_with_types += f'{columns[i]} {types[i]}, '
        # columns_names_with_types = columns_names_with_types[:-2]
        ti = kwargs["ti"]
        ti.xcom_push("columns_names_without_types", columns_names_without_types)
        ti.xcom_push("data", df.to_json())
        # ti.xcom_push("columns_names_with_types", columns_names_with_types)

    @task(task_id="create_tables_and_streams", do_xcom_push=True)
    def create_tables_and_streams(**kwargs):
        ti = kwargs["ti"]
        columns_names = ti.xcom_pull(task_ids="get_columns_names", key="columns_names_without_types")
        create_raw_table = f"""
                CREATE OR REPLACE TABLE {DATABASE_NAME}.{SCHEMA_NAME}.RAW_TABLE ({columns_names})"""

        cursor.execute(create_raw_table)

        cursor.execute("CREATE OR REPLACE TABLE STAGE_TABLE LIKE RAW_TABLE")
        cursor.execute("CREATE OR REPLACE TABLE MASTER_TABLE LIKE STAGE_TABLE")

        cursor.execute("CREATE OR REPLACE STREAM RAW_STREAM ON TABLE RAW_TABLE")
        cursor.execute("CREATE OR REPLACE STREAM STAGE_STREAM ON TABLE STAGE_TABLE")

    @task(task_id="upload_csv_to_snowflake")
    def upload_csv_to_snowflake(file, **kwargs):
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids="get_columns_names", key="data")

        data = pd.read_json(data)
        # df = pd.read_csv(file, nrows=1)
        # data = df.to_json()
        # data_end = pd.read_json(data)
        #df.to_sql(name="RAW_TABLE", con=con, if_exists="append", index=False)
        write_pandas(conn, data, table_name='RAW_TABLE')

    @task(task_id="stream_to_stage_table")
    def stream_to_stage_table(**kwargs):
        ti = kwargs["ti"]
        columns_names = ti.xcom_pull(task_ids="get_columns_names", key="columns_names_without_types")
        columns_names = columns_names.lstrip("string")
        cursor.execute(f"INSERT INTO STAGE_TABLE (SELECT {columns_names} FROM RAW_STREAM)")

    @task(task_id="stream_to_master_table")
    def stream_to_master_table(**kwargs):
        ti = kwargs["ti"]
        columns_names = ti.xcom_pull(task_ids="get_columns_names", key="columns_names_without_types")
        columns_names = columns_names.lstrip("string")
        cursor.execute(f"INSERT INTO MASTER_TABLE (SELECT {columns_names} FROM STAGE_STREAM)")

    # @task_group(group_id="create_tables")
    # def create_tables(file: str):
    #     return upload_csv_to_snowflake(create_tables_and_streams(get_columns_names(file)))
    #
    # @task_group(group_id="streaming_processes")
    # def streaming_processes():
    #     return stream_to_master_table(stream_to_stage_table())

    upload_csv_to_snowflake_task = upload_csv_to_snowflake(f"{DATA_INPUT}/{FILENAME}")
    get_columns_names_task = get_columns_names(f"{DATA_INPUT}/{FILENAME}")
    create_tables_and_streams_task = create_tables_and_streams()
    stream_to_stage_table_task = stream_to_stage_table()
    stream_to_master_table_task = stream_to_master_table()

    # create_tables(f"{DATA_INPUT}/{FILENAME}") >> upload_csv_to_snowflake_task >> streaming_processes()

    get_columns_names_task>>create_tables_and_streams_task>>upload_csv_to_snowflake_task>>stream_to_stage_table_task>>stream_to_master_table_task


task_8_dag()
