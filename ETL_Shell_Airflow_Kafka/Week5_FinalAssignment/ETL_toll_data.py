from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import datetime as dt

default_args = {
    'owner': 'jalvi',
    'start_date':  dt.datetime(2022, 9, 9),
    'email': ['joaopaulonobregaalvim@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily',
)

unzip_data = BashOperator(
    task_id='unzip',
    bash_command='tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id="extract_csv",
    bash_command="sudo chmod 777 /home/project/airflow/dags/finalassignment | cut -d',' -f1,2,3,4  /home/project/airflow/dags/finalassignment/vehicle-data.csv >  /home/project/airflow/dags/finalassignment/csv_data.csv",
    dag=dag

)

extract_data_from_tsv = BashOperator(

    task_id = "extract_tsv",
    bash_command="cut -d$'\t'  -f5,6,7  /home/project/airflow/dags/finalassignment/tollplaza-data.tsv  --output-delimiter=',' > /home/project/airflow/dags/finalassignment/tsv_data.csv",
    dag=dag
)

extract_data_from_fixed_width = BashOperator(

    task_id = "extract_fixed_width",
    bash_command="cut -b 49-57,59-61 /home/project/airflow/dags/finalassignment/payment-data.txt --output-delimiter=','  > /home/project/airflow/dags/finalassignment/fixed_width_data.csv",
    dag=dag
)

consolidate_data = BashOperator(
task_id = "consolidate",
bash_command = "paste -d ',' /home/project/airflow/dags/finalassignment/csv_data.csv  /home/project/airflow/dags/finalassignment/fixed_width_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv  > /home/project/airflow/dags/finalassignment/extracted_data.csv",
dag = dag
)

transform_data = BashOperator(
    task_id="transform",
    bash_command = "awk '$5 = toupper($5)' /home/project/airflow/dags/finalassignment/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv",
    dag=dag
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data