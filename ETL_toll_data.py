from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args={
    'owner':'blink',
    'start_date':datetime.today(),
    'email': 'dummy@example.com',
    'email_on_failure': True,
    'email_on_retry':True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag=DAG(
    'ETL_toll_data',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    description='Apache Airflow Final Assignment'
)

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/',
    dag=dag
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d "," -f 1,2,3,4 /path/to/vehicle-data.csv > /path/to/csv_data.csv',
    dag=dag,
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -d "\\t" -f 1,2,3 /path/to/tollplaza-data.tsv > /path/to/tsv_data.csv',
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c 1-10,11-20 /path/to/payment-data.txt > /path/to/fixed_width_data.csv',
    dag=dag,
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d "," /path/to/csv_data.csv /path/to/tsv_data.csv /path/to/fixed_width_data.csv > /path/to/extracted_data.csv',
    dag=dag,
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='awk -F "," \'{ $4=toupper($4); print }\' /path/to/extracted_data.csv > /path/to/transformed_data.csv',
    dag=dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
