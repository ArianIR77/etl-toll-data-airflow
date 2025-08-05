# Import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments
default_args = {
    'owner': 'Arian',
    'start_date': days_ago(0),
    'email': ['dummy@gmail.com'],
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
    schedule_interval=timedelta(days=1),
)

# define the tasks

# define the first task
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment',
    dag=dag
)


# second task
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv',
    dag=dag
)


# third task
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f8-10 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv',
    dag=dag
)

# task 4
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c59-61,63-67 /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv',
    dag=dag
)



# task 5
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d "," ' \
                 '/home/project/airflow/dags/finalassignment/staging/csv_data.csv ' \
                 '/home/project/airflow/dags/finalassignment/staging/tsv_data.csv ' \
                 '/home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv ' \
                 '> /home/project/airflow/dags/finalassignment/staging/extracted_data.csv',
    dag=dag
)


# task 6
transform_data = BashOperator(
    task_id='transform_data',
    bash_command="""
        awk -F',' '{
            $4 = toupper($4);
            OFS=","; print
        }' /home/project/airflow/dags/finalassignment/staging/extracted_data.csv \
        > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv
    """,
    dag=dag
)


# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
