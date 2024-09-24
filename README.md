# ETL-Pipeline-for-Toll-Traffic-Data-Extracting-and-Consolidating-Files-Using-Apache-Airflow
This project demonstrates how to build an ETL (Extract, Transform, Load) pipeline using Apache Airflow's BashOperator to process and consolidate toll traffic data from multiple formats, including CSV, TSV, and fixed-width files. The final output is a unified CSV file that can be used for further traffic data analysis.


### Project Scenario

This project requires us with decongesting national highways by analyzing traffic data from various toll plazas, each using different IT systems. The goal is to collect, transform, and consolidate the traffic data into a uniform format.

### Objectives

- Extract data from CSV, TSV, and fixed-width formatted files.
- Perform data transformation.
- Consolidate the transformed data into a staging area.
- Automate these tasks using Apache Airflow and BashOperator.

## Prerequisites

- Apache Airflow installed and running.
- Access to a terminal and text editor.
- Basic knowledge of bash commands and DAGs in Apache Airflow.


### Exercise 1: Set Up the Lab Environment

1. **Start Apache Airflow:**
   Ensure that Apache Airflow is running in your environment. Open a terminal and run the following commands:
   ```bash
   airflow webserver --port 8080
   airflow scheduler
   ```

2. **Create Staging Directory:**
   In your terminal, create a staging directory for this project:
   ```bash
   sudo mkdir -p /home/project/airflow/dags/finalassignment/staging
   ```

3. **Set Directory Permissions:**
   Adjust permissions for the directory:
   ```bash
   sudo chmod -R 777 /home/project/airflow/dags/finalassignment
   ```

4. **Download the Dataset:**
   Download the traffic data file using `curl`:
   ```bash
   sudo curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz -o /home/project/airflow/dags/finalassignment/tolldata.tgz
   ```

### Exercise 2: Create DAG and Arguments

1. **Create a Python File for the DAG:**
   In the project directory, create a new file named `ETL_toll_data.py` and open it in your preferred text editor.

2. **Import Required Libraries:**
   Start by importing necessary Airflow packages in `ETL_toll_data.py`:
   ```python
   from datetime import datetime, timedelta
   from airflow import DAG
   from airflow.operators.bash_operator import BashOperator
   ```

3. **Define DAG Arguments:**
   Define the default arguments for the DAG:
   ```python
   default_args = {
       'owner': 'your_name',
       'start_date': datetime(2024, 9, 24),
       'email_on_failure': True,
       'email_on_retry': True,
       'retries': 1,
       'retry_delay': timedelta(minutes=5),
   }
   ```

4. **DAG Definition:**
   Define the DAG with the parameters:
   ```python
   dag = DAG(
       'ETL_toll_data',
       default_args=default_args,
       description='Apache Airflow Final Assignment',
       schedule_interval='@daily',
   )
   ```

### Exercise 3: Create Tasks Using BashOperator

1. **Unzip Data:**
   Create a task to unzip the data:
   ```python
   unzip_data = BashOperator(
       task_id='unzip_data',
       bash_command='tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/',
       dag=dag
   )
   ```

2. **Extract Data from CSV:**
   Extract specific fields from the CSV file:
   ```python
   extract_data_from_csv = BashOperator(
       task_id='extract_data_from_csv',
       bash_command='cut -d"," -f1,2,3,4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv',
       dag=dag
   )
   ```

3. **Extract Data from TSV:**
   Extract fields from the TSV file:
   ```python
   extract_data_from_tsv = BashOperator(
       task_id='extract_data_from_tsv',
       bash_command='cut -f5,6,7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/tsv_data.csv',
       dag=dag
   )
   ```

4. **Extract Data from Fixed-Width File:**
   Extract fields from a fixed-width file:
   ```python
   extract_data_from_fixed_width = BashOperator(
       task_id='extract_data_from_fixed_width',
       bash_command='cut -c 1-10,11-20 /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv',
       dag=dag
   )
   ```

5. **Consolidate Data:**
   Consolidate data from all extracted files:
   ```python
   consolidate_data = BashOperator(
       task_id='consolidate_data',
       bash_command='paste /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv',
       dag=dag
   )
   ```

6. **Transform Data:**
   Transform the `vehicle_type` field to uppercase:
   ```python
   transform_data = BashOperator(
       task_id='transform_data',
       bash_command="tr '[:lower:]' '[:upper:]' < /home/project/airflow/dags/finalassignment/extracted_data.csv > /home/project/airflow/dags/finalassignment/transformed_data.csv",
       dag=dag
   )
   ```

### Exercise 4: Define Task Pipeline

Create dependencies between tasks in the DAG:
```python
unzip_data >> [extract_data_from_csv, extract_data_from_tsv, extract_data_from_fixed_width] >> consolidate_data >> transform_data
```

### Exercise 5: Submit, Unpause, and Trigger the DAG


1. **Submit the DAG:**
   Ensure your DAG file is located in the `dags/` folder in Airflow. Then, submit it by restarting Airflow:
   ```bash
   export AIRFLOW_HOME=/home/project/airflow
   cp my_first_dag.py $AIRFLOW_HOME/dags
   ```

   ```bash
   airflow dags list
   ```
 
3. **Unpause and Trigger the DAG:**
   In the terminal, unpause and trigger the DAG:
   ```bash
   airflow dags unpause ETL_toll_data
   airflow dags trigger ETL_toll_data
   ```

4. **Check DAG Status:**
   To check if the DAG is running properly:
   ```bash
   airflow tasks list ETL_toll_data
   airflow tasks list ETL_toll_data --tree
   ```

5. **Check Task Runs:**
   To verify that all tasks have run successfully, use:
   ```bash
   airflow tasks run ETL_toll_data unzip_data
   airflow tasks run ETL_toll_data extract_data_from_csv
   airflow tasks run ETL_toll_data extract_data_from_tsv
   airflow tasks run ETL_toll_data extract_data_from_fixed_width
   airflow tasks run ETL_toll_data consolidate_data
   airflow tasks run ETL_toll_data transform_data
   ```

6. **View DAG Run History:**
   Use the Web UI or CLI to view the DAG run history:
   ```bash
   airflow dags list-runs -d ETL_toll_data
   ```
We can monitor the progress of the DAGs through the Airflow Web UI or CLI, ensuring that all tasks complete successfully.
