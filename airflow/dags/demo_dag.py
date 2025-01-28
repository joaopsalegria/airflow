import datetime
import pendulum

from airflow.decorators import dag, task, task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable


@dag(
    dag_id="DEMO_DAG",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def DemoDag():
    
    @task
    def tesFunction():
        testVariable = Variable.get("test_variable")
        print(f"Hello World {testVariable}")
        
    @task_group()
    def initData():
        create_pet_table = SQLExecuteQueryOperator(
            task_id="create_pet_table",
            conn_id="my_prod_database",
            sql="""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'pet')
                BEGIN
                    CREATE TABLE pet (
                        pet_id INT IDENTITY(1,1) PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        pet_type VARCHAR(255) NOT NULL,
                        birth_date DATE NOT NULL,
                        owner VARCHAR(255) NOT NULL
                    );
                END
            """,
        )
        populate_pet_table = SQLExecuteQueryOperator(
            task_id="populate_pet_table",
            conn_id="my_prod_database",
            sql="""
                IF (SELECT COUNT(*) FROM pet) = 0
                BEGIN
                    INSERT INTO pet (name, pet_type, birth_date, owner)
                        VALUES 
                            ('Max', 'Dog', '2018-07-05', 'Jane'),
                            ('Susie', 'Cat', '2019-05-01', 'Phil'),
                            ('Lester', 'Hamster', '2020-06-23', 'Lily'),
                            ('Quincy', 'Parrot', '2013-08-11', 'Anne');
                END
                """,
        )
        create_dog_sum_table = SQLExecuteQueryOperator(
            task_id="create_dog_sum_table",
            conn_id="my_prod_database",
            sql="""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dog_sum')
                BEGIN
                    CREATE TABLE dog_sum (
                        pet_sum_id INT IDENTITY(1,1) PRIMARY KEY,
                        size INT NOT NULL,
                        date DATETIME NOT NULL
                    );
                END
            """,
        )
        
        create_pet_table >> populate_pet_table >> create_dog_sum_table
        
    count_dogs = SQLExecuteQueryOperator(
        task_id="count_dogs",
        conn_id="my_prod_database", 
        sql="SELECT COUNT(*) FROM pet WHERE pet_type = 'Dog';"
    )
    
    insert_into_dog_sum = SQLExecuteQueryOperator(
        task_id="insert_into_dog_sum",
        conn_id="my_prod_database",
        sql="INSERT INTO dog_sum (size, date) VALUES (1, GETDATE());"
    )

    tesFunction() >> initData() >> count_dogs >> insert_into_dog_sum


dag = DemoDag()