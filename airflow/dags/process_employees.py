import datetime
import pendulum

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable


@dag(
    dag_id="process_pets",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessPets():
    
    @task
    def tesFunction():
        testVariable = Variable.get("test_variable")
        print(f"Hello World {testVariable}")

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
            INSERT INTO pet (name, pet_type, birth_date, owner)
            VALUES 
                ('Max', 'Dog', '2018-07-05', 'Jane'),
                ('Susie', 'Cat', '2019-05-01', 'Phil'),
                ('Lester', 'Hamster', '2020-06-23', 'Lily'),
                ('Quincy', 'Parrot', '2013-08-11', 'Anne');
            """,
    )
    get_all_pets = SQLExecuteQueryOperator(task_id="get_all_pets",conn_id="my_prod_database", sql="SELECT * FROM pet;")
    get_birth_date = SQLExecuteQueryOperator(
        task_id="get_birth_date",
        conn_id="my_prod_database",
        sql="SELECT * FROM pet WHERE birth_date BETWEEN %(begin_date)s AND %(end_date)s",
        parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
        hook_params={"options": "-c statement_timeout=3000ms"},
    )

    tesFunction() >> create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date


dag = ProcessPets()