from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Runs a data quality check by executing a simple SQL COUNT(*) query on all tables
    
    redshift_conn_id: Redshift connection ID
    test_tables: Tables on which quality and analysis in done
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id = "",
                test_tables = [],
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_tables = test_tables

    def execute(self, context):
        self.log.info("Getting credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.test_tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")  
            
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f"{table} returned no results")
                raise ValueError(f"Data quality check failed. {table} returned no results")
                
            num_records = records[0][0]
            if num_records == 0:
                self.log.error(f"No records present in destination table {table}")
                raise ValueError(f"No records present in destination {table}")
        
            self.log.info(f"Data analysis on {table} shows {num_records} results")

        self.log.info("Data quality check passed")
