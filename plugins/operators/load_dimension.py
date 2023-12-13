from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Loads dimension table in Redshift from data in staging table
    
    redshift_conn_id: Redshift connection ID
    table: Target table in Redshift to load
    sql: SQL query for getting data to load into target table
        append_insert: Whether the append-insert or truncate-insert method
        of loading should be used
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):

        self.log.info("Getting redshift credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Insert data from staging table into {self.table} dimension table")
        table_insert_sql = f"""
                INSERT INTO {self.table}
                {self.sql}
                """
        redshift_hook.run(table_insert_sql)
