from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    '''
    Operator loads all JSON formatted files from S3 bucket to Amazon Redshift Cluster.
   
    redshift_conn_id:  Airflow conn_id of Redshift connection
    aws_credentials_id: Airflow conn_id of the aws_credentials granting access to s3 bucket(Default: 'aws_credentials')  
    table: The name of the Amazon Redshift table where the data should be loaded
    s3_path: The data source where to get data from          
    
    '''
    ui_color = '#358140'

    template_fields = ("s3_key",)
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id="",
                 table = "",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 json_path = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table           
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_path = json_path

    def execute(self, context):
        self.log.info("Getting AWS credentials")
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        
        self.log.info("Getting redshift credentials")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table, 
            s3_path, 
            aws_connection.login,
            aws_connection.password,
            self.region, 
            self.json_path
        )
        redshift.run(formatted_sql)
        self.log.info(f"Data sucessfully copied from S3 to {self.table} Redshift table")





