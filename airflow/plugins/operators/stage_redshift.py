from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    to_redshift_template = """
        COPY {table_name} FROM '{from_file}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        FORMAT AS JSON '{format_type}'
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id = "",
                 table_name = "",
                 from_file = "",
                 aws_credent = "",
                 format_type = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.from_file = from_file
        self.aws_credent = aws_credent
        self.format_type = format_type

    def execute(self, context):
        
        aws_web_hook = AwsHook(self.aws_credent)
        aws_credentials = aws_web_hook.get_credentials()
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("# Deleting existing files from Redshift Table")
        #redshift.run(f'DELETE FROM {self.table_name}')
        
        self.log.info("# COPY S3 data to staging tables")       
        
        to_redshift_query = StageToRedshiftOperator.to_redshift_template.format(
            table_name= self.table_name,
            from_file= self.from_file,
            access_key = aws_credentials.access_key,
            secret_key = aws_credentials.secret_key,
            format_type = self.format_type
        )
        
        redshift.run(to_redshift_query)
        #self.log.info('StageToRedshiftOperator not implemented yet')





