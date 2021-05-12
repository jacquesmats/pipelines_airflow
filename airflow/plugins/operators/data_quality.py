from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    redshift_tables_sql = """
        SELECT DISTINCT tablename
        FROM pg_table_def
        WHERE schemaname = 'public'
        ORDER BY tablename;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 schema = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.schema = schema

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        tables = redshift.get_records(DataQualityOperator.redshift_tables_sql)
        self.log.info(tables)
        for table in tables:
            
            table = table[0] #treat get_records output

            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")