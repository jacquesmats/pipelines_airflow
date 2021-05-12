from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
   
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 query="",
                 load_type="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.destination_table = destination_table
        self.load_type = load_type

    def execute(self, context):
        """ Insert or append data to the destination table in Redshift"""
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.load_type == 'insert':
            #self.log.info(f"# Cleaning dim table {self.destination_table}")
            #dim_rem_sql = f'DROP TABLE IF EXISTS {self.destination_table}'
            #redshift.run(dim_rem_sql)
            
            self.log.info(f"# Inserting dim table {self.destination_table} to Redshift")
            dim_ins_sql = f'INSERT INTO {self.destination_table}' + self.query
            redshift.run(dim_ins_sql)
        
        elif self.load_type == 'append':
            self.log.info(f"# Appending dim table {self.destination_table} to Redshift")
            dim_app_sql = f'UPSERT INTO {self.destination_table}' + self.query
            redshift.run(dim_app_sql)
        
        else:
            self.log.info(f"# [ERR] Somenting went wrong while loading {self.destination_table} to Redshift")

