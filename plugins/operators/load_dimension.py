from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,

                 redshift_conn_id="",
                 sql_query = "",
                 table = "",
                 clear_load = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.clear_load = clear_load

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_load:
            self.log.info(f"Clear load statement set to TRUE. Deleting existing data from  table {self.table}")
            redshift.run(f"DELETE FROM {self.table}")

        self.log.info(f"Running query to load data into Dimension Table {self.table_name}") 
        redshift.run(self.sql_query)
        self.log.info(f"{self.table_name} has been successfuly loaded.")
