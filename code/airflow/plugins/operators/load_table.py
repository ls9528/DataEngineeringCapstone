from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadTableOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    truncate_sql_template = """
        TRUNCATE public.{};
    """
    
    dim_sql_template = """
        INSERT INTO public.{table}{columns}
        {sql_statement};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 columns=""
                 sql_statement="",
                 truncate_data=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.columns=columns
        self.sql_statement = sql_statement
        self.truncate_data = truncate_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_data:
            truncate_sql = LoadTableOperator.truncate_sql_template.format(self.table)
            self.log.info("Truncating data from Redshift table " + self.table + " started")
            redshift.run(truncate_sql)
            self.log.info("Truncating data from Redshift table " + self.table + " completed") 
        else:
            self.log.info("Note: appending data to Redshift table " + self.table)     
        dim_sql = LoadTableOperator.dim_sql_template.format(
            table=self.table,
            sql_statement=self.sql_statement
        )
        self.log.info("Inserting data into Redshift table " + self.table + " started")
        redshift.run(dim_sql)
        self.log.info("Inserting data into Redshift table " + self.table + " completed")
