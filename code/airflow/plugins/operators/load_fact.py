from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    fact_sql_template = """
        INSERT INTO public.{table}
        {sql_statement};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_statement="",                 
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        fact_sql = LoadFactOperator.fact_sql_template.format(
            table=self.table,
            sql_statement=self.sql_statement
        )
        self.log.info("Inserting data into Redshift fact table " + self.table + " started")
        redshift.run(fact_sql)
        self.log.info("Inserting data into Redshift fact table " + self.table + " completed")
