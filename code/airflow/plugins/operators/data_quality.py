from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 include_date=False,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks
        self.include_date = include_date

    def execute(self, context):
        exec_dt = context['ds']
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        error_count = 0
        failing_tests = []
        self.log.info('Data quality checks started')
        for check in self.dq_checks:
            sql = check.get('check_sql')
            if self.include_date: 
                sql = sql + "'" + exec_dt + "'"
            exp_result = check.get('expected_result')
            comparison = check.get('comparison')
            records = redshift.get_records(sql)[0]
            
            if ((comparison == '=' and records[0] != exp_result) or (comparison == '>' and records[0] <= exp_result) \
            or (comparison == '>=' and records[0] < exp_result) or (comparison == '<' and records[0] >= exp_result) \
            or (comparison == '<=' and records[0] > exp_result) or (comparison == '<>'and records[0] == exp_result)):                
                error_count += 1
                failing_tests.append(sql)    
                       
        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')
        self.log.info('Data quality checks passed')