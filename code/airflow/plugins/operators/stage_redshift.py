from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql_parquet = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS PARQUET
    """
    copy_sql_csv = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 truncate_data=True,
                 data_format="CSV",
                 ignore_headers=1,
                 delimiter=",",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.truncate_data = truncate_data
        self.data_format=data_format
        self.ignore_headers = ignore_headers
        self.delimiter = delimiter

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_data: 
            self.log.info("Clearing data from destination Redshift table " + self.table)
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 bucket " + self.s3_bucket + " to Redshift table " + self.table + " started")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        if self.data_format == "PARQUET":
            formatted_sql = StageToRedshiftOperator.copy_sql_parquet.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key
            )
        elif self.data_format == "CSV":
            formatted_sql = StageToRedshiftOperator.copy_sql_csv.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
            )
        
        if formatted_sql:
            redshift.run(formatted_sql)
            self.log.info("Copying data from S3 bucket " + self.s3_bucket + " to Redshift table " + self.table + " complete")
        else:
            self.log.info("Not able to copy data from S3 bucket " + self.s3_bucket + " to Redshift table " + self.table + ". Invalid data format.")    





