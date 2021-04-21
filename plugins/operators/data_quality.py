from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 data_quality_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
        self.data_quality_checks = data_quality_checks

    def execute(self, context):
        redshift = PostgresHook('redshift')
        
        for check in self.data_quality_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            records = redshift.get_records(sql)
            num_records = records[0][0]
            if exp_result != num_records:
                raise ValueError(f'Data quality check failed. Expected {exp_result}, but found {records}')
            else:
                self.log.info(f'Data Quality check of {sql} passed with {records} records')
