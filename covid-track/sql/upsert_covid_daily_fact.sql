begin;

delete from staging.covid_daily_fact;

copy staging.covid_daily_fact
from 's3://covid19-us-tracking/data_process_output/covid_daily_fact'
CSV
IGNOREHEADER 1
credentials 'aws_iam_role=arn:aws:iam::424597550561:role/my-redshift-role'
delimiter ',' region 'ap-southeast-1';

end;
