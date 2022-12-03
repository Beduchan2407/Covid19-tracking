begin;

copy staging.covid_daily_fact
from 's3://covid19-us-tracking/data_process_output/covid_daily_fact'
CSV
IGNOREHEADER 1
credentials 'aws_iam_role=arn:aws:iam::424597550561:role/my-redshift-role'
delimiter ',' region 'ap-southeast-1';

-- Upsert covid_daily_fact table
update dw.covid_daily_fact as DW
set location_key = ST.location_key, date_key = ST.date_key
from staging.covid_daily_fact as ST
where DW.cumulative_cases = ST.cumulative_cases and DW.cumulative_deaths = ST.cumulative_deaths and DW.new_cases = ST.new_cases and DW.new_deaths = ST.new_deaths;

insert into dw.covid_daily_fact
select ST.*
from staging.covid_daily_fact as ST
left join dw.covid_daily_fact as DW
on ST.location_key = DW.location_key and ST.date_key = DW.date_key
where DW.cumulative_cases is null;

delete from staging.covid_daily_fact;

end;
