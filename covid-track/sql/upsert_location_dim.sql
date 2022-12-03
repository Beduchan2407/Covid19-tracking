begin;

copy staging.location_dim
from 's3://covid19-us-tracking/data_process_output/location_dim.csv'
CSV
IGNOREHEADER 1
credentials 'aws_iam_role=arn:aws:iam::424597550561:role/my-redshift-role'
delimiter ',' region 'ap-southeast-1';

-- Upsert covid_daily_fact table
update dw.location_dim as DW
set location_key = ST.location_key
from staging.location_dim as ST
where DW.state = ST.state and DW.county = ST.county and DW.total_population = ST.total_population;

insert into dw.location_dim
select ST.*
from staging.location_dim as ST
left join dw.location_dim as DW
on ST.location_key = DW.location_key
where DW.state is null;

delete from staging.location_dim;

end;
