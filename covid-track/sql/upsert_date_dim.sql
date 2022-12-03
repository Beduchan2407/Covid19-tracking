begin;

copy staging.date_dim
from 's3://covid19-us-tracking/data_process_output/date_dim.csv'
CSV
IGNOREHEADER 1
credentials 'aws_iam_role=arn:aws:iam::424597550561:role/my-redshift-role'
delimiter ',' region 'ap-southeast-1';

insert into dw.date_dim
select ST.*
from staging.date_dim as ST
left join dw.date_dim as DW
on ST.date_key = DW.date_key
where DW.date is null;

delete from staging.date_dim;

end;
