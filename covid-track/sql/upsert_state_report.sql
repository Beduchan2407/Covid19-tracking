begin;

insert into report.state_daily_new
select temp_table.* from
    (   select state, date, sum(new_cases) as total_new_cases, sum(new_deaths) as total_new_deaths
        from dw.covid_daily_fact
        join dw.date_dim
        on dw.covid_daily_fact.date_key = dw.date_dim.date_key
        join dw.location_dim
        on dw.covid_daily_fact.location_key = dw.location_dim.location_key
        group by state, date
    ) as temp_table
left join report.state_daily_new
on temp_table.state = report.state_daily_new.state and temp_table.date = report.state_daily_new.date
where new_cases is null;

end;
