begin;

create schema if not exists staging;

create schema if not exists dw;

create schema if not exists report;

create table if not exists staging.location_dim(
    location_key int primary key not null,
    state varchar not null,
    county varchar not null,
    fips_code int not null,
    total_population float not null
);

create table if not exists staging.date_dim(
    date_key int primary key not null,
    date date not null,
    year int not null ,
    month int not null ,
    day int not null

);

create table if not exists staging.covid_daily_fact(
    location_key int not null ,
    date_key int not null ,
    cumulative_cases float not null ,
    cumulative_deaths float not null,
    new_cases float not null ,
    new_deaths float not null,
    primary key (location_key, date_key)
);

create table if not exists dw.location_dim(
    location_key int primary key not null,
    state varchar not null,
    county varchar not null,
    fips_code int not null,
    total_population float not null
);

create table if not exists dw.date_dim(
    date_key int primary key not null,
    date date not null,
    year int not null ,
    month int not null ,
    day int not null

);

create table if not exists dw.covid_daily_fact(
    location_key int not null ,
    date_key int not null ,
    cumulative_cases float not null ,
    cumulative_deaths float not null,
    new_cases float not null ,
    new_deaths float not null,
    primary key (location_key, date_key)
);

create table if not exists report.state_daily_new(
    state varchar not null,
    date date not null,
    new_cases float not null,
    new_deaths float not null
);

end;
