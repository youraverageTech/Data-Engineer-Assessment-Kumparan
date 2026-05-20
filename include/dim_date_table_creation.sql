-- Membuat table dim_date di schema dwh
CREATE OR REPLACE TABLE dwh.dim_date (
    published_date_at NUMBER,
    date DATE,
    day INT,
    month INT,
    year INT,
    quarter INT
);

-- Mengisi data ke dalam table dim_date
INSERT INTO dwh.dim_date (published_date_at, date, day, month, year, quarter)
WITH RECURSIVE date_range as (
    select '2016-01-01'::date as date
    union all
    select dateadd(DAY, 1, date)
    from date_range
    where date < '2030-12-31'
)
SELECT TO_NUMBER(TO_VARCHAR(date, 'YYYYMMDD')) as published_date_at
    , date
    , DAY(date) as day
    , MONTH(date) as month
    , YEAR(date) as year
    , QUARTER(date) as quarter
FROM date_range;