CREATE TABLE analytics.emplymshp_cnt as
SELECT
    to_char(jobs.startdd, 'YYYY') as year,
    jobs.emplymshp as emplymshp
FROM raw_data.jobs as jobs;

select * from analytics.emplymshp_cnt limit 100000