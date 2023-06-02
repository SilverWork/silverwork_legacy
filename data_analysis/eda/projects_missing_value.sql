-- 사업 유형 Null값이 4711건 조회
SELECT
    count(1)
FROM raw_data.projects
WHERE LEFT(projStartDd, 4) NOT IN ('2021', '2019')
AND admDistCd is null
;

-- 1700건 데이터 결측치 해결 (2926)
create table raw_data.projects_temp (like raw_data.projects INCLUDING DEFAULTS);
insert into raw_data.projects_temp SELECT * from raw_data.projects;
update raw_data.projects_temp a
SET "admProvNm" = b.관활시도명,
    "admDistCd" = b.시군구코드,
    "institutionId" = b."기관ID"
from raw_data.projects23 b
WHERE a."projNo" = b.사업번호
;

SELECT
    count(1)
FROM raw_data.projects_temp
WHERE LEFT("projStartDd", 4) NOT IN ('2021', '2019')
AND "admDistCd" is null
;


-- 기관ID를 기준으로 누락된 값 조회
WITH sub as (SELECT * FROM raw_data.projects_temp t WHERE "admProvNm" is not null)
SELECT
    a."institutionId" , sub."institutionId",
    a."admDistCd" , sub."admDistCd",
    a."admProvNm", sub."admProvNm",
    a."admDistNm", sub."admDistNm"
FROM raw_data.projects_temp a
JOIN sub on a."institutionId" = sub."institutionId"
WHERE LEFT(a."projStartDd", 4) NOT IN ('2021', '2019')
and a."admProvNm" is null
;

-- 기관ID를 기준으로 누락된 값 보충
update raw_data.projects_temp a
SET "admProvNm" = b."admProvNm",
    "admDistCd" = b."admDistCd"
from (SELECT * FROM raw_data.projects_temp t WHERE "admProvNm" is not null) b
WHERE a."institutionId" = b."institutionId"
AND a."admProvNm" is null
;


-- 최종 결과 (122)
SELECT
    "admDistNm" , count(1)
FROM raw_data.projects_temp
WHERE LEFT("projStartDd", 4) NOT IN ('2021', '2019')
AND "admProvNm" is null
GROUP BY "admDistNm"
;