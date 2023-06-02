-- 년도별 월 사업 순위
SELECT
      DATE_PART('month', "projStartDd"::date)
    , count(1)
FROM raw_data.projects
WHERE left("projStartDd", 4) not in ('2021' , '2019')
AND "delYn" = 'N'
GROUP BY 1
ORDER BY 1
;

-- 년도별 월 사업 백분위 평균 순위
SELECT EXTRACT(YEAR FROM "projStartDd"::date) as year
    , EXTRACT(MONTH FROM "projStartDd"::date) as month
    , SUM(COUNT(1)) OVER (PARTITION BY LEFT("projStartDd", 4)) AS year_cnt
    , SUM(COUNT(1)) OVER (PARTITION BY LEFT("projStartDd", 7)) AS month_cnt
    , ROUND(SUM(COUNT(1)) OVER (PARTITION BY LEFT("projStartDd", 7)) * 100.0 /
          SUM(COUNT(1)) OVER (PARTITION BY LEFT("projStartDd", 4)), 2) AS percent
FROM
    raw_data.projects
WHERE
    LEFT("projStartDd", 4) NOT IN ('2021', '2019')
GROUP BY year, month, "projStartDd"
ORDER BY year, month
;


-- 지역별 사업 유형 분포------------------------------------------------------------------
-- ISO 3166-2:KR은 지리 식별 부호 추가
SELECT
    "admProvNm"
    , CASE "admProvNm"
        WHEN '서울특별시' THEN 'KR-11'
        WHEN '부산광역시' THEN 'KR-26'
        WHEN '대구광역시' THEN 'KR-27'
        WHEN '인천광역시' THEN 'KR-28'
        WHEN '광주광역시' THEN 'KR-29'
        WHEN '대전광역시' THEN 'KR-30'
        WHEN '울산광역시' THEN 'KR-31'
        WHEN '세종특별자치시' THEN 'KR-50'
        WHEN '경기도' THEN 'KR-41'
        WHEN '강원도' THEN 'KR-42'
        WHEN '충청북도' THEN 'KR-43'
        WHEN '충청남도' THEN 'KR-44'
        WHEN '전라북도' THEN 'KR-45'
        WHEN '전라남도' THEN 'KR-46'
        WHEN '경상북도' THEN 'KR-47'
        WHEN '경상남도' THEN 'KR-48'
        WHEN '제주특별자치도' THEN 'KR-49'
        end
    , count(1)
FROM raw_data.projects_temp
WHERE LEFT("projStartDd", 4) NOT IN ('2021', '2019')
AND "admProvNm" is not null
GROUP BY "admProvNm"
;


-- 지역별 사업 유형 분포 TOP 10
SELECT
    "admProvNm" || ' ' || btrim(left("admDistNm",4)) b
    , count(1) cnt
FROM raw_data.projects_temp
WHERE LEFT("projStartDd", 4) NOT IN ('2021', '2019')
AND "admProvNm" is not null
GROUP BY 1
ORDER BY cnt desc
;

----------------------------------------------------------------------------------------

-- 사업유형 분포
SELECT "projType"
    ,   "projTypeNm"
    ,   count(1)
FROM raw_data.projects
WHERE
    LEFT("projStartDd", 4) NOT IN ('2021', '2019')
    and "projTypeNm" != '기타'
GROUP BY 1,2
ORDER BY 3 desc
;


-----------------------------------------------------------------------------------------
-- 지역별 공고 분포
SELECT
    LEFT("workPlcNm", 2)
    , CASE LEFT("workPlcNm", 2)
        WHEN '서울' THEN 'KR-11'
        WHEN '부산' THEN 'KR-26'
        WHEN '대구' THEN 'KR-27'
        WHEN '인천' THEN 'KR-28'
        WHEN '광주' THEN 'KR-29'
        WHEN '대전' THEN 'KR-30'
        WHEN '울산' THEN 'KR-31'
        WHEN '세종' THEN 'KR-50'
        WHEN '경기' THEN 'KR-41'
        WHEN '강원' THEN 'KR-42'
        WHEN '충북' THEN 'KR-43'
        WHEN '충남' THEN 'KR-44'
        WHEN '전북' THEN 'KR-45'
        WHEN '전남' THEN 'KR-46'
        WHEN '경북' THEN 'KR-47'
        WHEN '경남' THEN 'KR-48'
        WHEN '제주' THEN 'KR-49'
        end
    , count(1)
FROM raw_data.jobs
WHERE "workPlcNm" is not null
GROUP BY 1
;

-- 지역별 공고 분포 (연도별 공고 변이)
SELECT
    LEFT("workPlcNm", 7), count(1)
FROM raw_data.jobs
WHERE "workPlcNm" is not null
GROUP BY 1
ORDER BY 2 DESC
;

-----------------------------------------------------------------------------------------

-- 년도별 월 사업 백분위 평균 순위 Yearly Average Jobs Dataset
-- 연도별 목표 일자리 수
(
    SELECT
        EXTRACT(YEAR FROM "startDd"::date) AS year, EXTRACT(MONTH FROM "startDd"::date) AS month,
        SUM(COUNT(1)) OVER (PARTITION BY LEFT("startDd", 4)) AS year_cnt,
        SUM(COUNT(1)) OVER (PARTITION BY LEFT("startDd", 7)) AS month_cnt,
        ROUND(SUM(COUNT(1)) OVER (PARTITION BY LEFT("startDd", 7)) * 100.0 /
              SUM(COUNT(1)) OVER (PARTITION BY LEFT("startDd", 4)), 2) AS percent, 'bar' AS chart_type
    FROM
        raw_data.jobs
    WHERE
        LEFT("startDd", 4) NOT IN ('2021', '2019')
    GROUP BY year, month, "startDd"
    ORDER BY year, month
)
union all
(
    SELECT
        EXTRACT(YEAR FROM "projStartDd"::date) AS year, EXTRACT(MONTH FROM "projStartDd"::date) AS month,
        SUM(COUNT("targetEmployment")) OVER (PARTITION BY LEFT("projStartDd", 4)) AS year_cnt,
        SUM(COUNT("targetEmployment")) OVER (PARTITION BY LEFT("projStartDd", 7)) AS month_cnt,
        ROUND(SUM(COUNT("targetEmployment")) OVER (PARTITION BY LEFT("projStartDd", 7)) * 100.0 /
              SUM(COUNT("targetEmployment")) OVER (PARTITION BY LEFT("projStartDd", 4)), 2) AS percent,
        'line' AS chart_type
    FROM
        raw_data.projects
    WHERE
        LEFT("projStartDd", 4) NOT IN ('2021', '2019')
    GROUP BY year, month, "projStartDd"
    ORDER BY year, month
)
;


SELECT "admProvNm" || ' ' || BTRIM(LEFT("admDistNm", 4)) AS "My column"
    , COUNT(*) AS cnt
    , round(avg(count(1)) OVER (ORDER BY 1))
FROM
    raw_data.projects_temp
WHERE
    (("admProvNm" IS NOT NULL) AND (LEFT("projStartDd", 4) NOT IN ('2021', '2019')))
GROUP BY "admProvNm" || ' ' || BTRIM(LEFT("admDistNm", 4))
ORDER BY cnt DESC;