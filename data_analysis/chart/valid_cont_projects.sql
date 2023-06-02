-- 사업 테이블에서 유효한 사업 데이터만 추출

/*
1. 사업 시작일이 null 이거나, 2018,20,22년도가 아닌 것은 제외
2. (사업기간종료알-사업기간시작일) 이 0 이하인 것 제외
2. '사업시작년도'에 대한 필드 생성 (projStartYear)
3. '지속년수' 에 대한 필드 생성 (durationYears)
*/

CREATE TABLE data_collection.analytics.valid_cont_projects AS
SELECT 
    *, 
    CAST(EXTRACT(YEAR FROM TO_DATE(projStartDd, 'YYYY-MM-DD')) AS VARCHAR) AS projStartYear,
    EXTRACT(YEAR FROM TO_DATE(LEFT(projStartDd, 4), 'YYYY')) - EXTRACT(YEAR FROM TO_DATE(LEFT(contProjStartYear, 4), 'YYYY')) AS durationYears
FROM data_collection.raw_data.projects
WHERE 
    contProjStartYear IS NOT NULL 
    AND projStartYear IN (2018,2020,2022)
    AND TO_DATE(projEndDd, 'YYYY-MM-DD') - TO_DATE(projStartDd, 'YYYY-MM-DD') >= 0;
