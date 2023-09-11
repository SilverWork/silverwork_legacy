# 공공데이터 API를 이용한 노인 일자리 분석
> 프로젝트 기간
> - (1차) 2023.05.29 ~ 2023.06.02  
> - (2차) 2023.06.26 ~ 2023.06.30  -  [2차 프로젝트 링크](https://github.com/SilverWork)

## 📑 프로젝트 소개 
### 1) 개요
 노인일자리 사업은 65세 이상 노인들에게 경제적 자립과 사회 참여를 목적으로 정부에서 시행하고 있는 복지사업입니다.   
최근 3개년 동안의 노인일자리 공고와 사업 정보를 수집하여 노인일자리 채용 현황과 고용 지속성을 조사합니다. 
<br> 

### 2) 프로젝트 주제 선정 이유
노인일자리 사업은 경제적 자립과 사회 참여를 촉진하는데 중요한 역할을 합니다.   
그러나 사업의 실제 현황과 채용 동향에 대한 정확한 정보 부족으로 인해 정책의 효과성을 평가하기 어렵습니다.    

이 프로젝트는 노인일자리의 공고와 사업 정보를 분석하여 노인일자리 구인 동향을 파악하고,   
정부의 노인 복지 정책의 효과성을 평가할 수 있습니다.
<br> 

### 3) 기대효과

> - 노인일자리에 대한 상세한 이해를 도모할 수 있습니다. 
> - 노인일자리 정책의 성과와 한계를 파악하고 개선할 수 있는 방안을 모색할 수 있습니다.
<br>

### 4) 분석 결과 보고서 및 프로젝트 PPT
[공공 데이터 API를 활용한 노인 일자리 분석 PPT.pdf](https://github.com/DW-BI-Project/silverwork/files/11725350/API.PPT.pdf)  

[공공 데이터 API를 활용한 노인 일자리 분석 보고서.pdf](https://github.com/DW-BI-Project/silverwork/files/11725351/API.pdf)

<br> 

## 👨‍👨‍👦 팀원 
| 이름 | 역할 | Github| 
|--|---|--|
| 김상희 | DB 모델링, 데이터 ETL | [@ksh1357](https://github.com/ksh1357) | 
| 김혜민 | DB 모델링, 데이터 분석 및 시각화 |  [@HyeM207](https://github.com/HyeM207) |
| 남윤아 | DB 모델링, 데이터 분석 및 시각화 |  [@namuna309](https://github.com/namuna309) |
| 이성희 | DB 모델링, 데이터 분석 및 시각화 |  [@LSH](https://github.com/gracia10) |
| 이하윤 | DB 모델링, 데이터 ETL |  [@ha6oon](https://github.com/ha6oon) |

<br>

## #️⃣ 활용 기술 및 프레임워크
### 1) 개발 환경
| Field | Stack |
|:---:|:---|
| 사용 언어 | <img src="https://img.shields.io/badge/python-3776AB?style=flat&logo=python&logoColor=white"/> |
| 데이터 적재 | <img src="https://img.shields.io/badge/amazons3-569A31?style=flat&logo=amazons3&logoColor=white"/> <img src="https://img.shields.io/badge/snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white"/> |
| Dashboard | Superset |
| 협업 도구 | <img src="https://img.shields.io/badge/github-181717?style=flat&logo=github&logoColor=white"/> <img src="https://img.shields.io/badge/slack-4A154B?style=flat&logo=slack&logoColor=white"/> <img src="https://img.shields.io/badge/googlesheets-34A853?style=flat&logo=googlesheets&logoColor=white"/>|

### 2) 프로젝트 아키텍처 
![project Architecture](https://github.com/DW-BI-Project/silverwork/assets/63229014/44d60cc5-b585-4f5d-99b6-4dfd3329e8e0)
<br> 

### 3) ERD
![erd](https://github.com/DW-BI-Project/silverwork/assets/63229014/64eed44e-82a6-4864-a792-ca2da5fc8e8e)

<br> 

## 📶 대시보드 
![image](https://github.com/DW-BI-Project/silverwork/assets/43624842/e97d513e-5bf9-4cda-94b6-06e2ae4c9bc9)

<br>

## 📌 Repository 구조

### data_analysis
- 데이터 분석 모듈
- 상수 및 분석, 테스트 스크립트

### data_collection
- 데이터 수집 모듈
- 상수 및 수집, 테스트 스크립트

### resouce
- 모듈이 공용으로 사용하는 자원정보
- CSV, XML, HTML 등이 저장된다


## ❗ 실행

1. 프로젝트 checkout

2. 루트 경로(silverwork/) 에서 shell 실행후 가상환경 생성
```
python -m venv venv
source venv/bin/activate
```
3. 패키지 다운로드
```
pip install -r requirements.txt
```
