import pandas as pd
import numpy as np 
# csv 파일 읽어오는 부분
def csv_to_dataframe(file_path):
    try:
        # dataframe으로 변경
        # utf-8-sig로 인코딩하는 경우 에러가 발생해서 변경
        return pd.read_csv(file_path, encoding='utf-8-sig')
    except Exception as e:
        print(repr(e))
        return pd.DataFrame()

#코드정보 csv 파일 로드
epl = csv_to_dataframe("epl.csv")
#emplymShp,emplymShpNm 컬럼만 추출
epl = epl[['emplymShp','emplymShpNm']]
# 출력해보기
print(epl)

# job csv 파일 로드
# emplymShp,emplymShpNm | emplymShpNm
jobs = csv_to_dataframe("jobs2020_final_check.csv")
# 출력해보기
print(jobs)

# 테이블 조인
test = pd.merge(jobs, epl, on='emplymShp', how='left')
print(test.columns)

# emplymShpNm_x 컬럼 drop
test.drop(['emplymShpNm_x'], axis=1, inplace=True)
#'Unnamed: 0' 컬럼 drop
test.drop(['Unnamed: 0'], axis=1, inplace=True)
# emplymShpNm_y 컬럼 rename
test.rename(columns={'emplymShpNm_y':'emplymShpNm'}, inplace=True)
# Key-> pk
test.rename(columns={'Key' : 'pk'}, inplace=True)
# 기획한 모델린 항목 순서로 정렬
columns = ['acptMthd','deadline','emplymShp','emplymShpNm','startDd','pk','jobcls','jobclsNm','oranNm','organYn','recrtTitle','stmId','stmNm','endDd','workPlc','workPlcNm','acptMthdCd','age','ageYn','clerk','clerkContt','clltPrnnum','createDy','detCnts','etcItm','homepage','plDetAddr','plbizNm','updDt']
test = test[columns]
#합친 데이터 csv로 저장하기
test.to_csv("new_jobs2020_final_check.csv", encoding='utf-8-sig')
print(test)
value_counts = test['pk'].value_counts()
print(value_counts)
#epl_2023 뽑기
'''
case_1 = 0
case_2 = 0
case_3 = 0
jobs_2023 = csv_to_dataframe("jobs_2023.csv")
for i in range(len(jobs_2023)):
    workPlcNm = jobs_2023.loc[i, 'workPlcNm']
    if workPlcNm is None:
        case_1 += 1
        continue
    elif isinstance(workPlcNm, str) and len(workPlcNm) > 0:
        case_2 += 1
        continue
    elif np.isnan(workPlcNm):
        case_3 += 1
        continue
    else:
        print(type(jobs_2023.loc[i, 'workPlcNm']), jobs_2023.loc[i, 'workPlcNm'])
print('None인 경우: {}'.format(case_1))
print('값이 있는 경우: {}'.format(case_2))
print('nan인 경우: {}'.format(case_3))
# # 특정 컬럼만 추출
# jobs_2023 = jobs_2023[['workPlc','workPlcNm']]
# # 결측치 제거
# jobs_2023.dropna(inplace=True)
# # 중복 제거
# jobs_2023 = jobs_2023.drop_duplicates()
# jobs_2023.to_csv("epl_2023.csv", encoding='utf-8-sig')
'''
# 방법1
# jobs_2023 = jobs_2023[['workPlc','workPlcNm']]
# jobs_2023.dropna(inplace=True)
# jobs_2023 = jobs_2023.drop_duplicates()
# print('중복제거 결과: {}'.format(len(jobs_2023)))
# jobs_2023.to_csv("epl_2023.csv", encoding='utf-8-sig')
'''
# 방법2
jobs_2023 = csv_to_dataframe("jobs_2023.csv")
mask = ~jobs_2023['workPlcNm'].isna()
test_2023 = jobs_2023.loc[mask, ['workPlc','workPlcNm']].drop_duplicates(subset=['workPlc','workPlcNm'])
test_2023.to_csv("epl_2023.csv", encoding='utf-8-sig')


# jobs -> epl
jobs_2022 = csv_to_dataframe("jobs2020_final_check.csv")
mask = ~jobs_2022['emplymShpNm'].isna()
test_2022 = jobs_2022[mask, ['emplymShp','emplymShpNm']].drop_duplicates(subset=['emplymShp','emplymShpNm'])
test_2022.to_csv("epl_2022.csv", encoding='utf-8-sig')
'''
'''
# workPlc, workPlcNm
df1 = pd.read_csv("new_jobs2018_final_check.csv", encoding='utf-8-sig')
df2 = pd.read_csv("epl_2023.csv", encoding='utf-8-sig')
value_counts = df1['pk'].value_counts()
print(value_counts)
value_counts = df2['workPlc'].value_counts()
print(value_counts)
#테이블 조인 pk, workPlc
test = pd.merge(df1, df2, on='workPlc', how='left') #체크
print(test.columns)
value_counts = test['pk'].value_counts()
print(value_counts)
# workPlcNm 컬럼 drop
test.drop(['workPlcNm_x'], axis=1, inplace=True)
#'Unnamed: 0' 컬럼 drop
test.drop(['Unnamed: 0_y'], axis=1, inplace=True)
test.drop(['Unnamed: 0_x'], axis=1, inplace=True)
# # emplymShpNm_y 컬럼 rename
test.rename(columns={'workPlcNm_y':'workPlcNm'}, inplace=True)
print(test)
print(test.columns)

columns = ['acptMthd','deadline','emplymShp','emplymShpNm','startDd','pk','jobcls','jobclsNm','oranNm','organYn','recrtTitle','stmId','stmNm','endDd','workPlc','workPlcNm','acptMthdCd','age','ageYn','clerk','clerkContt','clltPrnnum','createDy','detCnts','etcItm','homepage','plDetAddr','plbizNm','updDt']
test  = test[columns]
#합친 데이터 csv로 저장하
#test.to_csv("2018_jobs.csv", encoding='utf-8-sig')
#print(test)
'''


