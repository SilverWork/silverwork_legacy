'''
2018, 2020, 2022년도의 노인 공공 데이터와 그에 해당하는 상세공고 데이터가 모델링에 맞게 아래의 전처리 과정을 거쳐서 3개의 csv 파일로 저장하는 과정.
'''

import pandas as pd
import numpy as np

# 모델링에 맞춰 공고 전처리
data = pd.read_csv("search_2018.csv", encoding='utf-8-sig') # encoding='utf-8-sig', encoding='cp949'
#print(data.head())

#data.drop(['Unnamed: 0.2'], axis=1, inplace=True)
data.drop(['Unnamed: 0.1'], axis=1, inplace=True)
data.drop(['Unnamed: 0'], axis=1, inplace=True)
data.rename(columns={'frDd':'StartDd'}, inplace=True)
data.rename(columns={'toDd':'EndDd'}, inplace=True)
data.rename(columns={'jobId':'Key'}, inplace=True)
data.rename(columns={'StartDd':'startDd'}, inplace=True)
data.rename(columns={'EndDd':'endDd'}, inplace=True)
#print(data.head())

#합친 데이터 csv로 저장하기
data.to_csv("jobs2018_dropped.csv", encoding='utf-8-sig') 

# 모델링에 맞춰 상세공고 전처리
data = pd.read_csv("jobinfo_2020.csv", encoding='utf-8-sig') # encoding='utf-8-sig', encoding='cp949'
#print(data.head())

data.drop(['Unnamed: 0'], axis=1, inplace=True)
data.drop(['frAcptDd'], axis=1, inplace=True)
data.drop(['lnkStmId'], axis=1, inplace=True)
data.drop(['organYn'], axis=1, inplace=True)
data.drop(['repr'], axis=1, inplace=True)
data.drop(['stmId'], axis=1, inplace=True)
data.drop(['toAcptDd'], axis=1, inplace=True)
data.drop(['wantedAuthNo'], axis=1, inplace=True)
data.drop(['wantedTitle'], axis=1, inplace=True)

data.rename(columns={'ageLim':'ageYn'}, inplace=True)
data.rename(columns={'updDy':'updDt'}, inplace=True)
data.rename(columns={'jobId':'Key'}, inplace=True)
#print(data.head())

#합친 데이터 csv로 저장하기
data.to_csv("jobs2020_details_dropped.csv", encoding='utf-8-sig') 

# 공고랑 공고상세 조인하기
df1 = pd.read_csv("jobs2020_dropped.csv", encoding='utf-8-sig') 
#print(df1.head())
df2 = pd.read_csv("jobs2020_details_dropped.csv", encoding='utf-8-sig') 
#print(df2.head())
merged_df = pd.merge(left = df1 , right = df2, how = "inner", on = "Key")
merged_df.drop(['Unnamed: 0_x'], axis=1, inplace=True)
merged_df.drop(['Unnamed: 0_y'], axis=1, inplace=True)
#print(merged_df)

# pk 이슈 체크
#value_counts = merged_df['Key'].value_counts()
#print(value_counts) 

#합친 데이터 csv로 저장하기
merged_df.to_csv("jobs2020_merged.csv", encoding='utf-8-sig')  
df4 = pd.read_csv("jobs2020_merged.csv", encoding='utf-8-sig') # encoding='utf-8-sig', encoding='cp949'
#print(len(df4))

# ETL
df = pd.read_csv("jobs2020_merged.csv", encoding='utf-8-sig')
#print(df)

def date_format(x):  # YYYYMMDD -> YYYY-MM-DD : 날짜 포맷 변경 
    x = str(x)
    if len(x) == 8:
        return f'{x[:4]}-{x[4:6]}-{x[6:]}'
    else:
        return x
df['startDd'] = df['startDd'].apply(lambda x: date_format(x))
df['endDd'] = df['endDd'].apply(lambda x: date_format(x))


def has_age_value(x):  # age에 값이 있다면 ageYd 에 Y/N 넣어주기
    x = str(x)
    result = 'N'
    if x is not None:
        result = 'Y'
    return result
df['ageYn'] = df['age'].apply(lambda x: has_age_value(x))
df.drop(['Unnamed: 0'], axis=1, inplace=True)
print(df)

#csv로 저장하기
df.to_csv("jobs2020_final_check.csv", encoding='utf-8-sig') 

# emplymShp으로 emplymShpNm 채우기
def csv_to_dataframe(file_path): # csv 파일 읽어오는 부분
    try:
        return pd.read_csv(file_path, encoding='utf-8-sig')
    except Exception as e:
        print(repr(e))
        return pd.DataFrame()

#코드정보 csv 파일 로드
epl = csv_to_dataframe("epl.csv")

epl = epl[['emplymShp','emplymShpNm']] #emplymShp,emplymShpNm 컬럼만 추출
#print(epl)

# job csv 파일 로드
jobs = csv_to_dataframe("jobs2020_final_check.csv")
#print(jobs)

# 테이블 조인
test = pd.merge(jobs, epl, on='emplymShp', how='left')
#print(test.columns)

test.drop(['emplymShpNm_x'], axis=1, inplace=True) 
test.drop(['Unnamed: 0'], axis=1, inplace=True)
test.rename(columns={'emplymShpNm_y':'emplymShpNm'}, inplace=True)

# rename : Key-> pk 
test.rename(columns={'Key' : 'pk'}, inplace=True)

# 기획한 모델린 항목 순서로 정렬
columns = ['acptMthd','deadline','emplymShp','emplymShpNm','startDd','pk','jobcls','jobclsNm','oranNm','organYn','recrtTitle','stmId','stmNm','endDd','workPlc','workPlcNm','acptMthdCd','age','ageYn','clerk','clerkContt','clltPrnnum','createDy','detCnts','etcItm','homepage','plDetAddr','plbizNm','updDt']
test = test[columns]

#합친 데이터 csv로 저장하기
test.to_csv("new_jobs2020_final_check.csv", encoding='utf-8-sig')
#print(test)

# pk 이슈 체크
#value_counts = test['pk'].value_counts()
#print(value_counts)



