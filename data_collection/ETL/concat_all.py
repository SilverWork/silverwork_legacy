'''
etl.py를 통해 만들어진 2018, 2020, 2022년도의 노인 공공 데이터와 그에 해당하는 상세공고 데이터가 모델링에 맞게 전처리 과정을 거쳐 3개의 csv 파일로 저장됐고, 그것들을 concat()을 통해 하나의 csv로 만드는 과정.
'''

import pandas as pd
df1 = pd.read_csv("new_jobs2018_final_check.csv", encoding='utf-8-sig')
#print(df1)
df2 = pd.read_csv("new_jobs2020_final_check.csv", encoding='utf-8-sig')
#print(df2)
df3 = pd.read_csv("new_jobs2022_final_check.csv", encoding='utf-8-sig')
#print(df3)

df1.drop(['workPlcNm'], axis=1, inplace=True)
df2.drop(['workPlcNm'], axis=1, inplace=True)
df3.drop(['workPlcNm'], axis=1, inplace=True)
df1.drop(['Unnamed: 0'], axis=1, inplace=True)
df2.drop(['Unnamed: 0'], axis=1, inplace=True)
df3.drop(['Unnamed: 0'], axis=1, inplace=True)

# 데이터 합치기 - 데이터프레임 변수 : data
df_list = [df1, df2, df3]
df = pd.concat(df_list, ignore_index = True )
#print(df)

#합친 데이터 csv로 저장하기-> snowflake에 벌크 업데이트할 용도로 index=False, float_format='%.0f' 처리함
df.to_csv('Jobs.csv', index=False, float_format='%.0f', encoding='utf-8-sig')
#print(df)

# 합친 데이터 잘 저장됐는지 체크하기
#df4 = pd.read_csv("Jobs.csv", encoding='utf-8-sig')
#print(df4)
#print(len(df4))
