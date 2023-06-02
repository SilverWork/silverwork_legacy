import pandas as pd
df1 = pd.read_csv("new_jobs2018_final_check.csv", encoding='utf-8-sig')
print(df1)
df2 = pd.read_csv("new_jobs2020_final_check.csv", encoding='utf-8-sig')
print(df2)
df3 = pd.read_csv("new_jobs2022_final_check.csv", encoding='utf-8-sig')
print(df3)

df1.drop(['workPlcNm'], axis=1, inplace=True)
df2.drop(['workPlcNm'], axis=1, inplace=True)
df3.drop(['workPlcNm'], axis=1, inplace=True)
df1.drop(['Unnamed: 0'], axis=1, inplace=True)
df2.drop(['Unnamed: 0'], axis=1, inplace=True)
df3.drop(['Unnamed: 0'], axis=1, inplace=True)
#합친 데이터 csv로 저장하기 - 인덱스 제외


# 데이터 합치기 - 데이터프레임 변수 : data
df_list = [df1, df2, df3]
df = pd.concat(df_list, ignore_index = True )

print(df)

#합친 데이터 csv로 저장하기- snowflake용도로 index=False
df.to_csv('Jobs.csv', index=False, float_format='%.0f', encoding='utf-8-sig')
print(df)
'''
columns = ['acptMthd','deadline','emplymShp','emplymShpNm','startDd','pk','jobcls','jobclsNm','oranNm','organYn','recrtTitle','stmId','stmNm','endDd','workPlc','acptMthdCd','age','ageYn','clerk','clerkContt','clltPrnnum','createDy','detCnts','etcItm','homepage','plDetAddr','plbizNm','updDt']
df  = df[columns]
# 합친 데이터 잘 저장됐는지 체크하기
data_modified = pd.read_csv("Jobs.csv",encoding='utf-8-sig')
data_modified.tail()
'''

df4 = pd.read_csv("Jobs.csv", encoding='utf-8-sig')
print(df4)
print(len(df4))