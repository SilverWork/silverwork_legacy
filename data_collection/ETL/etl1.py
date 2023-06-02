import pandas as pd
# 공고 드로핑 + 이름
data = pd.read_csv("search_2018.csv", encoding='utf-8-sig') # encoding='utf-8-sig', encoding='cp949'
print(data.head())


#data.drop(['Unnamed: 0.2'], axis=1, inplace=True)
data.drop(['Unnamed: 0.1'], axis=1, inplace=True)
data.drop(['Unnamed: 0'], axis=1, inplace=True)
data.rename(columns={'frDd':'StartDd'}, inplace=True)
data.rename(columns={'toDd':'EndDd'}, inplace=True)
data.rename(columns={'jobId':'Key'}, inplace=True)
data.rename(columns={'StartDd':'startDd'}, inplace=True)
data.rename(columns={'EndDd':'endDd'}, inplace=True)
print(data.head())

#합친 데이터 csv로 저장하기
data.to_csv("jobs2018_dropped.csv", encoding='utf-8-sig') #인코딩고려

# 상세공고 드로핑 + 이름
data = pd.read_csv("jobinfo_2020.csv", encoding='utf-8-sig') # encoding='utf-8-sig', encoding='cp949'
print(data.head())

data.drop(['Unnamed: 0'], axis=1, inplace=True)
data.drop(['frAcptDd'], axis=1, inplace=True)
# data.drop(['jobId'], axis=1, inplace=True)
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
print(data.head())

#합친 데이터 csv로 저장하기
data.to_csv("jobs2020_details_dropped.csv", encoding='utf-8-sig') #인코딩고려


# 공고랑 공고상세 조인하기
df1 = pd.read_csv("jobs2020_dropped.csv", encoding='utf-8-sig') # encoding='utf-8-sig', encoding='cp949'
print(df1.head())
df2 = pd.read_csv("jobs2020_details_dropped.csv", encoding='utf-8-sig') # encoding='utf-8-sig', encoding='cp949'
print(df2.head())

# Key 값을 기준으로 Inner Join (1대1 대응이기 때문에 가능하다고 생각함)
# Key 값을 기준으로 Left Outer Join (일반적인 방법)
merged_df = pd.merge(left = df1 , right = df2, how = "inner", on = "Key")

merged_df.drop(['Unnamed: 0_x'], axis=1, inplace=True)
merged_df.drop(['Unnamed: 0_y'], axis=1, inplace=True)

print(merged_df)
value_counts = merged_df['Key'].value_counts()
print(value_counts)

#합친 데이터 csv로 저장하기
merged_df.to_csv("jobs2020_merged.csv", encoding='utf-8-sig')
df4 = pd.read_csv("jobs2020_merged.csv", encoding='utf-8-sig') # encoding='utf-8-sig', encoding='cp949'
print(len(df4))



# 전처리
df = pd.read_csv("jobs2020_merged.csv", encoding='utf-8-sig')


print(df)
# 전처리
# YYYYMMDD -> YYYY-MM-DD
def date_format(x):
    x = str(x)
    if len(x) == 8:
        return f'{x[:4]}-{x[4:6]}-{x[6:]}'
    else:
        return x
# 날짜 포맷 변경
# 'startDd', 'endDd'
df['startDd'] = df['startDd'].apply(lambda x: date_format(x))
df['endDd'] = df['endDd'].apply(lambda x: date_format(x))

def has_age_value(x):
    x = str(x)
    result = 'N'
    if x is not None:
        result = 'Y'
    return result


# age에 값이 있다면 ageYd 에 Y/N 넣어주기
df['ageYn'] = df['age'].apply(lambda x: has_age_value(x))
df.drop(['Unnamed: 0'], axis=1, inplace=True)
print(df)

#csv로 저장하기
df.to_csv("jobs2020_final_check.csv", encoding='utf-8-sig') #인코딩고려
