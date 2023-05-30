import pandas as pd

# csv 데이터프레임 생성
csv_path = '../resource/jobs.csv'
df = pd.read_csv(csv_path, encoding='utf-8-sig')


print(df)
