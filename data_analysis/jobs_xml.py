import xml.etree.ElementTree as ET
import pandas as pd
from constants import JOBS_OPTIONS

# XML 파싱
xml_path = '../resource/jobs.xml'
tree = ET.parse(xml_path)
root = tree.getroot()

# XML 데이터 추출
data = []
for item in root.findall('.//item'):  # 현재 요소의 모든 하위 요소를 검색하는 XPath 표현식
    row = {}

    for option in JOBS_OPTIONS:
        element = item.find(option)
        row[option] = element.text if element is not None else None

    data.append(row)

# DataFrame 생성
df = pd.DataFrame(data)

# CSV 생성
df.to_csv("../resource/jobs.csv", encoding='utf-8-sig')