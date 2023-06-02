'''
xlml 없다면 install하기 >>>pip install lxml
'''

import concurrent.futures
import time

import requests
import threading

from bs4 import BeautifulSoup
import pandas as pd
from urllib.request import urlopen
from urllib.error import HTTPError
from urllib.error import URLError
import time


# API 요청을 처리하는 함수
def process_api_request(url):
    global idx 
    global num 
    global datas
    global total_count
    columns_form = ['acptMthdCd', 'age', 'ageLim', 'clerk', 'clerkContt', 'clltPrnnum', 'createDy',
                                    'detCnts', 'etcItm', 'frAcptDd', 'homepage', 'jobId', 'lnkStmId', 'organYn',
                                    'plDetAddr', 'plbizNm', 'repr', 'stmId', 'toAcptDd', 'updDy', 'wantedAuthNo',
                                    'wantedTitle']

    worker_id = threading.get_ident()
    print(f"{worker_id} doing job.........")

    
    print(f'{idx}/{total_count}')
    idx += 1

    # 배치 500 기준 로그 csv 파일 생성
    if idx % 501 == 0:
        num += 1
        df = pd.DataFrame(datas, columns=columns_form)
        df.to_csv(f"2020_details_log_{num}.csv", encoding='utf-8-sig')
        print(f'{num}번째 csv파일로 저장 완료했습니다!')

    # 최종 csv 파일 생성
    if total_count <= idx:
        df = pd.DataFrame(datas, columns=columns_form)
        df.to_csv(f"2020_details.csv", encoding='utf-8-sig')
        print(f'모든 데이터를 csv파일로 저장 완료했습니다!')

    # api 요청
    while True:
        try:
            response = requests.get(url)
            house = BeautifulSoup(response.text, 'lxml-xml')
            te = house.find('item')
            #print(f'te:{te}')

            if isinstance(te, type(None)) is True:
                print('api 요청시 SERVICE ERROR 발생... 다시 요청중...')
                time.sleep(5)  
                continue
            else:
                break
        except Exception as e:
            print(e)
            continue
        break

    # 항목별 값이 없으면 'None'으로 대체
    if isinstance(te.acptMthdCd, type(None)) is True:
        acptMthdCd = 'None'
    else:
        acptMthdCd = te.acptMthdCd.string.strip()
    if isinstance(te.age, type(None)) is True:
        age = 'None'
    else:
        age = te.age.string.strip()
    if isinstance(te.ageLim, type(None)) is True:
        ageLim = 'None'
    else:
        ageLim = te.ageLim.string.strip()
    if isinstance(te.clerk, type(None)) is True:
        clerk = 'None'
    else:
        clerk = te.clerk.string.strip()
    if isinstance(te.clerkContt, type(None)) is True:
        clerkContt = 'None'
    else:
        clerkContt = te.clerkContt.string.strip()
    if isinstance(te.clltPrnnum, type(None)) is True:
        clltPrnnum = 'None'
    else:
        clltPrnnum = te.clltPrnnum.string.strip()
    if isinstance(te.createDy, type(None)) is True:
        createDy = 'None'
    else:
        createDy = te.createDy.string.strip()
    if isinstance(te.detCnts, type(None)) is True:
        detCnts = 'None'
    else:
        detCnts = te.detCnts.string.strip()
    if isinstance(te.etcItm, type(None)) is True:
        etcItm = 'None'
    else:
        etcItm = te.etcItm.string.strip()
    if isinstance(te.frAcptDd, type(None)) is True:
        frAcptDd = 'None'
    else:
        frAcptDd = te.frAcptDd.string.strip()
    if isinstance(te.homepage, type(None)) is True:
        homepage = 'None'
    else:
        homepage = te.homepage.string.strip()
    if isinstance(te.jobId, type(None)) is True:
        jobId = 'None'
    else:
        jobId = te.jobId.string.strip()
    if isinstance(te.lnkStmId, type(None)) is True:
        lnkStmId = 'None'
    else:
        lnkStmId = te.lnkStmId.string.strip()
    if isinstance(te.organYn, type(None)) is True:
        organYn = 'None'
    else:
        organYn = te.organYn.string.strip()
    if isinstance(te.plDetAddr, type(None)) is True:
        plDetAddr = 'None'
    else:
        plDetAddr = te.plDetAddr.string.strip()
    if isinstance(te.plbizNm, type(None)) is True:
        plbizNm = 'None'
    else:
        plbizNm = te.plbizNm.string.strip()
    if isinstance(te.repr, type(None)) is True:
        repr = 'None'
    else:
        repr = te.repr.string.strip()
    if isinstance(te.stmId, type(None)) is True:
        stmId = 'None'
    else:
        stmId = te.stmId.string.strip()
    if isinstance(te.toAcptDd, type(None)) is True:
        toAcptDd = 'None'
    else:
        toAcptDd = te.toAcptDd.string.strip()
    if isinstance(te.updDy, type(None)) is True:
        updDy = 'None'
    else:
        updDy = te.updDy.string.strip()
    if isinstance(te.wantedAuthNo, type(None)) is True:
        wantedAuthNo = 'None'
    else:
        wantedAuthNo = te.wantedAuthNo.string.strip()
    if isinstance(te.wantedTitle, type(None)) is True:
        wantedTitle = 'None'
    else:
        wantedTitle = te.wantedTitle.string.strip()

    data = [acptMthdCd, age, ageLim, clerk, clerkContt, clltPrnnum, createDy, detCnts, etcItm, frAcptDd,
            homepage, jobId, lnkStmId, organYn, plDetAddr, plbizNm, repr, stmId, toAcptDd, updDy,
            wantedAuthNo, wantedTitle]
    datas.append(data)

    print(str(worker_id) + ":::" + response.text[0:100])


# 병렬 처리를 위한 ThreadPoolExecutor 생성
executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)  # n 개의 스레드

# 공공데이터 api 요청 시 필요한 발급받은 ServiceKey 값
key = 'vkAm3dSI%2FvKLoPmiJZ7Ri%2BaQTPDmB151XefclOprrjay2WQ%2FfuYAu6Q%2FMO%2FHH8xScD8vqgtSn11NLBNefSjH2A%3D%3D'
# SrFzjmH%2BgKmA7CsGoHxtz1P5KyaTntW7512fKrftU02QY6VgGwQF2McCl3TzV1usyFiNHS7xvPYlZ0gBiJnRmQ%3D%3D
# vkAm3dSI%2FvKLoPmiJZ7Ri%2BaQTPDmB151XefclOprrjay2WQ%2FfuYAu6Q%2FMO%2FHH8xScD8vqgtSn11NLBNefSjH2A%3D%3D

# 병렬 처리할 API 요청 URL 리스트 만들기
idx = 0 # 데이터 인덱스 
num = 0 # 생성될 csv 파일 번호
datas = [] 
api_urls=[] 
start_time = time.time()  # 작업 시작 시간 기록

csv = pd.read_csv('search_2020.csv', encoding='utf-8-sig')  
jobId = csv['jobId'] # jobId(공고의 Key값) 리스트로 저장
jobId_list = jobId.values.tolist()

for jobId in jobId_list: # 상세공고 url 리스트로 저장
    api_urls.append(f'http://apis.data.go.kr/B552474/SenuriService/getJobInfo?ServiceKey={key}&id={jobId}')

total_count = len(api_urls)
api_urls = api_urls[::-1] # 1월부터 수집
print(api_urls[:10]) 

# 각 URL에 대해 병렬로 API 요청을 처리
futures = [executor.submit(process_api_request, url) for url in api_urls]

# 결과 확인
for future in concurrent.futures.as_completed(futures):
    try:
        result = future.result()
    except Exception as e:
        print(e)

# 시간 체크
end_time = time.time()  # 작업 종료 시간 기록
elapsed_time = end_time - start_time  # 작업에 걸린 시간 계산
print(elapsed_time)
