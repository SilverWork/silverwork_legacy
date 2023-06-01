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
    response = requests.get(url)
    worker_id = threading.get_ident()


    while True:
        try:
            JobList_response = requests.get(url).text.encode('utf-8')
            time.sleep(0.5)
        except:
            continue
        break

    
    house = BeautifulSoup(JobList_response, 'lxml-xml')
    te = house.find('item')
    print(f'te:{te}')

    #관련 에러  : <errMsg>SERVICE ERROR</errMsg>
    #에러 원인 예상 : 연속적인 요청시
    if isinstance(te, type(None)) is True:
        print('서비스 에러입니다.')
        time.sleep(5) #추가한 구문
    else:
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
executor = concurrent.futures.ThreadPoolExecutor(max_workers=300)  # 4개의 스레드 사용

key = 'SrFzjmH%2BgKmA7CsGoHxtz1P5KyaTntW7512fKrftU02QY6VgGwQF2McCl3TzV1usyFiNHS7xvPYlZ0gBiJnRmQ%3D%3D'
#SrFzjmH%2BgKmA7CsGoHxtz1P5KyaTntW7512fKrftU02QY6VgGwQF2McCl3TzV1usyFiNHS7xvPYlZ0gBiJnRmQ%3D%3D
#vkAm3dSI%2FvKLoPmiJZ7Ri%2BaQTPDmB151XefclOprrjay2WQ%2FfuYAu6Q%2FMO%2FHH8xScD8vqgtSn11NLBNefSjH2A%3D%3D
url = f'http://apis.data.go.kr/B552474/SenuriService/getJobList?serviceKey={key}&id=K172112305260071'



# 병렬 처리할 API 요청 URL 리스트
api_urls = []

for i in range(100):
    api_urls.append(url)

cnt = -1
error_idx1=[]
error_idx2=[]
error_idx3=[]
datas = []
start_time = time.time()  # 작업 시작 시간 기록

# 각 URL에 대해 병렬로 API 요청을 처리
futures = [executor.submit(process_api_request, url) for url in api_urls]


# 결과 확인
for future in concurrent.futures.as_completed(futures):
    cnt+=1
    print(cnt)
    try:
        result = future.result()
    except Exception as e:
        print(e)
        
    





end_time = time.time()                  # 작업 종료 시간 기록
elapsed_time = end_time - start_time    # 작업에 걸린 시간 계산
print(elapsed_time)