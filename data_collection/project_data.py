import pandas as pd
import numpy as np
import requests
import json


def get_content(path, page='1', perPage='10'):
    url = 'https://api.odcloud.kr/api' + path
    params ={'serviceKey': 'JhXXmb4BE+1Exy0IOcy0L9T9v7Qm8fgqKy0nZIl2iH6pd5s+lqVZKHqWJGPZTq8RnQJvUruKv3H3a/zyoDjoYQ==', 'page': page, 'perPage': perPage}

    response = requests.get(url, params=params)
    content = response.content
    # byte decode
    content = content.decode('utf-8-sig')
    # dictionary로 변경
    content = json.loads(content)

    return content


def get_adm_info():
    df = pd.DataFrame()
    for sig_cd in range(1, 6):
        url = 'http://api.vworld.kr/req/data'
        params ={
            'request': 'GetFeature',
            'key': '9FBC48F7-7677-368C-B8FB-E846BF6AECE3',
            'size': 1000,
            'data': 'LT_C_ADSIGG_INFO',
            'attrfilter': f'sig_cd:like:{sig_cd}',
            'columns': 'sig_cd,full_nm,sig_kor_nm',
            'geometry': 'false'
        }

        response = requests.get(url, params=params)
        content = response.content
        # byte decode
        content = content.decode('utf-8-sig')
        # dictionary로 변경
        content = json.loads(content)
        features = content.get('response', {}).get('result', {}).get('featureCollection', {}).get('features', [])

        result = []
        for feature in features:
            properties = feature.get('properties', {})
            result.append({
                'adm_dist_cd': '{0:0<10}'.format(properties.get('sig_cd')), 
                'adm_prov_nm': properties.get('full_nm', '').split(' ')[0],
                'adm_dist_nm': properties.get('sig_kor_nm')
            })
        # dictionary를 dataframe으로 변경
        adm_info = pd.DataFrame.from_dict(result)
        df = pd.concat([df, adm_info], axis=0)
    
    df.reset_index(drop=True, inplace=True)
    return df


# 오류 발생했던 부분 리턴값이 3개여야 함
def find_adm_info(df, adm_dist_nm):
    if adm_dist_nm is not None:
        # 해당 문자열로 시작하는 경우 필터링
        result = df[df['adm_dist_nm'].str.startswith(adm_dist_nm, na=False)]
        if not result.empty:
            result = list(result.itertuples(index=False, name=None))
            if len(result) == 1:
                return result[0]
            elif len(result) > 1 and adm_dist_nm.endswith('시'):
                adm_dist_cd = result[0][0]
                adm_dist_cd = adm_dist_cd[:4] +  '0' + adm_dist_cd[5:]
                adm_prov_nm = result[0][1].split(' ')[0]
                return adm_dist_cd, adm_prov_nm, adm_dist_nm
    return None, None, adm_dist_nm


# csv 파일 읽어오는 부분
def csv_to_dataframe(file_path):
    try:
        # dataframe으로 변경
        # utf-8-sig로 인코딩하는 경우 에러가 발생해서 변경
        return pd.read_csv(file_path, encoding='CP949')
    except Exception as e:
        print(repr(e))
        return pd.DataFrame()


def find_common_code_info(df, code_id):
    if code_id is not None:
        # 코드가 숫자여서 dataframe에서 float타입으로 되어있어 string으로 바꾸기전 int로 형변환
        if isinstance(code_id, float):
            code_id = int(code_id)
        code_id = str(code_id)
        result = df[df['코드_ID'].str.startswith(code_id, na=False)]
        if not result.empty:
            result = result['코드명'].tolist()
            if len(result) == 1:
                return result[0]
    return code_id


def find_proj_type_code_info(df, code_id, code_nm):
    if code_id is not None:
        code_id = str(code_id)
        result = df[df['종류'].str.startswith(code_id, na=False)]
        if not result.empty:
            result = result['클렌징 데이터'].tolist()
            if len(result) == 1:
                if result[0] is not None:
                    return code_id, result[0].replace(' ', '')
    if code_nm is not None:
        return code_id, code_nm.replace(' ', '')
    return code_id, code_nm


def get_project_type(df, proj_type_nm):
    if proj_type_nm is not None:
        # 사업유형이름(사업유형코드) -> 사업유형코드, 사업유형이름
        # 문자열 뒷부분부터 해당 문자를 검색
        idx_first = proj_type_nm.rfind('(')
        idx_last = proj_type_nm.rfind(')')
        if idx_first >= 0 and idx_last >= 0:
            return find_proj_type_code_info(df, proj_type_nm[idx_first+1:idx_last], proj_type_nm[:idx_first])
    return find_proj_type_code_info(df, None, proj_type_nm)


# YYYYMMDD -> YYYY-MM-DD
def date_format(x):
    if len(x) == 8:
        return f'{x[:4]}-{x[4:6]}-{x[6:]}'
    else:
        return x


if __name__ == "__main__":
    # 컬럼명
    columns = {
        '사업유형': 'projType',
        '사업번호': 'projNo',
        '사업계획변경순번': 'projPlanChangeNo',
        '사업년도': 'projYear',
        '계속사업여부': 'contProjYn',
        '계속사업시작년도': 'contProjStartYear',
        '사업유형코드': 'projTypeCd',
        '사업유형이름': 'projTypeNm',
        '비예산여부': 'nonBudgYn',
        '특수사업명코드': 'specProjCd',
        '사업명': 'projNm',
        '관할시도명': 'admProvNm',
        '시군구코드': 'admDistCd',
        '관할시군구': 'admDistNm',
        '기관아이디': 'institutionId',
        '사업기간시작일': 'projStartDd',
        '사업기간종료일': 'projEndDd',
        '사업계획서상태코드': 'planStatusCd',
        '목표일자리수': 'targetEmployment',
        '최초등록첨부파일': 'firstlAttachment',
        '최근승인첨부파일': 'recentApprovalAttachment',
        '삭제여부': 'delYn',
        
    }

    # 지역 정보
    adm_info = get_adm_info()
    print(adm_info)

    # 공통상세코드
    common_code_info = csv_to_dataframe(file_path="resource/한국노인인력개발원_취업연계 공통상세코드_20200924.csv")
    print(common_code_info)


    # 사업유형코드
    proj_type_code_info = csv_to_dataframe(file_path="resource/한국노인인력개발원_사업유형코드.csv")
    print(proj_type_code_info)


    df = pd.DataFrame()

    paths = {
        2018: '/15050148/v1/uddi:86a1651f-3c36-4efe-85f8-d781d61b70d1',
        2020: '/15050148/v1/uddi:4f2b83dd-93d0-4d82-b6eb-c1af32641817',
        # 2022: '/15050148/v1/uddi:c2a15a01-c539-4698-a06d-daa5309264a4',
        2023: '/15050148/v1/uddi:abd1cfb1-5ba2-491f-9729-84bba214f87d'
    }

    for year, path in paths.items():
        # totalCount 가져오기
        content = get_content(path=path, page='1', perPage='1')
        total_count = content['totalCount'] if 'totalCount' in content else 0
        print('{}년도 총 데이터 수: {}'.format(year, total_count))

        # 데이터 가져오기
        page = 0
        PERPAGEMAX = 500

        while total_count > 0:
            page += 1
            per_page = PERPAGEMAX if total_count > PERPAGEMAX else total_count
            total_count -= per_page
            # data 가져오기
            content = get_content(path=path, page=str(page), perPage=str(per_page))
            if 'data' in content:
                total_count += content['currentCount']
                data = pd.DataFrame.from_dict(content['data'])

                # 예산구분, 비예산여부 통일
                if '예산구분' in data:
                    data['비예산여부'] = data['예산구분'].apply(lambda x: 'Y' if x is None else 'N')
                if '계속사업시작연도' in data:
                    data.rename(columns = {'계속사업시작연도' : '계속사업시작년도'}, inplace = True)

                df = pd.concat([df, data], ignore_index=True)

    print('3개년 총 데이터 수: {}'.format(len(df)))

    # np.nan -> None으로 변경
    df = df.replace({np.nan: None})

    # 계속사업시작년도,  기관아이디 타입 변경 (object -> float)
    df = df.astype({'계속사업시작년도': 'float', '기관아이디': 'float'}, errors='ignore')

    # 날짜 포맷 변경
    # '사업기간시작일', '사업기간종료일'
    df['사업기간시작일'] = df['사업기간시작일'].apply(lambda x: date_format(x))
    df['사업기간종료일'] = df['사업기간종료일'].apply(lambda x: date_format(x))

    # 에러발생했던 부분 axis=1(columns), result_type='expand' 옵션을 사용해서 새로운 컬럼도 생성했는데 리턴 길이가 다르다는 오류 발생
    # apply 부분에 적용된 함수 리턴값의 문제
    # '사업유형코드', '사업유형이름' 분리
    df[['사업유형코드', '사업유형이름']] = df.apply(lambda x: get_project_type(proj_type_code_info, x['사업유형코드']), axis=1, result_type='expand')

    # '시군구코드', '관할시도명', '관할시군구'
    df[['시군구코드', '관할시도명', '관할시군구']] = df.apply(lambda x: find_adm_info(adm_info, x['관할시군구']), axis=1, result_type='expand')

    # 코드ID -> 코드명으로 변경
    # '사업계획서상태코드', '특수사업유형코드'
    df['사업계획서상태코드'] = df['사업계획서상태코드'].apply(lambda x: find_common_code_info(common_code_info, x))
    df['특수사업명코드'] = df['특수사업명코드'].apply(lambda x: find_common_code_info(common_code_info, x))

    # 컬럼명 변경
    df.rename(columns=columns, inplace=True)
    # dictionary value값을 열로 가지는 열만 가져옴
    df = df[columns.values()]

    print(df)
    df.to_csv('resource/projects.csv', index=False, float_format='%.0f', encoding='utf-8-sig')