import pandas as pd


class EmploymentAnalyzer:
    def __init__(self, data_file, output_file):
        self.data_file = data_file
        self.output_file = output_file
        self.name_list = ['year', 'emplymShp']
        self.row_list = []
        self.prefer_info = {
            '관련 학과(학교) 졸업자': ['대졸', '졸업', '학과', '전공', '전공자', '특성화고', '마이스터고'],
            '차량운전(운전면허증 소지자)': ['차량 소지자', '운전면허증', '운전면허'],
            '장애인': ['장애인', '경증'],
            '국가유공자 및 보훈취업지원대상자': ['국가보훈', '보훈대상자', '보훈취업', '보훈', '보훈보상대상자', '국가유공자카드', '국가유공', '국가유공자', '독립유공자', '5·18민주유공자', '유공자'],
            '경력자(경험자)': ['경험', '근무', '근무 경력', '해당분야 경력', '유관업무', '종사자', '종사', '경험', '유경험자', '유경험', '有 경험자'],
            '관련 자격증 소지자(관련 교육 이수자)': ['기술자격증', '자격증', '산업기사', '관련 자격', '기사', '자격 소지', '(자격)', '이수', '이수증', '이수자', '측량가능자', '측량', '지게차', '회계', '능력 우대'],
            '컴퓨터 활용 능력': ['프로그래밍', '영상편집', 'HTML', 'CSS', '일러스트레이터', '스마트폰', '포토샵', '일러스트', 'ms office', '엑셀', '파워포인트', 'OA', '컴퓨터', 'MS-Office', '워드', 'Word', 'word', '워드프로세서', 'MOS', '컴퓨터활용능력', '컴퓨터 활용'],
            '60세이하': ['(준)고령자(50세이상)', '장년', '장년우대', '청년', '청년층', '청년층우대'],
            '취업 취약 계층 및 고용장려 대상자': ['기초생활수급자', '결혼이민자', '가정폭력피해자', '범죄구조피해자', '갱생보호대상자', '1년 이상 장기실업자', '저소득자', '취업보호', '고용촉진장려금대상자', '복지카드 수당', '취약계층', '저소득층', '성매매피해자', '경력단절여성', '경력단절여성우대', '다문화'],
            '외국어 능력': ['토익', 'TOEIC', '영어', '외국어', '일본어', 'JPT', 'JLPT', '중국어', 'HKS', '러시아어', 'TORFL', '국외 출장', '해외출장', '해외 출장'],
            '북한이탈주민': ['북한이탈주민', '탈북'],
            '제대군인': ['장기복무', '제대군인', '군인', '군 경력', '전역', '간부'],
            '인근 거주자': ['인근 거주', '통근버스', '인근', '거주', '가까운', '인근거주자'],
            '즉시근무': ['즉시 출근 가능자', '즉시', '바로 근무'],
            '장기근무': ['장기 근무 가능자', '장기 근무', '장기간'],
            '주말(휴일) 근무 가능자': ['주말근무', '휴일근무', '주말 근무', '공휴일 근무'],
            '여성우대': ['여성우대']
        }
        self.prefer_count = {key: 0 for key in self.prefer_info.keys()}

    def analyze_employment(self):
        datas = pd.read_csv(self.data_file)

        for etc_itm in datas['etcItm']:
            if etc_itm == '':
                continue
            for key in self.prefer_info.keys():
                for value in self.prefer_info[key]:
                    if value in str(etc_itm):
                        self.prefer_count[key] += 1
                        break

        for key, value in self.prefer_count.items():
            print(f'{key} : {value}')
            self.row_list.append([key, value])

        result_df = pd.DataFrame(self.row_list, columns=self.name_list)
        result_df.to_csv(self.output_file, encoding='utf-8', index=False)
        print(f"분석 결과가 {self.output_file}에 저장되었습니다.")


# 예시 사용법
analyzer = EmploymentAnalyzer(data_file='C:/Users/ASUS/Documents/Data_Enginearing_Dev_cousre/test/job_detail_all.csv',
                                  output_file='emplymShp.csv')
analyzer.analyze_employment()
