import FinanceDataReader as fdr
import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta

def send_telegram_msg(message):
    token = os.environ.get('TELEGRAM_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, data=data)
    except Exception as e:
        print(f"전송 오류: {e}")

# 1. 주도 업종(섹터) 분석 함수
def get_leading_sectors():
    df_desc = fdr.StockListing('KRX-DESC')
    df_all = fdr.StockListing('KRX')
    
    # 등락률 컬럼명 찾기 (ChgRate 또는 Ratio)
    rate_col = [col for col in df_all.columns if 'Rate' in col or 'Ratio' in col][0]
    
    merged = df_desc.merge(df_all[['Code', rate_col]], on='Code')
    sector_rank = merged.groupby('Sector')[rate_col].mean().sort_values(ascending=False)
    return sector_rank.head(3)

# 2. 메인 분석 시작
sector_rank = get_leading_sectors()
top_sector = sector_rank.index[0]

# 현재 1위 업종 내 종목 추출
df_desc = fdr.StockListing('KRX-DESC')
sector_stocks = df_desc[df_desc['Sector'] == top_sector].copy()
df_current = fdr.StockListing('KRX')
merged_df = sector_stocks.merge(df_current, on='Code')

# 거래대금 상위 10개 필터링
top_stocks = merged_df.sort_values(by='Amount', ascending=False).head(10)

# 리포트 헤더
report = f"🔥 <b>주도 업종: [{top_sector}]</b>\n"
report += f"업종 평균 등락: {sector_rank.iloc[0]:.2f}%\n"
report += "--------------------------------\n"
report += "<b>종목별 수급 (외인/기관)</b>\n\n"

# 3. 개별 종목 수급 상세 분석
# 데이터 수집 시작일 (최근 5일치 정도면 충분)
start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

for i, row in top_stocks.iterrows():
    code = row['Code']
    name = row['Name']
    
    try:
        # 투자자별 매매동향 가져오기
        df_investors = fdr.DataReader(code, start_date).tail(1)
        
        # 컬럼 존재 여부 확인 후 데이터 추출
        frn_net = int(df_investors['Foreign'].iloc[0]) if 'Foreign' in df_investors.columns else 0
        inst_net = int(df_investors['Institution'].iloc[0]) if 'Institution' in df_investors.columns else 0
        
        # 수급 상태 이모지
        frn_icon = "🔵" if frn_net > 0 else "⚪"
        inst_icon = "🟠" if inst_net > 0 else "⚪"
        
    except Exception as e:
        frn_net, inst_net = 0, 0
        frn_icon, inst_icon = "❓", "❓"

    amount_billion = round(row['Amount'] / 100000000)
    rate_val = row.get('ChgRate', row.get('Ratio', 0))
    
    report += f"<b>{name}</b> ({code})\n"
    report += f"현재: {int(row['Close']):,}({rate_val}%)\n"
    report += f"거래대금: {amount_billion:,}억\n"
    report += f"{frn_icon}외인: {frn_net:,} / {inst_icon}기관: {inst_net:,}\n\n"
    
    time.sleep(0.1) # API 부하 방지

# 리포트 푸터
report += "--------------------------------\n"
report += f"🥈 2위: {sector_rank.index[1]} ({sector_rank.iloc[1]:.2f}%)\n"
report += f"🥉 3위: {sector_rank.index[2]} ({sector_rank.iloc[2]:.2f}%)\n"
report += "<i>*수급은 전일 확정치 기준입니다.</i>"

# 4. 전송
send_telegram_msg(report)
print(f"[{top_sector}] 분석 리포트 발송 완료")
