import FinanceDataReader as fdr
import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta

def send_telegram_msg(message):
    token = os.environ.get('TELEGRAM_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')
    if not token or not chat_id: return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    requests.post(url, data=data)

# 내가 보고 싶은 섹터 정의 (KRX-DESC 기준 키워드)
MY_SECTORS = ['반도체', '조선', '방산', '원자력', '로봇', '자동차']

try:
    print("데이터 수집 시작...")
    df_desc = fdr.StockListing('KRX-DESC')
    df_all = fdr.StockListing('KRX')
    df_all.columns = df_all.columns.str.strip()

    # 컬럼 매핑 (오타 대응 포함)
    rate_col = next((c for c in df_all.columns if 'Ratio' in c or 'Rate' in c), 'ChgRate')
    
    # 데이터 병합 (시가총액, PER, PBR 등은 KRX 기본 데이터에 포함됨)
    # Marcap: 시가총액, PER: PER, PBR: PBR
    merged = df_desc.merge(df_all, on='Code')

    report = f"📊 <b>관심 섹터별 수급 TOP 5</b>\n"
    report += f"기준 시각: {datetime.now().strftime('%m/%d %H:%M')}\n\n"

    for target in MY_SECTORS:
        # 해당 키워드가 포함된 섹터 필터링
        filtered = merged[merged['Sector'].str.contains(target, na=False)]
        if filtered.empty: continue
        
        # 거래대금(Amount) 순으로 TOP 5 추출
        top_5 = filtered.sort_values(by='Amount', ascending=False).head(5)
        
        report += f"<b>[ {target} ]</b>\n"
        
        for _, row in top_5.iterrows():
            name = row['Name_x'] if 'Name_x' in row else row['Name']
            price = int(row['Close'])
            rate = row[rate_col]
            # 시총(Marcap)은 보통 '원' 단위이므로 조 단위로 변환
            m_cap = round(row['Marcap'] / 1000000000000, 1) if 'Marcap' in row else 0
            per = row.get('PER', 'N/A')
            pbr = row.get('PBR', 'N/A')
            
            report += f"• <b>{name}</b>\n"
            report += f"  {price:,}원 ({rate}%) | 시총 {m_cap}조\n"
            report += f"  PER: {per} | PBR: {pbr}\n"
        
        report += "--------------------------------\n"
        time.sleep(0.1)

    send_telegram_msg(report)
    print("리포트 전송 성공!")

except Exception as e:
    print(f"에러: {e}")
    send_telegram_msg(f"❌ 섹터 리포트 에러: {e}")
