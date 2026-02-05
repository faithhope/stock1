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

MY_SECTORS = {
    '반도체': '반도체',
    '조선': '선박',
    '방산': '항공',
    '원전': '전기',
    '로봇': '기계',
    '자동차': '자동차'
}

try:
    print("🚀 지표 강화 리포트 로드 중...")
    
    # 1. 섹터 정보 로드
    df_desc = fdr.StockListing('KRX-DESC')
    
    # 2. 투자지표가 포함된 마켓 데이터 로드 (이게 핵심입니다)
    # KRX-MARCAP은 시총, PER, PBR 등이 포함된 일자별 데이터셋입니다.
    df_all = fdr.StockListing('KRX-MARCAP') 
    df_all.columns = df_all.columns.str.strip()

    # 3. 데이터 클렌징 (숫자 변환)
    for col in ['PER', 'PBR', 'Marcap', 'Amount', 'Close', 'ChgRate']:
        if col in df_all.columns:
            df_all[col] = pd.to_numeric(df_all[col], errors='coerce').fillna(0)

    # 4. 데이터 병합
    merged = df_desc.merge(df_all, on='Code')

    report = f"🎯 <b>핵심 지표 리포트 (PER/PBR 보완)</b>\n"
    report += f"기준: {(datetime.utcnow() + timedelta(hours=9)).strftime('%m/%d %H:%M')}\n\n"

    for label, keyword in MY_SECTORS.items():
        filtered = merged[merged['Sector'].str.contains(keyword, na=False)]
        if filtered.empty: continue
        
        # 거래대금(Amount) 순 TOP 5
        top_5 = filtered.sort_values(by='Amount', ascending=False).head(5)
        
        report += f"<b>[ {label} ]</b>\n"
        for _, row in top_5.iterrows():
            name = row.get('StockName') or row.get('Name_x') or row.get('Name')
            price = int(row['Close'])
            rate = row.get('ChgRate', 0)
            m_cap = round(row['Marcap'] / 1000000000000, 1)
            
            # 지표 추출 및 포맷팅 (0이거나 NaN이면 N/A 표시)
            per = row['PER']
            pbr = row['PBR']
            per_str = f"{per:.2f}" if per > 0.01 else "N/A"
            pbr_str = f"{pbr:.2f}" if pbr > 0.01 else "N/A"
            
            report += f"• <b>{name}</b>\n"
            report += f"  {price:,}원 ({rate:+.2f}%) | 시총 {m_cap}조\n"
            report += f"  PER: {per_str} | PBR: {pbr_str}\n"
        
        report += "--------------------------------\n"
        time.sleep(0.1)

    send_telegram_msg(report)
    print("성공")

except Exception as e:
    send_telegram_msg(f"❌ 지표 리포트 에러: {str(e)}")
