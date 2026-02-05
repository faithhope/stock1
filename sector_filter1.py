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

# 섹터 키워드 (KRX 섹터 명칭에 포함된 단어들)
MY_SECTORS = {
    '반도체': '반도체',
    '조선': '선박',
    '방산': '항공기',
    '원전': '전기장비',
    '로봇': '특수 목적용 기계',
    '자동차': '자동차'
}

try:
    print("데이터 로드 중...")
    df_desc = fdr.StockListing('KRX-DESC')
    df_all = fdr.StockListing('KRX')
    df_all.columns = df_all.columns.str.strip()

    # 데이터 타입 강제 변환 (에러 방지 핵심)
    # 숫자가 아닌 값('-', 'N/A')을 NaN으로 바꾸고 0으로 채움
    cols_to_fix = ['Close', 'Marcap', 'Amount', 'PER', 'PBR']
    for col in cols_to_fix:
        if col in df_all.columns:
            df_all[col] = pd.to_numeric(df_all[col], errors='coerce').fillna(0)

    rate_col = next((c for c in df_all.columns if 'Ratio' in c or 'Rate' in c), 'ChgRate')
    merged = df_desc.merge(df_all, on='Code')

    report = f"🎯 <b>핵심 섹터별 리포트</b>\n"
    report += f"기준: {datetime.now().strftime('%m/%d %H:%M')}\n\n"

    for label, keyword in MY_SECTORS.items():
        # 섹터 필터링
        filtered = merged[merged['Sector'].str.contains(keyword, na=False)]
        if filtered.empty: continue
        
        # 거래대금(Amount) 순 TOP 5
        top_5 = filtered.sort_values(by='Amount', ascending=False).head(5)
        
        report += f"<b>[ {label} ]</b>\n"
        
        for _, row in top_5.iterrows():
            name = row['Name_x'] if 'Name_x' in row else row['Name']
            price = int(row['Close'])
            rate = row[rate_col]
            m_cap = round(row['Marcap'] / 1000000000000, 1) # 조 단위
            
            # 지표 값이 0(NaN)인 경우 N/A 표시
            per = row['PER'] if row['PER'] > 0 else "N/A"
            pbr = row['PBR'] if row['PBR'] > 0 else "N/A"
            
            report += f"• <b>{name}</b>\n"
            report += f"  {price:,}원 ({rate}%) | 시총 {m_cap}조\n"
            report += f"  PER: {per} | PBR: {pbr}\n"
        
        report += "--------------------------------\n"
        time.sleep(0.1)

    send_telegram_msg(report)
    print("전송 성공!")

except Exception as e:
    import traceback
    err_detail = traceback.format_exc()
    print(err_detail)
    send_telegram_msg(f"❌ 에러 상세 발생:\n{str(e)}")
