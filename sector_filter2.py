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

# 섹터 키워드 매핑 (원하시는 대로 수정 가능)
MY_SECTORS = {
    '반도체': '반도체',
    '조선': '선박',
    '방산': '항공기',
    '원전': '전기',
    '로봇': '기계',
    '자동차': '자동차'
}

try:
    print("🚀 데이터 분석 시작...")
    df_desc = fdr.StockListing('KRX-DESC')
    df_all = fdr.StockListing('KRX')
    df_all.columns = df_all.columns.str.strip()

    # 1. 컬럼 존재 여부 확인 및 데이터 클렌징
    # PER, PBR 컬럼이 없는 경우를 대비해 기본값 0으로 초기화된 컬럼 생성
    for col in ['PER', 'PBR', 'Marcap', 'Amount']:
        if col not in df_all.columns:
            df_all[col] = 0  # 컬럼이 없으면 0으로 채운 컬럼 생성
        else:
            # 존재한다면 숫자형으로 변환 (하이픈 '-' 등 에러 방지)
            df_all[col] = pd.to_numeric(df_all[col], errors='coerce').fillna(0)

    rate_col = next((c for c in df_all.columns if 'Ratio' in c or 'Rate' in c), 'ChgRate')
    merged = df_desc.merge(df_all, on='Code')

    report = f"🎯 <b>섹터별 수급 및 지표 리포트</b>\n"
    report += f"기준: {datetime.now().strftime('%m/%d %H:%M')}\n\n"

    for label, keyword in MY_SECTORS.items():
        # 섹터 필터링 (keyword 포함 여부)
        filtered = merged[merged['Sector'].str.contains(keyword, na=False)]
        if filtered.empty: continue
        
        # 거래대금(Amount) 순 TOP 5
        top_5 = filtered.sort_values(by='Amount', ascending=False).head(5)
        
        report += f"<b>[ {label} ]</b>\n"
        
        for _, row in top_5.iterrows():
            # 병합 시 이름 중복 처리
            name = row.get('StockName') or row.get('Name_x') or row.get('Name')
            price = int(row['Close'])
            rate = row[rate_col]
            m_cap = round(row['Marcap'] / 1000000000000, 1) # 조 단위
            
            # PER, PBR 표시 (0보다 큰 경우만 수치 표시, 아니면 N/A)
            per_val = row['PER']
            pbr_val = row['PBR']
            per_str = f"{per_val:.1f}" if per_val > 0 else "N/A"
            pbr_str = f"{pbr_val:.2f}" if pbr_val > 0 else "N/A"
            
            report += f"• <b>{name}</b>\n"
            report += f"  {price:,}원 ({rate}%) | 시총 {m_cap}조\n"
            report += f"  PER: {per_str} | PBR: {pbr_str}\n"
        
        report += "--------------------------------\n"
        time.sleep(0.1)

    send_telegram_msg(report)
    print("✨ 전송 완료!")

except Exception as e:
    import traceback
    print(traceback.format_exc())
    send_telegram_msg(f"❌ 에러 발생: {str(e)}")
