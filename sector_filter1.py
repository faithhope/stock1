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

try:
    # 1. 한국 시간 설정 (UTC+9)
    # 깃허브 서버 시간 오차를 방지하기 위해 한국 시간을 계산합니다.
    now_kst = datetime.utcnow() + timedelta(hours=9)
    start_date = (now_kst - timedelta(days=15)).strftime('%Y-%m-%d')

    # 2. 데이터 로드 및 전처리 (기존 로직 동일)
    df_desc = fdr.StockListing('KRX-DESC')
    df_all = fdr.StockListing('KRX')
    df_all.columns = df_all.columns.str.strip()
    df_desc.columns = df_desc.columns.str.strip()

    rate_col = next((c for c in df_all.columns if 'Ratio' in c or 'Rate' in c), None)
    amount_col = next((c for c in ['Amount', '거래대금'] if c in df_all.columns), 'Amount')
    
    df_all_sub = df_all[['Code', 'Name', rate_col, amount_col, 'Close']].copy()
    df_all_sub.columns = ['Code', 'StockName', 'Rate', 'Amount', 'Close']

    merged = df_desc.merge(df_all_sub, on='Code')
    merged['Rate'] = pd.to_numeric(merged['Rate'], errors='coerce')
    sector_rank = merged.groupby('Sector')['Rate'].mean().sort_values(ascending=False)
    top_sector = sector_rank.index[0]

    # 3. 리포트 생성
    top_stocks = merged[merged['Sector'] == top_sector].sort_values(by='Amount', ascending=False).head(10)
    
    report = f"🚀 <b>클라우드 리포트: [{top_sector}]</b>\n"
    report += f"평균 등락: {sector_rank.iloc[0]:.2f}%\n"
    report += "--------------------------------\n"

    for i, row in top_stocks.iterrows():
        try:
            # 수급 데이터 로드
            df_invest = fdr.DataReader(row['Code'], start_date)
            
            # [핵심] 외인/기관 데이터가 0이 아닌 마지막 영업일 데이터를 찾습니다.
            # 장중(오전 10시)에는 오늘 데이터가 0으로 나오므로, 필터링을 통해 '전일 확정치'를 집습니다.
            valid_df = df_invest[(df_invest['Foreign'] != 0) | (df_invest['Institution'] != 0)]
            
            if not valid_df.empty:
                last_row = valid_df.tail(1)
                frn = int(last_row['Foreign'].iloc[0])
                inst = int(last_row['Institution'].iloc[0])
                # 데이터 기준 날짜 (예: 02/04)
                data_date = last_row.index[0].strftime('%m/%d')
                f_icon, i_icon = ("🔵" if frn > 0 else "⚪"), ("🟠" if inst > 0 else "⚪")
            else:
                frn, inst, f_icon, i_icon, data_date = 0, 0, "❓", "❓", "N/A"
        except:
            frn, inst, f_icon, i_icon, data_date = 0, 0, "❓", "❓", "N/A"

        amt_billion = round(row['Amount'] / 100000000) if row['Amount'] else 0
        report += f"<b>{row['StockName']}</b> ({data_date} 수급)\n"
        report += f"{int(row['Close']):,}({row['Rate']}%) | {amt_billion}억\n"
        report += f"{f_icon}외:{frn:,} / {i_icon}기:{inst:,}\n\n"
        time.sleep(0.1)

    report += "--------------------------------\n"
    if len(sector_rank) > 2:
        report += f"🥈 2위: {sector_rank.index[1]} | 🥉 3위: {sector_rank.index[2]}"
    
    send_telegram_msg(report)

except Exception as e:
    send_telegram_msg(f"❌ 클라우드 에러 발생: {e}")
