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
    # 1. 날짜 설정 (최근 15일치 데이터를 넉넉히 가져옴)
    now_kst = datetime.utcnow() + timedelta(hours=9)
    start_date = (now_kst - timedelta(days=15)).strftime('%Y-%m-%d')

    # 2. 기초 데이터 로드
    df_desc = fdr.StockListing('KRX-DESC')
    df_all = fdr.StockListing('KRX')
    df_all.columns = df_all.columns.str.strip()
    
    # 컬럼 매핑
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
        frn, inst, data_date = 0, 0, "N/A"
        f_icon, i_icon = "❓", "❓"
        
        try:
            # 해당 종목의 히스토리 데이터 로드
            df_hist = fdr.DataReader(row['Code'], start_date)
            
            if not df_hist.empty:
                # [핵심] 뒤에서부터 검사하며 외인/기관 합계가 0이 아닌 첫 번째 행을 찾음
                # 보통 index[-1]은 오늘(데이터 없음), index[-2]가 어제 데이터임
                for j in range(len(df_hist)-1, -1, -1):
                    temp_row = df_hist.iloc[j]
                    # Foreign이나 Institution 중 하나라도 0이 아닌 값을 찾으면 확정
                    if temp_row['Foreign'] != 0 or temp_row['Institution'] != 0:
                        frn = int(temp_row['Foreign'])
                        inst = int(temp_row['Institution'])
                        data_date = df_hist.index[j].strftime('%m/%d')
                        f_icon = "🔵" if frn > 0 else "⚪"
                        i_icon = "🟠" if inst > 0 else "⚪"
                        break
        except:
            pass

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
    send_telegram_msg(f"❌ 최종 에러: {str(e)}")
