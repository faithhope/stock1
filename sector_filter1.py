import FinanceDataReader as fdr
import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta

def send_telegram_msg(message):
    # 깃허브 Secrets에서 값을 가져옵니다.
    token = os.environ.get('TELEGRAM_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')
    if not token or not chat_id: return
    
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    requests.post(url, data=data)

try:
    # 1. 데이터 로드 및 전처리
    df_desc = fdr.StockListing('KRX-DESC')
    df_all = fdr.StockListing('KRX')
    df_all.columns = df_all.columns.str.strip()
    df_desc.columns = df_desc.columns.str.strip()

    # 2. 컬럼 표준화 (로컬 테스트에서 검증된 방식)
    rate_col = next((c for c in df_all.columns if 'Ratio' in c or 'Rate' in c), None)
    amount_col = next((c for c in ['Amount', '거래대금'] if c in df_all.columns), 'Amount')
    
    df_all_sub = df_all[['Code', 'Name', rate_col, amount_col, 'Close']].copy()
    df_all_sub.columns = ['Code', 'StockName', 'Rate', 'Amount', 'Close']

    # 3. 데이터 병합 및 분석
    merged = df_desc.merge(df_all_sub, on='Code')
    merged['Rate'] = pd.to_numeric(merged['Rate'], errors='coerce')
    sector_group = merged.groupby('Sector')['Rate'].mean()
    sector_rank = sector_group.sort_values(ascending=False)
    top_sector = sector_rank.index[0]

    # 4. 리포트 생성
    top_stocks = merged[merged['Sector'] == top_sector].sort_values(by='Amount', ascending=False).head(10)
    
    report = f"🚀 <b>클라우드 리포트: [{top_sector}]</b>\n"
    report += f"평균 등락: {sector_rank.iloc[0]:.2f}%\n"
    report += "--------------------------------\n"

    start_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    
    for i, row in top_stocks.iterrows():
        try:
            # 1. 최근 10일치 데이터를 넉넉히 가져옵니다.
            df_invest = fdr.DataReader(row['Code'], start_date)
            
            # 2. 'Foreign'이나 'Institution' 컬럼이 0이 아닌 마지막 날을 찾습니다.
            # 장 중에는 오늘 데이터가 0일 수 있으므로, 실제 값이 있는 마지막 영업일 데이터를 추출합니다.
            valid_invest = df_invest[df_invest['Foreign'] != 0].tail(1)
            
            # 만약 오늘 데이터가 0이라면 바로 전일 데이터를 사용하게 됩니다.
            if not valid_invest.empty:
                frn = int(valid_invest['Foreign'].iloc[0])
                inst = int(valid_invest['Institution'].iloc[0])
                # 수급이 집계된 날짜 (예: 2024-05-20)
                invest_date = valid_invest.index[0].strftime('%m/%d')
            else:
                frn, inst, invest_date = 0, 0, "미집계"

            f_icon, i_icon = ("🔵" if frn > 0 else "⚪"), ("🟠" if inst > 0 else "⚪")
        except:
            frn, inst, invest_date, f_icon, i_icon = 0, 0, "N/A", "❓", "❓"

        amt_billion = round(row['Amount'] / 100000000) if row['Amount'] else 0
        
        report += f"<b>{row['StockName']}</b> ({invest_date} 수급)\n"
        report += f"{int(row['Close']):,}({row['Rate']}%) | {amt_billion}억\n"
        report += f"{f_icon}외:{frn:,} / {i_icon}기:{inst:,}\n\n"
        time.sleep(0.1)

    report += "--------------------------------\n"
    if len(sector_rank) > 2:
        report += f"🥈 2위: {sector_rank.index[1]} | 🥉 3위: {sector_rank.index[2]}"
    
    send_telegram_msg(report)

except Exception as e:
    send_telegram_msg(f"❌ 클라우드 에러 발생: {e}")

