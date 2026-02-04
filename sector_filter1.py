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
    # 1. 기초 데이터 로드
    df_desc = fdr.StockListing('KRX-DESC')
    df_all = fdr.StockListing('KRX') # 전 종목 시세 및 기본 수급 포함
    df_all.columns = df_all.columns.str.strip()

    # 2. 컬럼 매핑 (클라우드 환경 대응)
    rate_col = next((c for c in df_all.columns if 'Ratio' in c or 'Rate' in c), None)
    amount_col = next((c for c in ['Amount', '거래대금'] if c in df_all.columns), 'Amount')
    
    # 3. 데이터 병합 및 섹터 분석
    df_all_sub = df_all[['Code', 'Name', rate_col, amount_col, 'Close']].copy()
    df_all_sub.columns = ['Code', 'StockName', 'Rate', 'Amount', 'Close']
    merged = df_desc.merge(df_all_sub, on='Code')
    merged['Rate'] = pd.to_numeric(merged['Rate'], errors='coerce')
    
    sector_rank = merged.groupby('Sector')['Rate'].mean().sort_values(ascending=False)
    top_sector = sector_rank.index[0]

    # 4. 리포트 생성 시작
    top_stocks = merged[merged['Sector'] == top_sector].sort_values(by='Amount', ascending=False).head(10)
    
    report = f"🚀 <b>클라우드 리포트: [{top_sector}]</b>\n"
    report += f"평균 등락: {sector_rank.iloc[0]:.2f}%\n"
    report += "--------------------------------\n"

    # 5. 수급 데이터 보정 루프
    # DataReader의 불안정성을 피하기 위해 어제(T-1) 데이터를 명시적으로 요청
    target_date = (datetime.now() - timedelta(days=1))
    if target_date.weekday() >= 5: # 주말이면 금요일로 후퇴
        target_date -= timedelta(days=target_date.weekday() - 4)
    date_str = target_date.strftime('%Y-%m-%d')

    for i, row in top_stocks.iterrows():
        try:
            # 개별 종목 수급 상세 (실패 시 N/A 방지 로직)
            # data_source를 'KRX'로 명시하여 안정성 확보
            df_invest = fdr.DataReader(row['Code'], date_str, date_str)
            
            if not df_invest.empty:
                # 데이터가 존재하면 마지막 행 사용
                last = df_invest.iloc[-1]
                frn = int(last.get('Foreign', 0))
                inst = int(last.get('Institution', 0))
                data_date = df_invest.index[-1].strftime('%m/%d')
            else:
                # 데이터가 비어있으면 0으로 처리 (N/A 방지)
                frn, inst, data_date = 0, 0, date_str[5:].replace('-', '/')
            
            f_icon = "🔵" if frn > 0 else "⚪"
            i_icon = "🟠" if inst > 0 else "⚪"
        except:
            frn, inst, f_icon, i_icon, data_date = 0, 0, "❓", "❓", "ERR"

        amt_billion = round(row['Amount'] / 100000000) if row['Amount'] else 0
        report += f"<b>{row['StockName']}</b> ({data_date} 수급)\n"
        report += f"{int(row['Close']):,}({row['Rate']}%) | {amt_billion}억\n"
        report += f"{f_icon}외:{frn:,} / {i_icon}기:{inst:,}\n\n"
        time.sleep(0.2) # API 과부하 방지 (중요)

    report += "--------------------------------\n"
    if len(sector_rank) > 2:
        report += f"🥈 2위: {sector_rank.index[1]} | 🥉 3위: {sector_rank.index[2]}"
    
    send_telegram_msg(report)

except Exception as e:
    send_telegram_msg(f"❌ 최종 에러: {str(e)}")
