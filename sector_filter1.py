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
    df_all = fdr.StockListing('KRX')
    
    df_all.columns = df_all.columns.str.strip()
    df_desc.columns = df_desc.columns.str.strip()

    # 2. 컬럼 자동 매핑 (오타 'ChagesRatio' 대응 포함)
    # 이름 컬럼 후보
    name_col = next((c for c in ['Name', 'CodeName', '한글종목명'] if c in df_all.columns), None)
    
    # 등락률 컬럼 후보 (Ratio라는 단어가 포함된 모든 컬럼 검색)
    rate_col = next((c for c in df_all.columns if 'Ratio' in c or 'Rate' in c), None)
    
    # 거래대금 컬럼 후보 (Amount 또는 Marcap 등)
    amount_col = next((c for c in ['Amount', '거래대금'] if c in df_all.columns), 'Amount')

    if not name_col or not rate_col:
        raise Exception(f"필수 컬럼 누락 (Name:{name_col}, Rate:{rate_col})")

    # 3. 데이터 병합
    merged = df_desc.merge(df_all[['Code', name_col, rate_col, amount_col, 'Close']], on='Code')
    
    # 4. 섹터 분석
    merged[rate_col] = pd.to_numeric(merged[rate_col], errors='coerce')
    sector_group = merged.groupby('Sector')[rate_col].mean()
    sector_rank = sector_group.sort_values(ascending=False)
    
    top_sector = sector_rank.index[0]
    
    # 5. 리포트 생성
    top_stocks = merged[merged['Sector'] == top_sector].sort_values(by=amount_col, ascending=False).head(10)
    
    report = f"🔥 <b>주도 업종: [{top_sector}]</b>\n"
    report += f"업종 평균 등락: {sector_rank.iloc[0]:.2f}%\n"
    report += "--------------------------------\n"

    start_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    
    for i, row in top_stocks.iterrows():
        try:
            df_invest = fdr.DataReader(row['Code'], start_date).tail(1)
            # 수급 컬럼 대응 (일반적인 이름들 검색)
            frn_col = next((c for c in ['Foreign', 'NetPurchaseForeign'] if c in df_invest.columns), None)
            inst_col = next((c for c in ['Institution', 'NetPurchaseInstitution'] if c in df_invest.columns), None)
            
            frn = int(df_invest[frn_col].iloc[0]) if frn_col else 0
            inst = int(df_invest[inst_col].iloc[0]) if inst_col else 0
            f_icon, i_icon = ("🔵" if frn > 0 else "⚪"), ("🟠" if inst > 0 else "⚪")
        except:
            frn, inst, f_icon, i_icon = 0, 0, "❓", "❓"

        amt_billion = round(row[amount_col] / 100000000) if row[amount_col] else 0
        
        report += f"<b>{row[name_col]}</b>\n{int(row['Close']):,}({row[rate_col]}%) | {amt_billion}억\n"
        report += f"{f_icon}외:{frn:,} / {i_icon}기:{inst:,}\n\n"
        time.sleep(0.1)

    report += "--------------------------------\n"
    if len(sector_rank) > 2:
        report += f"🥈 2위: {sector_rank.index[1]} | 🥉 3위: {sector_rank.index[2]}"
    
    send_telegram_msg(report)
    print("성공!")

except Exception as e:
    err_msg = f"❌ 에러 발생: {str(e)}"
    print(err_msg)
    send_telegram_msg(err_msg)
