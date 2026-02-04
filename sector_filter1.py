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
    df_desc = fdr.StockListing('KRX-DESC') # 섹터 정보
    df_all = fdr.StockListing('KRX')      # 현재 시세 정보
    
    # [보정] 컬럼명에서 공백 제거 (가끔 'Name ' 처럼 들어오는 경우 방지)
    df_all.columns = df_all.columns.str.strip()
    df_desc.columns = df_desc.columns.str.strip()

    # 2. 이름(Name) 컬럼 찾기 (유연한 대응)
    name_col = next((c for c in ['Name', 'CodeName', '한글종목명'] if c in df_all.columns), None)
    rate_col = next((c for c in ['ChgRate', 'Ratio', 'Rate', 'CmpRate'] if c in df_all.columns), None)
    
    if not name_col or not rate_col:
        raise Exception(f"필수 컬럼 누락 (Name:{name_col}, Rate:{rate_col})")

    # 3. 데이터 병합
    # 필요한 컬럼만 추출하여 병합 (Code는 공통)
    merged = df_desc.merge(df_all[['Code', name_col, rate_col, 'Amount', 'Close']], on='Code')
    
    # 4. 섹터 분석
    sector_group = merged.groupby('Sector')[rate_col].mean()
    sector_rank = sector_group.sort_values(ascending=False)
    top_sector = sector_rank.index[0]
    
    # 5. 리포트 생성
    top_stocks = merged[merged['Sector'] == top_sector].sort_values(by='Amount', ascending=False).head(10)
    
    report = f"🔥 <b>주도 업종: [{top_sector}]</b>\n"
    report += f"업종 평균 등락: {sector_rank.iloc[0]:.2f}%\n"
    report += "--------------------------------\n"

    start_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    
    for i, row in top_stocks.iterrows():
        # 수급 데이터 (실패해도 리포트 중단 안 함)
        try:
            df_invest = fdr.DataReader(row['Code'], start_date).tail(1)
            frn = int(df_invest['Foreign'].iloc[0]) if 'Foreign' in df_invest.columns else 0
            inst = int(df_invest['Institution'].iloc[0]) if 'Institution' in df_invest.columns else 0
            f_icon, i_icon = ("🔵" if frn > 0 else "⚪"), ("🟠" if inst > 0 else "⚪")
        except:
            frn, inst, f_icon, i_icon = 0, 0, "❓", "❓"

        amt = round(row['Amount'] / 100000000)
        # 컬럼 변수를 사용하여 안전하게 접근
        report += f"<b>{row[name_col]}</b>\n{int(row['Close']):,}({row[rate_col]}%) | {amt}억\n"
        report += f"{f_icon}외:{frn:,} {i_icon}기:{inst:,}\n\n"
        time.sleep(0.1)

    report += "--------------------------------\n"
    if len(sector_rank) > 2:
        report += f"🥈 2위: {sector_rank.index[1]} | 🥉 3위: {sector_rank.index[2]}"
    
    send_telegram_msg(report)
    print("성공적으로 전송되었습니다.")

except Exception as e:
    err_msg = f"❌ 에러 발생: {str(e)}"
    print(err_log := err_msg)
    send_telegram_msg(err_msg)
