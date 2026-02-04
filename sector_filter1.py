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
    
    # 컬럼명 공백 제거 및 대소문자 통일 (방어 코드)
    df_all.columns = df_all.columns.str.strip()
    df_desc.columns = df_desc.columns.str.strip()

    # 2. 컬럼 자동 매핑 (유연성 극대화)
    # 이름 컬럼 후보
    name_candidates = ['Name', 'CodeName', '한글종목명', '종목명']
    # 등락률 컬럼 후보
    rate_candidates = ['ChgRate', 'Ratio', 'Rate', 'CmpRate', '등락률', '변동률']
    # 거래대금 컬럼 후보
    amount_candidates = ['Amount', 'MarCap', '거래대금', '시가총액']

    name_col = next((c for c in name_candidates if c in df_all.columns), None)
    rate_col = next((c for c in rate_candidates if c in df_all.columns), None)
    amount_col = next((c for c in amount_candidates if c in df_all.columns), None)
    
    # 만약 rate_col을 못 찾았다면 'Ratio'를 기본으로 생성 시도 (데이터에 따라 다름)
    if not rate_col:
        # 마지막 수단으로 숫자로 된 컬럼 중 등락률인 것을 유추하거나 에러 처리
        raise Exception(f"등락률 컬럼을 찾을 수 없습니다. (현재 컬럼: {df_all.columns.tolist()})")

    # 3. 데이터 병합
    merged = df_desc.merge(df_all[['Code', name_col, rate_col, amount_col, 'Close']], on='Code')
    
    # 4. 섹터 분석
    # 데이터 타입 변환 (문자열로 들어오는 경우 대비)
    merged[rate_col] = pd.to_numeric(merged[rate_col], errors='coerce')
    sector_group = merged.groupby('Sector')[rate_col].mean()
    sector_rank = sector_group.sort_values(ascending=False)
    
    if sector_rank.empty:
        raise Exception("섹터 분석 결과가 비어있습니다.")
        
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
            # 수급 데이터 컬럼도 유연하게 처리
            frn = int(df_invest['Foreign'].iloc[0]) if 'Foreign' in df_invest.columns else 0
            inst = int(df_invest['Institution'].iloc[0]) if 'Institution' in df_invest.columns else 0
            f_icon, i_icon = ("🔵" if frn > 0 else "⚪"), ("🟠" if inst > 0 else "⚪")
        except:
            frn, inst, f_icon, i_icon = 0, 0, "❓", "❓"

        # 거래대금 계산 (억 단위)
        amt_val = row[amount_col] if pd.notnull(row[amount_col]) else 0
        amt_billion = round(amt_val / 100000000)
        
        report += f"<b>{row[name_col]}</b>\n{int(row['Close']):,}({row[rate_col]}%) | {amt_billion}억\n"
        report += f"{f_icon}외:{frn:,} {i_icon}기:{inst:,}\n\n"
        time.sleep(0.1)

    report += "--------------------------------\n"
    if len(sector_rank) > 2:
        report += f"🥈 2위: {sector_rank.index[1]} | 🥉 3위: {sector_rank.index[2]}"
    
    send_telegram_msg(report)
    print("성공적으로 리포트를 전송했습니다.")

except Exception as e:
    err_msg = f"❌ 에러 발생: {str(e)}"
    print(err_msg)
    send_telegram_msg(err_msg)
