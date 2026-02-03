import FinanceDataReader as fdr
import requests
import time
import os

def send_telegram_msg(message):
    # 환경변수에서 값을 가져오도록 변경
    token = os.environ.get('TELEGRAM_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    requests.post(url, data=data)

def check_golden_cross(code):
    try:
        df = fdr.DataReader(code).tail(80)
        if len(df) < 60: return False
        
        ma20 = df['Close'].rolling(window=20).mean()
        ma60 = df['Close'].rolling(window=60).mean()
        
        prev_ma20, curr_ma20 = ma20.iloc[-2], ma20.iloc[-1]
        prev_ma60, curr_ma60 = ma60.iloc[-2], ma60.iloc[-1]
        
        # 골든크로스 조건
        return prev_ma20 < prev_ma60 and curr_ma20 >= curr_ma60
    except:
        return False

# 1. 데이터 준비
df_krx_desc = fdr.StockListing('KRX-DESC')
target_sector = '반도체'
sector_stocks = df_krx_desc[df_krx_desc['Sector'].str.contains(target_sector, na=False)].copy()

df_current = fdr.StockListing('KRX')
ratio_col = [col for col in df_current.columns if 'Ratio' in col][0]
merged_df = sector_stocks.merge(df_current[['Code', 'Close', ratio_col, 'Amount']], on='Code')

# 거래대금 상위 20개 추출
top_20 = merged_df.sort_values(by='Amount', ascending=False).head(20)

# 2. 리포트 생성
report = f"📊 <b>오늘의 {target_sector} TOP 20 현황</b>\n"
report += "--------------------------------\n"

golden_list = [] # 나중에 요약을 위해 따로 저장

for i, row in top_20.iterrows():
    is_golden = check_golden_cross(row['Code'])
    close_price = int(row['Close'])
    amount_billion = round(row['Amount'] / 100000000)
    
    # 골든크로스 발생 여부에 따라 마킹 추가
    mark = "🔥 <b>골든크로스!</b>" if is_golden else ""
    if is_golden: golden_list.append(row['Name'])
    
    report += f"<b>{row['Name']}</b> {mark}\n"
    report += f"종가: {close_price:,}원 ({row[ratio_col]}%)\n"
    report += f"거래대금: {amount_billion:,}억\n\n"
    
    time.sleep(0.05) # 속도 조절

# 3. 하단 요약 추가
report += "--------------------------------\n"
if golden_list:
    report += f"✅ <b>오늘의 신호 종목:</b> {', '.join(golden_list)}\n"
else:
    report += "✅ 오늘 신호가 포착된 종목은 없습니다.\n"
report += "--------------------------------"

# 4. 전송
send_telegram_msg(report)
print("상위 20개 종목 리포트 발송 완료!")