from upbit import Upbitpy
import pandas as pd
from datetime import datetime, timedelta
import time
import random
from pymongo import MongoClient
import uuid

class Screening:
    def __init__(self):
        self.upbit = Upbitpy()
        self.volList = []
        self.pumpDict = {}
        self.gcList = []
        self.dcList = []

    def dbConnection(self):
        self.conn = MongoClient('127.0.0.1', 27017)
        self.db = self.conn.coins

    def getTargetCoin(self, a1=1, a2=365):
        today = datetime.today()
        targetCoin = self.coinInfo[self.coinInfo['52_week_ratio'] > a1]   ## 고점대비 현재 위치(%)
        targetCoin = targetCoin[targetCoin['highest_52_week_date'] > today - timedelta(days=a2)]  ## 고점 대비 조정 기간(days)
        
        return targetCoin

    def getBasicInfo(self):
        coinList = pd.DataFrame(data=self.upbit.getMarketAll())
        time.sleep(random.randint(1, 2))
        coinList = coinList[coinList['market'].str.contains('KRW-')]

        coinInfo = pd.DataFrame(data=self.upbit.getTicker(list(coinList['market'].values)))
        time.sleep(random.randint(1, 2))
        coinInfo['52_week_ratio'] = coinInfo['trade_price'] / coinInfo['highest_52_week_price'] * 100
        coinInfo['highest_52_week_date'] = [datetime.strptime(x, '%Y-%m-%d') for x in
                                            coinInfo['highest_52_week_date']]

        return coinList, coinInfo ### list는 이름 등 기본 정보 info는 수치 데이터도 있음

    def insertPumpingSignal(self, code, type):
        now = datetime.now()
        engNm = self.coinList[self.coinList['market'] == code]['english_name'].values[0]
        korNm = self.coinList[self.coinList['market'] == code]['korean_name'].values[0]
        rtn2 = self.coinInfo[self.coinInfo['market'] == code]['signed_change_rate'].values[0]

        signalDict = {}
        signalDict['pumpingCode'] = uuid.uuid4()
        signalDict['coinCode'] = code
        signalDict['englishName'] = engNm
        signalDict['koreaName'] = korNm
        signalDict['tradePrice'] = self.coinInfo[self.coinInfo['market'] == code]['trade_price'].values[0]
        signalDict['oneDayRate'] = rtn2
        signalDict['type'] = type
        signalDict['createdTime'] = now

        self.dbConnection()
        col = self.db.pumping
        col.insert_one(signalDict)

    def insertCrossSignal(self, code, type):
        now = datetime.now()
        korNm = self.coinList[self.coinList['market'] == code]['korean_name'].values[0]
        engNm = self.coinList[self.coinList['market'] == code]['english_name'].values[0]
        rtn2 = self.coinInfo[self.coinInfo['market'] == code]['signed_change_rate'].values[0]

        signalDict = {}
        signalDict['crossCode'] = uuid.uuid4()
        signalDict['coinCode'] = code
        signalDict['englishName'] = engNm
        signalDict['koreaName'] = korNm
        signalDict['tradePrice'] = self.coinInfo[self.coinInfo['market'] == code]['trade_price'].values[0]
        signalDict['oneDayRate'] = rtn2
        signalDict['type'] = type
        signalDict['createdTime'] = now

        self.dbConnection()
        col = self.db.cross
        col.insert_one(signalDict)

    def insertVolSignal(self, code, type):
        now = datetime.now()
        engNm = self.coinList[self.coinList['market'] == code]['english_name'].values[0]
        korNm = self.coinList[self.coinList['market'] == code]['korean_name'].values[0]
        rtn2 = self.coinInfo[self.coinInfo['market'] == code]['signed_change_rate'].values[0]

        signalDict = {}
        signalDict['volCode'] = uuid.uuid4()
        signalDict['coinCode'] = code
        signalDict['englishName'] = engNm
        signalDict['koreaName'] = korNm
        signalDict['tradePrice'] = self.coinInfo[self.coinInfo['market'] == code]['trade_price'].values[0]
        signalDict['oneDayRate'] = rtn2
        signalDict['type'] = type
        signalDict['createdTime'] = now

        self.dbConnection()
        col = self.db.vol
        col.insert_one(signalDict)

    def findPumpingSignal(self, mkt):        
        ## 펌핑 코인 조건 설정 ##
        temp = pd.DataFrame(data=self.upbit.getMinutesCandles(unit=30, market=mkt, count=5))  ## 30분봉, 5개를 살펴봄

        if temp.iloc[0]['candle_acc_trade_volume'] > 1.5 * temp.iloc[1:]['candle_acc_trade_volume'].mean():     ## 거래량 조건 - 최근 2시간 평균 거래량 1.5배 돌파
            if temp.iloc[0]['trade_price'] > temp.iloc[1:]['high_price'].max():     ## 가격 조건 - 최근 30분 고점 돌파
                if temp.iloc[0]['trade_price'] > temp.iloc[0]['high_price']*0.9:
                    code = temp['market'].iloc[0]

                    if code not in self.pumpDict:
                        self.pumpDict[code] = temp.iloc[0]['trade_price']
                        self.insertPumpingSignal(code, "new pumping")
                    else:
                        if self.pumpDict[code] >= temp.iloc[0]['trade_price']:
                            self.pumpDict[code] = temp.iloc[0]['trade_price']
                            return
                        else:
                            self.pumpDict[code] = temp.iloc[0]['trade_price']
                            self.insertPumpingSignal(code, "pumping")

    def findCrossSignal(self, mkt):
        ### 이평선 골든크로스 코인 스크리닝 ###
        temp = pd.DataFrame(data=self.upbit.getMinutesCandles(unit=240, market=mkt, count=21))    ## 4시간봉, 20
        temp = temp.set_index("candle_date_time_utc").sort_index(ascending=True)
        price = temp['trade_price']
        ma5 = temp['trade_price'].rolling(5).mean()
        ma20 = temp['trade_price'].rolling(20).mean()

        if price.iloc[-1] > ma5.iloc[-1]:
            if (ma5.iloc[-1] > ma20.iloc[-1]) and (ma5.iloc[-2] < ma20.iloc[-2]):
                code = temp['market'].iloc[0]

                if code in self.dcList:
                    self.gcList.remove(code)
                if code in self.gcList:
                    return

                self.gcList.append(code)

                self.insertCrossSignal(code, "goldencross")

        if price.iloc[-1] < ma5.iloc[-1]:
            if (ma5.iloc[-1] < ma20.iloc[-1]) and (ma5.iloc[-2] > ma20.iloc[-2]):
                code = temp['market'].iloc[0]

                if code in self.gcList:
                    self.dcList.remove(code)
                if code in self.dcList:
                    return

                self.dcList.append(code)

                self.insertCrossSignal(code, "deadcross")

    def findVolSignal(self, mkt):
        ### 변동성 돌파 전략 ###
        temp = pd.DataFrame(data=self.upbit.getDaysCandles(market=mkt, count=2))  ## 일봉
        temp = temp.set_index("candle_date_time_utc").sort_index(ascending=True)
        temp['vol'] = temp['high_price'] - temp['low_price']
        target = temp['vol'].iloc[-2].max()

        if (temp['trade_price'].iloc[-1] - temp['opening_price'].iloc[-1]) > target * 0.5:
            code = temp['market'].iloc[0]

            for vol in self.volList:
                if(vol == code):
                    return

            self.volList.append(code)

            self.insertVolSignal(code, "vol")

    def findSignal(self):
        try:
            self.coinList, self.coinInfo = self.getBasicInfo()
            self.targetCoin = self.getTargetCoin()

            for mkt in self.targetCoin['market']:
                self.findPumpingSignal(mkt)#펌핑 시그널 찾기
                time.sleep(random.randint(1, 2))

                self.findCrossSignal(mkt)#데드,골든크로스 시그널 찾기
                time.sleep(random.randint(1, 2))

                self.findVolSignal(mkt)#변동성 돌파 시그널 찾기
                time.sleep(random.randint(1, 2))
        except Exception as e:
            pass

if __name__ == "__main__":
    scr = Screening()
    dirt1 = 0
    dirt2 = 0

    while True:
        now = datetime.now()
        
        if now.hour == 8 and now.minute == 59 and 50 <= now.second <= 59:
            if dirt2 == 0:#8시 59분에 list, dict 초기화 작업
                scr.volList = []
                scr.pumpDict = {}
                scr.gcList = []
                scr.dcList = []
                dirt2 = 1
        else:
            dirt2 = 0

        if not(now.hour == 8 and 55 < now.minute < 59) and 0 <= now.second <= 9:# 08:55 ~ 08:59을 제외한 매분 코인 스크리닝 실행
            if dirt1 == 0:
                scr.findSignal()
                dirt1 = 1
        else:
            dirt1 = 0