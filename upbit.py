import json
import requests
import logging
import logging.handlers
from urllib.parse import urlencode

class Upbitpy():
    """
    Upbit API
    https://docs.upbit.com/v1.0/reference
    """

    def __init__(self, accessKey=None, secret=None):
        '''
        Constructor
        accessKey, secret이 없으면 인증가능 요청(EXCHANGE API)은 사용할 수 없음
        :param str accessKey: 발급 받은 acccess key
        :param str secret: 발급 받은 secret
        '''
        self.accessKey = accessKey
        self.secret = secret
        self.markets = self._loadMarkets()
        self.logger = self.makeLogger()

        logging.info('Initializing OK')
    
    def makeLogger(self):
        logging.getLogger("urllib3").setLevel(logging.ERROR)

        #1 logger instance를 만든다.
        logger = logging.getLogger()

        #2 logger의 level을 가장 낮은 수준인 DEBUG로 설정해둔다.
        logger.setLevel(logging.DEBUG)

        #3 formatter 지정
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        
        #4 handler instance 생성
        #console = logging.StreamHandler()
        fileHandler = logging.handlers.TimedRotatingFileHandler(
            filename = 'server',
            when = 'midnight',
            interval = 1
        )
        # 파일명 끝에 붙여줌; ex. log-20190811
        fileHandler.suffix = '%Y%m%d.log'
        #5 handler 별로 다른 level 설정
        #console.setLevel(logging.INFO)
        fileHandler.setLevel(logging.DEBUG)

        #6 handler 출력 format 지정
        #console.setFormatter(formatter)
        fileHandler.setFormatter(formatter)

        #7 logger에 handler 추가
        #logger.addHandler(console)
        logger.addHandler(fileHandler)

        return logger

    ###############################################################
    # QUOTATION API
    ###############################################################

    def getMarketAll(self):
        '''
        마켓 코드 조회
        업비트에서 거래 가능한 마켓 목록
        https://docs.upbit.com/v1.0/reference#%EB%A7%88%EC%BC%93-%EC%BD%94%EB%93%9C-%EC%A1%B0%ED%9A%8C
        :return: json array
        '''
        URL = 'https://api.upbit.com/v1/market/all'
        logging.info('getMarketAll() called')

        return self._get(URL)

    def getMinutesCandles(self, unit, market, to=None, count=None):
        '''
        분(Minute) 캔들
        https://docs.upbit.com/v1.0/reference#%EB%B6%84minute-%EC%BA%94%EB%93%A4-1
        :param int unit: 분 단위. 가능한 값 : 1, 3, 5, 15, 10, 30, 60, 240
        :param str market: 마켓 코드 (ex. KRW-BTC, BTC-BCC)
        :param str to: 마지막 캔들 시각 (exclusive). 포맷 : yyyy-MM-dd'T'HH:mm:ssXXX. 비워서 요청시 가장 최근 캔들
        :param int count: 캔들 개수(최대 200개까지 요청 가능)
        :return: json array
        '''
        URL = 'https://api.upbit.com/v1/candles/minutes/%s' % unit
        logging.info('getMinutesCandles() called')
        logging.debug('unit: %s, market: %s, to: %s, count: %s' % (unit, market, to, count))

        if unit not in [1, 3, 5, 10, 15, 30, 60, 240]:
            logging.error('invalid unit: %s' % unit)
            raise Exception('invalid unit: %s' % unit)
        if market not in self.markets:
            logging.error('invalid market: %s' % market)
            raise Exception('invalid market: %s' % market)

        params = {'market': market}
        if to is not None:
            params['to'] = to
        if count is not None:
            params['count'] = count

        return self._get(URL, params=params)

    def getDaysCandles(self, market, to=None, count=None):
        '''
        일(Day) 캔들
        https://docs.upbit.com/v1.0/reference#%EC%9D%BCday-%EC%BA%94%EB%93%A4-1
        :param str market: 마켓 코드 (ex. KRW-BTC, BTC-BCC)
        :param str to: 마지막 캔들 시각 (exclusive). 포맷 : yyyy-MM-dd'T'HH:mm:ssXXX. 비워서 요청시 가장 최근 캔들
        :param int count: 캔들 개수
        :return: json array
        '''
        URL = 'https://api.upbit.com/v1/candles/days'
        logging.info('getDaysCandles() called')
        logging.debug('market: %s, to: %s, count: %s' % (market, to, count))

        if market not in self.markets:
            logging.error('invalid market: %s' % market)
            raise Exception('invalid market: %s' % market)

        params = {'market': market}
        if to is not None:
            params['to'] = to
        if count is not None:
            params['count'] = count

        return self._get(URL, params=params)

    def getTicker(self, markets):
        '''
        현재가 정보
        요청 당시 종목의 스냅샷을 반환한다.
        https://docs.upbit.com/v1.0/reference#%EC%8B%9C%EC%84%B8-ticker-%EC%A1%B0%ED%9A%8C
        :param str[] markets: 마켓 코드 리스트 (ex. KRW-BTC, BTC-BCC)
        :return: json array
        '''
        URL = 'https://api.upbit.com/v1/ticker'
        logging.info('getTicker() called')
        logging.debug('markets: %s' % markets)

        if not isinstance(markets, list):
            logging.error('invalid parameter: markets should be list')
            raise Exception('invalid parameter: markets should be list')

        if len(markets) == 0:
            logging.error('invalid parameter: no markets')
            raise Exception('invalid parameter: no markets')

        for market in markets:
            if market not in self.markets:
                logging.error('invalid market: %s' % market)
                raise Exception('invalid market: %s' % market)

        logging.info('Make params')
        marketsData = markets[0]
        for market in markets[1:]:
            marketsData += ',%s' % market
        params = {'markets': marketsData}
        logging.debug('params: %s, marketsData: %s' % (params, marketsData))

        return self._get(URL, params=params)


    ###############################################################

    def _get(self, url, headers=None, data=None, params=None):
        resp = requests.get(url, headers=headers, data=data, params=params)
        if resp.status_code not in [200, 201]:
            logging.error('get(%s) failed(%d)' % (url, resp.status_code))
            if resp.text is not None:
                logging.error('resp: %s' % resp.text)
                raise Exception('get(%s) failed(%d) resp.text: %s' % (url, resp.status_code, resp.text))
            raise Exception('get(%s) failed(%d)' % (url, resp.status_code))
        
        return json.loads(resp.text)

    def _loadMarkets(self):
        marketAll = self.getMarketAll()
        if marketAll is None:
            logging.warning('getMarketAll() returns None')
            return None
        markets = []
        for market in marketAll:
            markets.append(market['market'])
            
        return markets