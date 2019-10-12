import requests
from .entity import (
    Aggs,
    Trade, Trades,
    Quote, Quotes,
    Exchange, SymbolTypeMap, ConditionMap,
    Company, Dividends, Splits, Earnings, Financials, NewsList, Ticker, Analysts
)
from requests.adapters import HTTPAdapter
import urllib.parse
from .file_cacher import FileCacher
from datetime import datetime


def _is_list_like(o):
    return isinstance(o, (list, set, tuple))


class REST(FileCacher):

    def clean_filename(self, str):
        ret = urllib.parse.quote(str).replace('/', '_')
        return "".join([c for c in ret if c.isalpha() or c.isdigit() or c == ' ']).rstrip()

    def __init__(self, api_key, staging=False, timeout=5, max_retries=5, cache_files=True):
        super().__init__(cache_files)
        self._api_key = api_key
        self._staging = staging
        self._session = requests.Session()
        self._base_url = 'https://api.polygon.io/'
        self._timeout = timeout
        self._session.mount(self._base_url, HTTPAdapter(max_retries=max_retries))

    def _request(self, method, path, params=None, version='v1'):
        cache_key = self.clean_filename('{}-{}-{}-{}'.format(method, path, params, version))
        ret = self.read_cache(cache_key)
        if ret:
            return ret
        else:
            url = self._base_url + version + path
            params = params or {}
            params['apiKey'] = self._api_key
            if self._staging:
                params['staging'] = 'true'
            resp = self._session.request(method, url, params=params, timeout=self._timeout)
            resp.raise_for_status()
            ret = resp.json()
            self.write_cache(cache_key, ret)
            return ret

    def get(self, path, params=None, version='v1'):
        return self._request('GET', path, params=params, version=version)

    def exchanges(self):
        path = '/meta/exchanges'
        return [Exchange(o) for o in self.get(path)]

    def symbol_type_map(self):
        path = '/meta/symbol-types'
        return SymbolTypeMap(self.get(path))

    def historic_trades(self, symbol, date, offset=None, limit=None):
        path = '/historic/trades/{}/{}'.format(symbol, date)
        params = {}
        if offset is not None:
            params['offset'] = offset
        if limit is not None:
            params['limit'] = limit
        raw = self.get(path, params)

        return Trades(raw)

    def historic_quotes(self, symbol, date, offset=None, limit=None):
        path = '/historic/quotes/{}/{}'.format(symbol, date)
        params = {}
        if offset is not None:
            params['offset'] = offset
        if limit is not None:
            params['limit'] = limit
        raw = self.get(path, params)

        return Quotes(raw)

    def historic_agg_v2(self,  symbol,
                         _from=None, to=None, limit=None,timeframe='day'):
        path = '/aggs/ticker/{}/range/1/{}/{}/{}'.format(symbol,timeframe,_from,to)

        params = {}
        # if _from is not None:
        #     params['from'] = _from
        # if to is not None:
        #     params['to'] = to
        # if limit is not None:
        #     params['limit'] = limit
        raw = self.get(path, params,version='v2')
        map = {'o': 'open', 'c': 'close', 'h': 'high', 'l': 'low', 'v': 'volume', 't': 'timestamp', 'd': 'day', 'n' : 'numberOfWindows'}
        new_raw = self._v2_to_v1_format(raw,map)

        aggType = 'daily'
        if timeframe == 'day':
            aggType = 'daily'
        elif timeframe == 'minute':
            aggType = 'min'

        new_raw['aggType'] = aggType
        return Aggs(new_raw)

    def _v2_to_v1_format(self, raw,map):
        raw['map'] = map
        raw['ticks'] = raw['results']
        for tick in raw['ticks']:
            start = datetime.fromtimestamp(tick['t'] / 1000)
            tick['d'] =  start.strftime('%Y-%m-%d')

        return raw

    def historic_agg(self, size, symbol,
                     _from=None, to=None, limit=None):
        path = '/historic/agg/{}/{}'.format(size, symbol)
        params = {}
        if _from is not None:
            params['from'] = _from
        if to is not None:
            params['to'] = to
        if limit is not None:
            params['limit'] = limit
        raw = self.get(path, params)

        return Aggs(raw)

    def last_trade(self, symbol):
        path = '/last/stocks/{}'.format(symbol)
        raw = self.get(path)
        return Trade(raw['last'])

    def last_quote(self, symbol):
        path = '/last_quote/stocks/{}'.format(symbol)
        raw = self.get(path)
        # TODO status check
        return Quote(raw['last'])

    def condition_map(self, ticktype='trades'):
        path = '/meta/conditions/{}'.format(ticktype)
        return ConditionMap(self.get(path))

    def company(self, symbol):
        return self._get_symbol(symbol, 'company', Company)

    def _get_symbol(self, symbol, resource, entity):
        multi = _is_list_like(symbol)
        symbols = symbol if multi else [symbol]
        if len(symbols) > 50:
            raise ValueError('too many symbols: {}'.format(len(symbols)))
        params = {
            'symbols': ','.join(symbols),
        }
        path = '/meta/symbols/{}'.format(resource)
        res = self.get(path, params=params)
        if isinstance(res, list):
            res = {o['symbol']: o for o in res}
        retmap = {sym: entity(res[sym]) for sym in symbols if sym in res}
        if not multi:
            return retmap.get(symbol)
        return retmap

    def dividends(self, symbol):
        return self._get_symbol(symbol, 'dividends', Dividends)

    def splits(self, symbol):
        path = '/meta/symbols/{}/splits'.format(symbol)
        return Splits(self.get(path))

    def earnings(self, symbol):
        return self._get_symbol(symbol, 'earnings', Earnings)

    def financials(self, symbol):
        return self._get_symbol(symbol, 'financials', Financials)

    def analysts(self, symbol):
        path = '/meta/symbols/{}/analysts'.format(symbol)
        return Analysts(self.get(path))

    def news(self, symbol):
        path = '/meta/symbols/{}/news'.format(symbol)
        return NewsList(self.get(path))

    def all_tickers(self, us_only=True):
        path = '/reference/tickers'
        params = {'active': 'true'}
        all_tickers = []
        if us_only:
            params['market'] = 'stocks'
            params['locale'] = 'us'
            params['page'] = 1

        json = self.get(path, version='v2', params=params)
        while json:
            if json and json['page'] and json['perPage']:
                all_tickers += json['tickers']
                count = json['count']
                if json['page'] * json['perPage'] < count:
                    params['page'] += 1
                    json = self.get(path, version='v2', params=params)
                else:
                    break

        return [
            Ticker(ticker) for ticker in
            all_tickers
        ]

    def snapshot(self, symbol):
        path = '/snapshot/locale/us/markets/stocks/tickers/{}'.format(symbol)
        return Ticker(self.get(path, version='v2'))
