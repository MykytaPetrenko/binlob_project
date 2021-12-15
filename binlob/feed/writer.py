from binance import Client, ThreadedWebsocketManager
from binance.streams import ReconnectingWebsocket
from threading import Thread, Lock
import time
from datetime import datetime
import os
import pickle as pickle

ReconnectingWebsocket.MAX_QUEUE_SIZE = 10000


class FeedWriter:
    def __init__(self, **kwargs):
        self._lock = Lock()
        self._client = Client(api_key='', api_secret='')
        self._twm = ThreadedWebsocketManager(api_key='', api_secret='')

        # output params
        self._temp_dir = kwargs.get('temp_dir', 'temp')
        self._output_dir = kwargs.get('output_dir', 'output')
        self._check_dirs()

        # Snapshots params
        self._limit = kwargs.get('snapshot_limit', 100)
        self._do_snapshots = kwargs.get('book_snapshots', True)
        self._snapshot_period = kwargs.get('', 60)

        # Stream params
        depth_streams = kwargs.get('depth_streams', None)
        trade_streams = kwargs.get('trade_streams', None)
        self._futures_streams = set()
        self._futures_tickers = set()
        self._spot_streams = set()
        self._spot_tickers = set()
        # Manage depth streams
        if depth_streams:
            for stream in depth_streams.get('spot', list()):
                ticker = stream.split('@')[0]
                self._spot_streams.add(stream)
                self._spot_tickers.add(ticker.upper())
            for stream in depth_streams.get('futures', list()):
                ticker = stream.split('@')[0]
                self._futures_streams.add(stream)
                self._futures_tickers.add(ticker.upper())
        # Manage trade streams
        if trade_streams:
            for stream in trade_streams.get('spot', list()):
                self._spot_streams.add(stream)
            for stream in trade_streams.get('futures', list()):
                self._futures_streams.add(stream)

        self._num_streams = len(self._spot_tickers) + len(self._futures_tickers)
        self._request_period = None
        if self._num_streams != 0:
            self._request_period = self._snapshot_period / self._num_streams

        self._snapshot_thread = None
        self._info_thread = None
        self._output_streams = dict()
        self._stats = dict()

    def _check_dirs(self):
        if not os.path.exists(self._temp_dir):
            os.makedirs(self._temp_dir, exist_ok=True)
        if not os.path.exists(self._output_dir):
            os.makedirs(self._output_dir, exist_ok=True)

    def start(self):
        self._start_socket()
        self._snapshot_thread = Thread(target=self._snapshot_loop)
        self._snapshot_thread.start()
        self._info_thread = Thread(target=self._info_loop)
        self._info_thread.start()

    def _get_writer(self, symbol: str, section: str, data_type: str):
        stream_name = f'{symbol.lower()}_{section.lower()}_{data_type}'

        with self._lock:
            stream, creation_day = self._output_streams.get(stream_name, (None, None))
        today = datetime.utcnow()
        if stream is not None and today.day != creation_day:
            stream.close()

        if today.day != creation_day:
            date_str = today.strftime('%Y-%m-%d')
            filename = f'{symbol}-{date_str}-{section}-{data_type}.pkl'
            stream = open(os.path.join(self._temp_dir, filename), 'wb')
            with self._lock:
                self._output_streams[stream_name] = (stream, today.day)
        return stream

    def _write_orderbook(self, symbol: str, section: str = 'SPOT'):
        stream = self._get_writer(symbol, section, 'book')
        if section == 'SPOT':
            orderbook = self._client.get_order_book(symbol=symbol, limit=self._limit)
        elif section == 'FUTURES':
            orderbook = self._client.futures_order_book(symbol=symbol, limit=self._limit)
        else:
            raise Exception(f'Incorrect section. Got {section}. Use "SPOT" or "FUTURES"')

        pickle.dump(orderbook, stream, protocol=pickle.HIGHEST_PROTOCOL)
        self._increment_stat(symbol, section, 'book')
        stream.flush()

    def _snapshot_loop(self):
        first_run = True
        while True:
            for ticker in self._spot_tickers:
                self._write_orderbook(ticker, 'SPOT')
                if first_run:
                    pass
                else:
                    time.sleep(self._request_period)
            for ticker in self._futures_tickers:
                self._write_orderbook(ticker, 'FUTURES')
                if first_run:
                    pass
                else:
                    time.sleep(self._request_period)
            first_run = False

    def _info_loop(self):
        while True:
            with self._lock:
                for symbol, symbol_stats in sorted(self._stats.items()):
                    print('\t' + symbol)
                    for section, section_stats in symbol_stats.items():
                        line = f'{section:9s}:'
                        for datatype, stream_stats in section_stats.items():
                            line += f'\t{datatype} - {stream_stats}'
                        print(line)
                print('-' * 60)
                self._stats.clear()
            time.sleep(1)

    def _increment_stat(self, symbol, section, datatype):
        with self._lock:
            if symbol in self._stats:
                symbol_stats = self._stats[symbol]
            else:
                symbol_stats = dict(
                    SPOT=dict(depth=0, trade=0, book=0),
                    FUTURES=dict(depth=0, trade=0, book=0)
                )
                self._stats[symbol] = symbol_stats

            if section in symbol_stats:
                section_stats = symbol_stats[section]
            else:
                section_stats = dict(depth=0, trade=0, book=0)
                symbol_stats[section] = section_stats

            if datatype in section_stats:
                section_stats[datatype] += 1
            else:
                section_stats[datatype] = 1

    def _write_message(self, msg: dict, section: str):
        if 'data' in msg:
            data = msg.get('data')
            datatype = 'depth' if data['e'] == 'depthUpdate' else 'trade'
            symbol = data['s']
            stream = self._get_writer(symbol, section, datatype)
            pickle.dump(data, stream, protocol=pickle.HIGHEST_PROTOCOL)
            self._increment_stat(symbol, section, datatype)
        else:
            return

    def _route_spot(self, msg):
        self._write_message(msg, 'SPOT')

    def _route_futures(self, msg):
        self._write_message(msg, 'FUTURES')

    def _start_socket(self):
        self._twm.daemon = True
        self._twm.start()
        if self._spot_streams:
            self._spot_conn = self._twm.start_multiplex_socket(
                callback=self._route_spot,
                streams=list(self._spot_streams)
            )
        if self._futures_streams:
            self._futures_conn = self._twm.start_futures_multiplex_socket(
                callback=self._route_futures,
                streams=list(self._futures_streams)
            )
        # self._twm.join()
