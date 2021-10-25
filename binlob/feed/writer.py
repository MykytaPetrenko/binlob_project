from binance import Client, ThreadedWebsocketManager
from threading import Thread, Lock
import time
from datetime import datetime
import os
import pickle as pickle


class FeedWriter:
    def __init__(self, config: dict):
        self._lock = Lock()
        self._client = Client(api_key='', api_secret='')
        self._twm = ThreadedWebsocketManager(api_key='', api_secret='')
        self._temp_dir = 'C:/temp'
        self._output_dir = 'C:/output'
        self._limit = config.get('limit', 100)
        self._request_period = config.get('request_period', 60)
        self._symbols = config.get('symbols')
        self._num_symbols = len(self._symbols)
        self._snapshot_thread = None
        self._output_streams = dict()
        self._depth_messages_received = 0
        self._trade_messages_received = 0
        if not os.path.exists(self._temp_dir):
            os.makedirs(self._temp_dir, exist_ok=True)
        if not os.path.exists(self._output_dir):
            os.makedirs(self._output_dir, exist_ok=True)

    def start(self):
        self._start_socket()
        self._start_snapshot_loop()
        self._start_info_loop()

    def _get_file_stream(self, symbol: str, section: str, data_type: str):
        stream_name = f'{symbol.lower()}_{section.lower()}_{data_type}'

        with self._lock:
            stream, create_day = self._output_streams.get(stream_name, (None, None))

        today = datetime.utcnow()
        if stream and today.day != create_day:
            stream.close()

        if today.day != create_day:
            date_str = today.strftime('%Y-%m-%d')
            filename = f'{symbol}-{date_str}-{section}-{data_type}.pkl'
            stream = open(os.path.join(self._temp_dir, filename), 'wb')
            with self._lock:
                self._output_streams[stream_name] = (stream, today.day)
        return stream

    def _write_orderbook(self, symbol: str, section: str = 'SPOT'):
        stream = self._get_file_stream(symbol, section, 'orderbook')

        if section == 'SPOT':
            orderbook = self._client.get_order_book(symbol=symbol, limit=self._limit)
        elif section == 'FUTURES':
            orderbook = self._client.futures_order_book(symbol=symbol, limit=self._limit)

        pickle.dump(orderbook, stream, protocol=pickle.HIGHEST_PROTOCOL)
        stream.flush()

    def _snapshot_loop(self):
        while True:
            for symbol in self._symbols:
                if symbol['spot']:
                    self._write_orderbook(symbol['symbol'], 'SPOT')
                    time.sleep(self._request_period)
                if symbol['futures']:
                    self._write_orderbook(symbol['symbol'], 'FUTURES')
                    time.sleep(self._request_period)

    def _start_info_loop(self):
        while True:
            with self._lock:
                print(f'Depth messages: {self._depth_messages_received}\t'
                      f'Trade messages: {self._trade_messages_received}')
                self._depth_messages_received = 0
                self._trade_messages_received = 0
            time.sleep(1)

    def _receive_spot(self, msg):
        data = msg['data']
        datatype = 'depth' if data['e'] == 'depthUpdate' else 'trade'
        symbol = data['s']
        stream = self._get_file_stream(symbol, 'SPOT', datatype)
        pickle.dump(data, stream, protocol=pickle.HIGHEST_PROTOCOL)
        with self._lock:
            if datatype == 'depth':
                self._depth_messages_received += 1
            elif datatype == 'trade':
                self._trade_messages_received += 1

    def _receive_futures(self, msg):
        data = msg['data']
        datatype = 'depth' if data['e'] == 'depthUpdate' else 'trade'
        symbol = data['s']
        stream = self._get_file_stream(symbol, 'FUTURES', datatype)
        pickle.dump(data, stream, protocol=pickle.HIGHEST_PROTOCOL)
        with self._lock:
            if datatype == 'depth':
                self._depth_messages_received += 1
            elif datatype == 'trade':
                self._trade_messages_received += 1

    def _start_snapshot_loop(self):
        self._snapshot_thread = Thread(target=self._snapshot_loop)
        self._snapshot_thread.start()

    def _start_socket(self):
        spot_streams = list()
        futures_streams = list()
        for symbol in self._symbols:
            trade_stream = symbol['symbol'].lower() + '@trade'
            if symbol['spot']:
                depth_stream = symbol['symbol'].lower() + symbol['spot_depth_stream']
                spot_streams.extend([depth_stream, trade_stream])
            if symbol['futures']:
                depth_stream = symbol['symbol'].lower() + symbol['futures_depth_stream']
                futures_streams.extend([depth_stream, trade_stream])
        self._twm.start()
        if spot_streams:
            self._spot_conn = self._twm.start_multiplex_socket(callback=self._receive_spot, streams=spot_streams)
        if futures_streams:
            self._futures_conn = self._twm.start_futures_multiplex_socket(callback=self._receive_futures, streams=futures_streams)
        # self._twm.join()
