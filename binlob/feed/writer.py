from binance import Client, ThreadedWebsocketManager
from threading import Thread
import time
from datetime import datetime
import os
import pickle as pickle


class FeedWriter:
    def __init__(self, config: dict):
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
        if not os.path.exists(self._temp_dir):
            os.makedirs(self._temp_dir, exist_ok=True)
        if not os.path.exists(self._output_dir):
            os.makedirs(self._output_dir, exist_ok=True)

    def _get_file_stream(self, symbol: str, section: str, data_type: str):
        pass

    def _write_orderbook(self, symbol: str, section: str = 'SPOT'):
        print('Write')
        # Get stream
        stream_name = f'{symbol.lower()}_{section.lower()}_orderbook'
        if stream_name in self._output_streams:
            stream = self._output_streams[stream_name]['stream']
            stream_day = self._output_streams[stream_name]['day']
        else:
            stream = None
            stream_day = None

        today = datetime.utcnow()
        if today.day != stream_day:
            if stream:
                stream.close()
            date_str = today.strftime('%Y-%m-%d')
            filename = f'{symbol}-{date_str}-{section}-book.pkl'
            stream = open(os.path.join(self._temp_dir, filename), 'wb')
            self._output_streams[stream_name] = dict(stream=stream, day=today.day)

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

    def _init_files(self, symbol):
        pass

    def _receive_spot(self, msg):
        pass
        # print(msg['stream'])

    def _receive_futures(self, msg):
        pass
        # print(msg['stream'])

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
                spot_streams.extend([depth_stream])
            if symbol['futures']:
                depth_stream = symbol['symbol'].lower() + symbol['futures_depth_stream']
                futures_streams.extend([depth_stream])
        self._twm.start()
        if spot_streams:
            self._spot_conn = self._twm.start_multiplex_socket(callback=self._receive_spot, streams=spot_streams)
        if futures_streams:
            self._futures_conn = self._twm.start_futures_multiplex_socket(callback=self._receive_futures, streams=futures_streams)
        # self._twm.join()
