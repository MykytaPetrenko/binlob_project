import asyncio
import threading
from binance.streams import ReconnectingWebsocket
from datetime import datetime
import os
import pickle as pickle
import logging
from binance import BinanceSocketManager, AsyncClient


ReconnectingWebsocket.MAX_QUEUE_SIZE = 10000


class FeedWriter:
    def __init__(self, **kwargs):
        self._lock = threading.Lock()
        self._log = logging.getLogger(__name__)
        self.reconnect_timeout = 2
        # outputs
        self._temp_dir = kwargs.get('temp_dir', 'temp')
        self._output_dir = kwargs.get('output_dir', 'output')
        self._check_dirs()

        # Snapshots params
        self._do_snapshots = kwargs.get('book_snapshots', True)
        if self._do_snapshots:
            self._limit = kwargs.get('snapshot_limit', 100)
            self._snapshot_period = kwargs.get('', 60)

        # Stream params
        depth_streams = kwargs.get('depth_streams', None)
        trade_streams = kwargs.get('trade_streams', None)
        self._futures_streams = set()
        self._spot_streams = set()
        self._tickers = set()
        # Manage depth streams
        if depth_streams:
            for stream in depth_streams.get('spot', list()):
                ticker = stream.split('@')[0]
                self._spot_streams.add(stream)
                self._tickers.add(('SPOT', ticker.upper()))
            for stream in depth_streams.get('futures', list()):
                ticker = stream.split('@')[0]
                self._futures_streams.add(stream)
                self._tickers.add(('FUTURES', ticker.upper()))
        # Manage trade streams
        if trade_streams:
            for stream in trade_streams.get('spot', list()):
                self._spot_streams.add(stream)
            for stream in trade_streams.get('futures', list()):
                self._futures_streams.add(stream)

        self._num_streams = len(self._spot_streams) + len(self._futures_streams)
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

    def _get_writer(self, symbol: str, section: str, data_type: str, today: datetime):
        stream_name = f'{symbol.lower()}_{section.lower()}_{data_type}'

        with self._lock:
            stream, creation_day = self._output_streams.get(stream_name, (None, None))
            if stream is not None and today.day != creation_day:
                stream.close()

            if today.day != creation_day:
                date_str = today.strftime('%Y-%m-%d')
                filename = f'{symbol}-{date_str}-{section}-{data_type}.pkl'
                stream = open(os.path.join(self._temp_dir, filename), 'wb')
                self._output_streams[stream_name] = (stream, today.day)
        return stream

    async def _snapshot_loop(self):
        first_run = True
        await asyncio.sleep(10)
        client = AsyncClient(api_key='', api_secret='')
        while True:
            for section, ticker in self._tickers:
                if section == 'SPOT':
                    try:
                        orderbook = await client.get_order_book(symbol=ticker, limit=self._limit)
                    except Exception as ex:
                        self._log.error(f'Cannot get spot orderbook: {str(ex)}')
                        return
                elif section == 'FUTURES':
                    try:
                        orderbook = await client.futures_order_book(symbol=ticker, limit=self._limit)
                    except Exception as ex:
                        self._log.error(f'Cannot get futures orderbook: {str(ex)}')
                        return
                else:
                    raise Exception(f'Incorrect section. Got {section}. Use "SPOT" or "FUTURES"')
                today = datetime.utcnow()
                stream = self._get_writer(ticker, section, 'book', today)
                pickle.dump(orderbook, stream, protocol=pickle.HIGHEST_PROTOCOL)
                self._increment_stat(ticker, section, 'book')
                stream.flush()
                if first_run:
                    pass
                else:
                    await asyncio.sleep(self._request_period)

            if first_run:
                await asyncio.sleep(self._request_period)
                first_run = False

    async def _info_loop(self):
        while True:
            print('-' * 60)
            print(f'UTC TIME: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
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
            await asyncio.sleep(60)

    async def _snapshots_and_info(self):
        await asyncio.gather(self._info_loop(), self._snapshot_loop())

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
            today = datetime.utcfromtimestamp(data['E'] / 1000)
            stream = self._get_writer(symbol, section, datatype, today=today)
            pickle.dump(data, stream, protocol=pickle.HIGHEST_PROTOCOL)
            self._increment_stat(symbol, section, datatype)
        else:
            return

    def _route_spot(self, msg):
        self._write_message(msg, 'SPOT')

    def _route_futures(self, msg):
        self._write_message(msg, 'FUTURES')

    # New Code
    async def _ws_track_loop(self, section: str = 'SPOT'):
        client = AsyncClient(api_key='', api_secret='')
        bwm = BinanceSocketManager(client)

        if section.lower() == 'futures':
            streams = list(self._futures_streams)
        else:
            streams = list(self._spot_streams)

        ws = bwm.multiplex_socket(
            streams
        )

        async with ws as ws_context:
            while True:
                try:
                    result = await ws_context.recv()
                    self._write_message(result, section)
                except asyncio.TimeoutError:
                    self._log.error('Timeout. Reconnecting...')
                    asyncio.create_task(self._ws_track_loop(section))

    def _ws_target(self, section: str = 'SPOT'):
        loop = asyncio.new_event_loop()
        loop.create_task(self._ws_track_loop(section))
        loop.run_forever()

    def _snapshot_and_info_target(self):
        loop = asyncio.new_event_loop()

        if self._do_snapshots:
            loop.run_until_complete(self._snapshots_and_info())
        else:
            loop.run_until_complete(self._info_loop())

    def start(self):
        ws_spot_th = threading.Thread(target=self._ws_target, args=('SPOT',))
        ws_spot_th.daemon = True
        ws_spot_th.start()

        ws_futures_th = threading.Thread(target=self._ws_target, args=('FUTURES',))
        ws_futures_th.daemon = True
        ws_futures_th.start()

        requests_info_th = threading.Thread(target=self._snapshot_and_info_target)
        requests_info_th.start()

    def stop(self):
        # Close all file streams
        # and stop threads
        pass
