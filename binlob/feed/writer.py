from binance import Client, ThreadedWebsocketManager


class FeedWriter:
    def __init__(self, symbols: list, limit: int, high_res: bool = False):
        self._client = Client(api_key='', api_secret='')
        self._twm = ThreadedWebsocketManager(api_key='', api_secret='')
        self._refresh_time = 60
        self._temp_dir = 'C:/temp'
        self._output_dir = 'C:/output'
        self._limit = limit
        self._symbols = symbols
        self._high_res = high_res

    def _write_orderbook(self, symbol: str):
        orderbook = self._client.get_order_book(symbol=symbol, limit=self._limit)

    def _init_files(self, symbol):
        pass

    def _process_message(self, msg):
        print(msg['stream'])

    def _start_socket(self):
        streams = list()
        for symbol in self._symbols:
            depth_stream = symbol.lower() + '@depth' + ('@100ms' if self._high_res else '')
            trade_stream = symbol.lower() + '@trade'
            streams.extend([depth_stream, trade_stream])

        self._twm.start()

        self._conn_key = self._twm.start_multiplex_socket(callback=self._process_message, streams=streams)

        self._twm.join()

