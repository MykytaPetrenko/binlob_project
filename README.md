# BinLOB Project
Visualization of LOB from binance

### Main features

##### Write orderbook and trade data to pickle files


```
from binlob import FeedWriter


writer = FeedWriter(
    output_dir='C:/temp/',
    temp_dir='C:/temp/',
    book_snapshots=True,    # Write limit order book snapshots or not
    snapshot_period=60*5,   # Period between LOB snapshots through http request
    snapshot_limit=100,     # depth limit
    depth_streams={
        'spot': ['bnbusdt@depth@100ms'],
        'futures': ['bnbusdt@depth@100ms']
    },
    trade_streams={
        'spot': ['bnbusdt@trade'],
        'futures': ['bnbusdt@trade']
    }
)
writer.start()
```

