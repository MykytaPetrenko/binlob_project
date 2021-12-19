import yaml
from binlob.feed.async_writer import FeedWriter
import threading
import logging
import asyncio





async def main():
    with open('tests/writer_config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    writer = FeedWriter(**config)
    writer.start()
    while True:
        await asyncio.sleep(1)


loop = asyncio.get_event_loop()
print(loop)
loop.run_until_complete(main())
