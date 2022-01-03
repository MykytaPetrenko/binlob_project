import yaml
from binlob import FeedWriter
import logging


logging.basicConfig(level=logging.ERROR, filename='writer.log')


def main():
    with open('tests/writer_config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    writer = FeedWriter(**config)
    writer.start()


if __name__ == '__main__':
    main()
