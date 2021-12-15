import yaml
from binlob import FeedWriter


def main():
    with open('writer_config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    writer = FeedWriter(**config)
    writer.start()


if __name__ == '__main__':
    main()
