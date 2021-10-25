import yaml
from binlob import FeedWriter


def main():
    with open('writer_config.yaml', 'r') as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            print(exc)

    writer = FeedWriter(config)
    writer.start()


if __name__ == '__main__':
    main()
