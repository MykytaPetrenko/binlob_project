"""
pip install
"""

from binlob import FeedWriter


def main():
    writer = FeedWriter(['BNBUSDT'], 100)
    writer._start_socket()


if __name__ == '__main__':
    main()
