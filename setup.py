from setuptools import setup, find_packages

setup(
    name='binlob',
    version='0.0.0.1',
    packages=find_packages(),
    install_requires=['python-binance'],
    long_description='Binance LOB processing'
)