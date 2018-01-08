from setuptools import setup, find_packages

setup(
    name='wb_next_gen_etl',
    version='1.0',
    description='PySpark jobs to crawl arbitrary S3 locations and load '+
    'results into Redshift and S3 data lake',
    packages=find_packages(),
    install_requires=[
        'pyjks',
        'anytree',
        'parquet',
        'boto3',
        'chardet'
    ]
)
