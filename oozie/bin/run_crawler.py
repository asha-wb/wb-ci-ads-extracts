import os
os.environ['PYTHON_EGG_CACHE'] = '/tmp/'
os.environ['PYTHON_EGG_DIR'] = '/tmp/'

import hive_crawler

if __name__ == '__main__':
    hive_crawler.main()
