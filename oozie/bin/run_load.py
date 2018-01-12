import os
os.environ['PYTHON_EGG_CACHE'] = '/tmp/'
os.environ['PYTHON_EGG_DIR'] = '/tmp/'

import s3_load

if __name__ == '__main__':
    s3_load.main()
