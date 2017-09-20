import os, yaml
import logging, logging.config

def setup_logging(
    default_path=None,
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """Setup logging configuration

    """
    if not default_path:
        default_path='logging.yaml'
    
    path = default_path 
    
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):        
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        x = config['root']['handlers']
        for row, i in enumerate(x):
            #print("row: {}, data: {}".format(row, i))
            filename = config['handlers'][x[row]]['filename']
            fullpath = os.path.abspath(filename)
            #print(fullpath)
            fullpath = fullpath.rsplit(sep='\\', maxsplit=1)
            #print(fullpath)
            if os.path.exists(fullpath[0]):
                print("path exist")
            else:
                print('log folder path does not exist, creating folder')
                os.mkdir(fullpath[0])
                
        #print(config['handlers'][x[0]]['filename'])
        logging.config.dictConfig(config)
        logging.info('creating defined logging configuration!')
    else:
        logging.basicConfig(level=default_level)
        logging.info('creating default logging configuration!')   

