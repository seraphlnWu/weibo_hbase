# coding=utf8
'''
    This is the log module for weibo_hbase module.
'''
import logging
import threading  
 
_LOCALS = threading.local()
 
LOG_FILE = 'smlog.log'
LOG_LEVEL = logging.DEBUG
 
def get_logger(package_name):
    '''
        get a logger instance
    '''
    logger = getattr(_LOCALS, package_name, None)
    if logger is not None:
        return logger
 
    logger = logging.getLogger(package_name)
    hdlr = logging.FileHandler(LOG_FILE)
    formatter = logging.Formatter(
        '[%(asctime)s] Level:%(levelname)s location:%(name)s FuncName:%(funcName)s Line:%(lineno)d Message:%(message)s',
        '%Y-%m-%d %a %H:%M:%S',
    )
    hdlr.setFormatter(formatter)  
    logger.addHandler(hdlr)  
    logger.setLevel(logging.NOTSET)  
    logger.setLevel(LOG_LEVEL)
    setattr(_LOCALS, package_name, logger)  
    return logger
