# coding=utf8

'''
    error module.
'''
class DataError(StandardError):
    '''
        raise DataError if got failed message.
    '''
    def __init__(self, reason):
        '''
            init func. 
        '''
        self.reason = reason

    def __str__(self):
        '''
            
        '''
        return self.reason.encode('utf8')
