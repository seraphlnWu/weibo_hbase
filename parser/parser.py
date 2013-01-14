# coding=utf8
'''
    parse the data structure for mongodb and hbase
'''
from models import ModelFactory
from error import DataError
from utils import import_simplejson


class Parser(object):
    '''     
        parser base class
    '''     
    def parse(self, method, payload):
        ''' 
            parse the response and return the result
        ''' 
        raise NotImplementedError
           
    def parse_error(self, method, payload):
        ''' 
            parse the error response and return the result
        ''' 
        raise NotImplementedError

    def de_parse(self, prefix, method, payload):
        ''' 
            deparse the response and return the result
        ''' 
        raise NotImplementedError

    def de_parse_error(self, method, payload):
        ''' 
            deparse the error response and return the result
        ''' 
        raise NotImplementedError
        


class ModelParser(Parser):
    '''
        parse a model 
    '''
    def __init__(self, model_factory=None):
        self.model_factory = model_factory or ModelFactory

    def parse(self, method, payload):
        try: 
            if method is None:
                return

            model = getattr(self.model_factory, method)
        except AttributeError:
            raise DataError('No model for this payload type: %s' % (method))

        if isinstance(payload, list):
            result = model.parse_list(payload[0], payload)
        else:
            result = model.parse(payload, payload)

        return result


    def de_parse(self, prefix, method, payload):
        '''
            de parse a structure 
        '''
        try: 
            if method is None:
                return

            model = getattr(self.model_factory, method)
        except AttributeError:
            raise DataError('No model for this payload type: %s' % (method))

        if isinstance(payload, list):
            result = model.de_parse_list(prefix, payload)
        else:
            result = model.de_parse(prefix, payload)

        return result
