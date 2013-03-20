# coding=utf8
''' parse the data structure between mongodb type and hbase type '''

from models import ModelFactory
from error import DataError


class Parser(object):
    ''' parser base class '''     

    def serialized(self, method, payload):
        ''' parse the response and return the result ''' 
        raise NotImplementedError
           
    def deserialized(self, prefix, method, payload):
        ''' deparse the response and return the result ''' 
        raise NotImplementedError


class ModelParser(Parser):
    ''' parse models '''

    def __init__(self, model_factory=None):
        ''' 
            init the model_factory 
            
            @model_factory: default will be the ModelFactory
        '''
        self.model_factory = model_factory or ModelFactory

    def serialized(self, method, payload):
        '''
            parse the give payload to the type of method 
            
            @method: the given type.
            @payload: data object need to be translate.
        '''
        try:
            if method is None: return

            model = getattr(self.model_factory, method)
        except AttributeError:
            raise DataError('No model for this payload type: %s' % (method))

        if isinstance(payload, list):
            result = model.serialized_list(payload)
        else:
            result = model.serialized(payload)

        return result


    def deserialized(self, method, payload):
        ''' de parse a structure '''
        try: 
            if method is None:
                return

            model = getattr(self.model_factory, method)
        except AttributeError:
            raise DataError('No model for this payload type: %s' % (method))

        if isinstance(payload, list):
            result = model.deserialized_list(payload)
        else:
            result = model.deserialized(payload)

        return result
