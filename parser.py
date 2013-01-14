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


class JsonObjectParser(Parser):
    '''      
        a parser class for json object.
    '''      
             
    payload_format = 'json'
             
    def __init__(self):
        '''  
        '''  
        self.json = import_simplejson()

    def parse(self, method, payload):
        '''  
            parse the payload
        '''  
        try: 
            json = self.json.loads(payload)
        except Exception as e:
            print "Cannot parse the payload"
            raise DataError("Failed to parse JSON object: %s", (e, ))

        return json

    def parse_error(self, method, payload):
        '''  
            parse the error payload
        '''  
        try: 
            json = self.json.loads(payload)
        except Exception as e:
            print "Cannot parse the error payload"
            raise DataError("Failed to parse JSON ERROR object: %s", (e, ))
             
        return json


class ModelParser(JsonObjectParser):
    '''
        parse a model 
    '''
    def __init__(self, model_factory=None):
        JsonObjectParser.__init__(self)
        self.model_factory = model_factory or ModelFactory

    def parse(self, method, payload):
        try: 
            if method.payload_type is None:
                return

            model = getattr(self.model_factory, method.payload_type)
        except AttributeError:
            raise WeibopError(
                'No model for this payload type: %s' % (method.payload_type, ))

        json = JsonObjectParser.parse(self, method, payload)
        if isinstance(json, tuple):
            json, cursors = json
        else:
            cursors = None
             
        if method.payload_list:
            result = model.parse_list(method.api, json)
        else:
            result = model.parse(method.api, json)
        if cursors:
            return result, cursors
        else:
            return result
