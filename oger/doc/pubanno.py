#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017
# http://www.pubannotation.org/docs/annotation-format/


'''
Formatter for simple JSON output.
(Compatible with PubAnnotation.)
http://www.pubannotation.org/docs/annotation-format/
'''

import json

from .export import MemoryFormatter


class PubAnnoJSONFormatter(MemoryFormatter):
    '''
    Light XML format for annotations only.
    '''
    
    ext = 'json'
    
    def dump(self, content):
        
        json_object = {'text': '', 'denotation':[]}
        json_object['text'] = content.__str__()
        
        for entity in content.iter_entities():
            denotation_object = { 'id' : entity.id_,
                                  'span' : '',
                                  'obj' : ''}
            span_object = { 'begin' : entity.start ,
                            'end' : entity.end }
            denotation_object['span'] = span_object
            denotation_object['obj'] = entity.cid
            
            json_object['denotation'].append(denotation_object)
        
        json_string = json.dumps(json_object)
        return json_string
