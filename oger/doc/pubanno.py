#!/usr/bin/env python3
# coding: utf8

# Author: Nicola Colic, 2018


'''
Formatter for PubAnnotation JSON output.

http://www.pubannotation.org/docs/annotation-format/
'''


import json

from .document import Section
from .export import StreamFormatter


class PubAnnoJSONFormatter(StreamFormatter):
    '''
    PubAnnotation JSON format.
    '''
    ext = 'json'

    def write(self, stream, content):
        json_object = {}
        json_object['text'] = ''.join(
            s.text for s in content.get_subelements(Section))
        json_object['denotations'] = [self._entity(e)
                                      for e in content.iter_entities()]
        return json.dump(json_object, stream)

    def _entity(self, entity):
        return {'id' : self._format_id(entity.id_),
                'span' : {'begin': entity.start,
                          'end': entity.end},
                'obj' : entity.cid}

    @staticmethod
    def _format_id(id_):
        '''
        For numeric IDs, produce "T<N>" format.
        '''
        if isinstance(id_, int) or id_.isdigit():
            return 'T{}'.format(id_)
        else:
            return id_
