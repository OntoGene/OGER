#!/usr/bin/env python3
# coding: utf8

# Author: Nicola Colic, 2018


'''
Formatter for PubAnnotation JSON output.

http://www.pubannotation.org/docs/annotation-format/
'''


__all__ = ['PubAnnoJSONFormatter']


import json

from .document import Article
from .export import StreamFormatter


class PubAnnoJSONFormatter(StreamFormatter):
    '''
    PubAnnotation JSON format.
    '''
    ext = 'json'

    def write(self, stream, content):
        if isinstance(content, Article):
            json_object = self._document(content)
        else:
            json_object = [self._document(a)
                           for a in content.get_subelements(Article)]
        return json.dump(json_object, stream, indent=2)

    def _document(self, article):
        doc = {}
        doc['text'] = article.text
        doc['denotations'] = [self._entity(e)
                              for e in article.iter_entities()]
        doc['sourceid'] = article.id_
        doc.update(self._metadata())
        return doc

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

    def _metadata(self):
        meta = dict(self.config.p.pubanno_meta)
        meta.setdefault(
            'sourcedb',
            'PubMed' if self.config.p.article_format == 'pubmed' else 'unknown')
        return meta
