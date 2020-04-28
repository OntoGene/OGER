#!/usr/bin/env python3
# coding: utf8

# Author: Nicola Colic, 2018


'''
Formatter for PubAnnotation JSON output.

http://www.pubannotation.org/docs/annotation-format/
'''


__all__ = ['PubAnnoJSONFormatter']


import json

from .document import Article, Section
from .export import StreamFormatter


class PubAnnoJSONFormatter(StreamFormatter):
    '''
    PubAnnotation JSON format.
    '''
    ext = 'json'

    def write(self, stream, content):
        if isinstance(content, Section):
            json_object = self._division(content)
        elif isinstance(content, Article):
            json_object = self._document(content)
        else:
            json_object = [self._document(a)
                           for a in content.get_subelements(Article)]
        return json.dump(json_object, stream, indent=2)

    def _division(self, section):
        return self._annotation(section, offset=section.start,
                                sourceid=section.article.id_,
                                divid=section.id_+1)

    def _document(self, article):
        return self._annotation(article, sourceid=article.id_)

    def _annotation(self, content, offset=0, **ann):
        ann['text'] = content.text
        ann['denotations'] = list(self._entities(content, offset))
        ann.update(self._metadata())
        return ann

    @staticmethod
    def _entities(content, offset):
        for id_, entity in enumerate(content.iter_entities(), start=1):
            yield {'id' : 'T{}'.format(id_),
                   'span' : {'begin': entity.start-offset,
                             'end': entity.end-offset},
                   'obj' : entity.cid}

    def _metadata(self):
        meta = dict(self.config.p.pubanno_meta)
        meta.setdefault(
            'sourcedb',
            'PubMed' if self.config.p.article_format == 'pubmed' else 'unknown')
        return meta
