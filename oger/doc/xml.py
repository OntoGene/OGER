#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Formatter for simple XML output.
'''


from lxml.builder import E

from .document import Collection
from .export import XMLMemoryFormatter


class EntityXMLFormatter(XMLMemoryFormatter):
    '''
    Light XML format for annotations only.
    '''
    def _dump(self, content):
        root = E('entities')
        if isinstance(content, Collection):
            label = 'collection_id'
        else:
            label = 'article_id'
        root.set(label, str(content.id_))

        for entity in content.iter_entities():
            root.append(self._entity(entity))

        return root

    def _entity(self, entity):
        node = E('entity',
                 id=str(entity.id_),
                 start=str(entity.start),
                 end=str(entity.end))

        for label, value in entity.info_items(self.config.entity_fields):
            node.set(label, value)

        node.text = entity.text

        return node


class TextXMLFormatter(XMLMemoryFormatter):
    '''
    Light XML format for text only.
    '''
    def _dump(self, content):
        if isinstance(content, Collection):
            return self._collection(content)
        else:
            return self._article(content)

    def _collection(self, coll):
        node = E('collection', id=str(coll.id_))

        for article in coll:
            node.append(self._article(article))

        return node

    def _article(self, article):
        node = E('article', id=article.id_)

        if article.year is not None:
            node.set('year', article.year)
        if article.type_ is not None:
            node.set('type', article.type_)

        for section in article:
            node.append(self._section(section))

        return node

    def _section(self, section):
        node = E('section', id=str(section.id_), type=section.type_)

        for sent in section:
            node.append(self._sentence(sent))

        return node

    def _sentence(self, sent):
        node = E('sentence', id=str(sent.id_))

        sent.tokenize()
        for tok in sent:
            node.append(self._token(tok))

        return node

    @staticmethod
    def _token(tok):
        node = E('token',
                 id=str(tok.id_),
                 start=str(tok.start),
                 end=str(tok.end),
                 length=str(tok.end-tok.start))
        node.text = tok.text
        return node
