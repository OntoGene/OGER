#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Loader and formatter for BioC XML.
'''


from lxml.builder import E

from .document import Collection, Entity
from .export import XMLMemoryFormatter
from ..util.misc import iter_codepoint_indices_utf8


class BioCFormatter(XMLMemoryFormatter):
    '''
    BioC XML output format.
    '''
    doctype = '<!DOCTYPE collection SYSTEM "BioC.dtd">'

    def write(self, stream, content):
        if isinstance(content, Collection):
            # For their size, serialise collections in a memory-friendly way.
            # The downside is that indentation isn't perfect.
            for chunk in self._iter_bytes(content):
                stream.write(chunk)
        else:
            super().write(stream, content)

    def _dump(self, content):
        coll = self._wrap_in_collection(content)
        return self._collection(coll)

    def _tostring(self, node, **kwargs):
        kwargs.setdefault('doctype', self.doctype)
        return super()._tostring(node, **kwargs)

    @staticmethod
    def _wrap_in_collection(content):
        if not isinstance(content, Collection):
            # Wrap this document in a collection.
            coll = Collection(content.id_, content.basename)
            coll.add_article(content)
            content = coll
        return content

    def _iter_bytes(self, coll):
        '''
        Iterate over fragments of serialised BioC bytes.
        '''
        # Serialise the outer shell and split off the closing tag.
        shell = self._tostring(self._collection_frame(coll))
        tail = '</collection>\n'.encode('UTF-8')
        head = shell[:-len(tail)]

        # Yield fragment by fragment.
        yield head

        for article in coll:
            node = self._document(article)
            frag = self._tostring(node, doctype=None, xml_declaration=False)
            yield frag

        yield tail

    def _collection(self, coll):
        node = self._collection_frame(coll)
        for article in coll:
            node.append(self._document(article))
        return node

    def _collection_frame(self, coll):
        meta = self.config.p.bioc_meta
        if meta is None:
            meta = coll.metadata

        node = E('collection',
                 E('source', meta.get('source', '')),
                 E('date', meta.get('date', '')),
                 E('key', meta.get('key', '')))

        for key, value in meta.items():
            if key not in ('source', 'date', 'key'):
                self._infon(node, key, value)

        return node

    def _document(self, article):
        node = E('document', E('id', article.id_))

        if article.year is not None:
            self._infon(node, 'year', article.year)
        if article.type_ is not None:
            self._infon(node, 'type', article.type_)
        self._add_meta(node, article.metadata)

        offset_mngr = get_offset_manager(self.config.p.byte_offsets)
        for section in article:
            node.append(self._passage(section, offset_mngr))

        return node

    def _passage(self, section, offset_mngr):
        node = E('passage')
        if section.type_ is not None:
            self._infon(node, 'type', section.type_)
        self._add_meta(node, section.metadata)
        node.append(E('offset', str(offset_mngr.passage(section))))

        # BioC allows text at sentence or passage level.
        # The annotations are anchored at the same level.
        if self.config.p.sentence_level:
            for sent in section:
                node.append(self._sentence(sent, offset_mngr))
        else:
            node.append(E('text', section.text))
            for sent in section:
                offset_mngr.sentence(sent)  # synchronise without direct usage
                for entity in sent.iter_entities():
                    node.append(self._entity(entity, offset_mngr))

        return node

    def _sentence(self, sent, offset_mngr):
        node = E('sentence')
        self._add_meta(node, sent.metadata)
        node.append(E('offset', str(offset_mngr.sentence(sent))))
        node.append(E('text', sent.text))

        for entity in sent.iter_entities():
            node.append(self._entity(entity, offset_mngr))

        return node

    def _entity(self, entity, offset_mngr):
        node = E('annotation', id=str(entity.id_))

        for label, value in Entity.info_items(entity):
            self._infon(node, label, value)

        start, length = offset_mngr.entity(entity)
        node.append(E('location', offset=str(start), length=str(length)))

        node.append(E('text', entity.text))

        return node

    def _add_meta(self, node, meta):
        for key, value in meta.items():
            self._infon(node, key, value)

    @staticmethod
    def _infon(node, key, value):
        '''
        Add an <infons> element.
        '''
        node.append(E('infon', value, key=key))


def get_offset_manager(anchor):
    '''
    Get an actual or dummy offset manager.
    '''
    if not anchor:
        return BioCOffsetManager()
    else:
        return BioCByteOffsetManager(anchor)

class BioCOffsetManager:
    '''
    Offer the same interface as BioCByteOffsetManager
    without doing anything interesting.
    '''

    @staticmethod
    def passage(section):
        '''
        Return the start offset of this passage/section.
        '''
        return section.start

    @staticmethod
    def sentence(sentence):
        '''
        Return the start offset of this sentence.
        '''
        return sentence.start

    @staticmethod
    def entity(entity):
        '''
        Return start and length of this annotation.
        '''
        return entity.start, entity.end-entity.start

class BioCByteOffsetManager(BioCOffsetManager):
    '''
    Keep track of bytes offsets.
    '''
    def __init__(self, anchor):
        self.anchor = anchor
        self._cumulated = 0
        self._last_sentence = 0
        self._sent_start = None
        self._cp_index = None

    def passage(self, section):
        '''
        New passage/section.

        Update counts and return the cumulated start offset.
        '''
        if self.anchor == 'passage':
            self._cumulated = section.start
        else:
            self._cumulated += self._last_sentence
            self._last_sentence = 0
        return self._cumulated

    def sentence(self, sentence):
        '''
        New sentence.

        Update counts and return the cumulated start offset.
        '''
        self._cp_index = list(iter_codepoint_indices_utf8(sentence.text))
        self._sent_start = sentence.start
        if self.anchor == 'sentence':
            self._cumulated = sentence.start
        else:
            self._cumulated += self._last_sentence
            self._last_sentence = self._cp_index[-1]
        return self._cumulated

    def entity(self, entity):
        '''
        Calculate start and length for this annotation.
        '''
        start, end = (self._cp_index[n-self._sent_start]+self._cumulated
                      for n in (entity.start, entity.end))
        return start, end-start
