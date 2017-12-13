#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Loader and formatter for BioC XML.
'''


import logging

from lxml import etree
from lxml.builder import E

from .document import Collection, Article, Entity, EntityTuple
from .load import CollLoader, text_node
from .export import XMLMemoryFormatter
from ..util.iterate import peekaheaditer
from ..util.misc import iter_codepoint_indices_utf8, iter_byte_indices_utf8


class BioCLoader(CollLoader):
    '''
    Parser for BioC XML.

    Currently, any existing relation nodes are discarded.
    '''
    def __init__(self, config):
        super().__init__(config)
        self._warned_already = set()  # cache warnings
        anchor = self.config.p.byte_offsets
        self._anno_byte_offsets = bool(anchor and 'annotation' in anchor)

    def collection(self, source, id_):
        '''
        Read BioC XML into a document.Collection object.
        '''
        collection = Collection(id_)

        it = peekaheaditer(self._iterparse(source))
        coll_node = next(it).getparent()
        collection.metadata = self._meta_dict(coll_node)

        for doc in it:
            collection.add_article(self._article(doc))

        return collection

    def iter_documents(self, source):
        '''
        Iterate over document.Article objects.
        '''
        for doc in self._iterparse(source):
            yield self._article(doc)

    @staticmethod
    def _iterparse(source):
        for _, node in etree.iterparse(source, tag='document'):
            yield node
            node.clear()

    def _article(self, node):
        '''
        Read a document node into a document.Article object.
        '''
        article = Article(node.find('id').text,
                          tokenizer=self.config.text_processor)
        article.metadata = self.infon_dict(node)
        article.year = article.metadata.pop('year', None)
        article.type_ = article.metadata.pop('type', None)
        for passage in node.iterfind('passage'):
            sec_type, text, offset, infon, anno = self._section(passage)
            article.add_section(sec_type, text, offset)
            section = article.subelements[-1]
            section.metadata = infon
            self._insert_annotations(section, anno)
            # Get infon elements on sentence level.
            for sent, sent_node in zip(section, passage.iterfind('sentence')):
                sent.metadata = self.infon_dict(sent_node)
        return article

    def _section(self, node):
        'Get type, text and offset from a passage node.'
        infon = self.infon_dict(node)
        type_ = infon.pop('type', None)
        offset = int(node.find('offset').text)
        text = text_node(node, 'text', ifnone='')
        if text is None:
            # Text and annotations at sentence level.
            text, anno = [], []
            for sent in node.iterfind('sentence'):
                t, o = self._sentence(sent)
                text.append((t, o))
                anno.extend(self._get_annotations(sent, t, o))
        else:
            # Text and annotations at passage level.
            anno = self._get_annotations(node, text, offset)
        return type_, text, offset, infon, anno

    @staticmethod
    def _sentence(node):
        'Get text and offset from a sentence node.'
        offset = int(node.find('offset').text)
        text = text_node(node, 'text', ifnone='')
        return text, offset

    def _get_annotations(self, node, text, offset):
        '''
        Iterate over annotations.

        Any non-contiguous annotation is split up into
        multiple contiguous annotations.

        If the offsets are given as bytes, recalculate them
        wrt codepoints, relative to the start offset of the
        level at which they are anchored (sentence/passage).
        '''
        if self._anno_byte_offsets:
            index = list(iter_byte_indices_utf8(text))
            def _offset_conv(i):
                return index[i-offset]+offset
            for start, end, anno in self._get_raw_annotations(node):
                start, end = _offset_conv(start), _offset_conv(end)
                yield (start, end, anno)
        else:
            yield from self._get_raw_annotations(node)

    @staticmethod
    def _get_raw_annotations(node):
        for anno in node.iterfind('annotation'):
            for loc in anno.iterfind('location'):
                start = int(loc.get('offset'))
                end = start + int(loc.get('length'))
                yield (start, end, anno)

    def _insert_annotations(self, section, annotations):
        '''
        Add term annotations to the correct sentence.

        This method changes the section by side-effect.
        '''
        entities = sorted(annotations, key=lambda e: e[:2])
        if not entities:
            # Short circuit if possible.
            return

        sentences = iter(section)
        try:
            sent = next(sentences)
            for start, end, anno in entities:
                while start >= sent.end:
                    sent = next(sentences)
                sent.entities.append(self._entity(anno, start, end))
        except StopIteration:
            logging.warning('annotations outside character range')

    def _entity(self, anno, start, end):
        'Create an EntityTuple instance from a BioC annotation node.'
        id_ = anno.get('id')
        text = text_node(anno, 'text', ifnone='')
        info = self._entity_info(anno)
        return EntityTuple(id_, text, start, end, info)

    def _entity_info(self, anno):
        'Create an `info` tuple.'
        infons = self.infon_dict(anno)
        values = tuple(infons.pop(label, 'unknown')
                       for label in Entity.fields)
        for unused in infons:
            if unused not in self._warned_already:
                logging.warning('ignoring BioC annotation attribute %s',
                                unused)
                self._warned_already.add(unused)
        return values

    def _meta_dict(self, node):
        'Read metadata into a dictionary.'
        meta = {n: node.find(n).text for n in ('source', 'date', 'key')}
        meta.update(self.infon_dict(node))
        return meta

    @staticmethod
    def infon_dict(node):
        'Read all infon nodes into a dictionary.'
        return {n.attrib['key']: n.text for n in node.iterfind('infon')}


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
        self._passage_anchor = 'passage' in anchor
        self._sentence_anchor = 'sentence' in anchor
        self._cumulated = 0
        self._last_sentence = 0
        self._sent_start = None
        self._cp_index = None

    def passage(self, section):
        '''
        New passage/section.

        Update counts and return the cumulated start offset.
        '''
        if self._passage_anchor:
            self._cumulated = section.start
        elif self._sentence_anchor and section.subelements:
            # If there are sentence anchors, use the first sentence's offset.
            self._cumulated = section.subelements[0].start
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
        if self._sentence_anchor:
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
