#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2018


'''
Loader and formatter for the PubTator format.
'''


import csv
import logging
import itertools as it

from .document import Collection, Article, Entity
from .load import CollLoader
from .export import StreamFormatter
from ..util.misc import tsv_format
from ..util.stream import text_stream


class PubTatorLoader(CollLoader):
    '''
    Load PubTator documents.
    '''

    _section_labels = {'t': 'Title', 'a': 'Abstract'}

    def collection(self, source, id_):
        entity_counter = it.count(1)
        docs = self._iter_documents(source, entity_counter)
        return Collection.from_iterable(docs, id_)

    def iter_documents(self, source):
        return self._iter_documents(source)

    def _iter_documents(self, source, entity_counter=None):
        with text_stream(source) as f:
            for doc_lines in self._split(f):
                yield self._document(doc_lines, entity_counter)

    @staticmethod
    def _split(stream):
        doc_lines = []
        for line in stream:
            if line.strip():
                doc_lines.append(line)
            elif doc_lines:
                yield doc_lines
                doc_lines = []
        if doc_lines:
            yield doc_lines

    def _document(self, lines, entity_counter):
        if entity_counter is None:
            entity_counter = it.count(1)
        docid, sections, anno = self._parse(lines, entity_counter)
        article = Article(docid, tokenizer=self.config.text_processor)
        for label, text in sections:
            article.add_section(label, text)
        self._insert_annotations(article, anno)
        return article

    def _parse(self, lines, entity_counter):
        docid = None
        sections = []
        anno = []
        for line in lines:
            sep = self._separator(line, docid)
            docid, *fields = line.split(sep)
            if sep == '|' and not anno:
                sections.append(self._section(fields))
            elif sep == '\t' and sections:
                if len(fields) == 3:
                    # Relation annotations are silently ignored.
                    continue
                fields[-1] = fields[-1].rstrip('\n\r')
                anno.append(self._entity(fields, entity_counter))
            else:
                raise ValueError('invalid format: doc {}'.format(docid))
        return docid, sections, anno

    @staticmethod
    def _separator(line, docid):
        if docid is None:
            return '|'

        i = len(docid)
        if line[:i] != docid:
            raise ValueError('inconsistent document IDs ({}, {})'
                             .format(docid, line[:i]))
        return line[i]

    def _section(self, fields):
        try:
            label, text = fields
        except ValueError:  # pipe character in text body
            label = fields[0]
            text = '|'.join(fields[1:])
        label = self._section_labels.get(label)
        return label, text

    @staticmethod
    def _entity(fields, ids):
        start, end, text, type_, cid = fields
        info = (type_, 'unknown', 'unknown', cid, 'unknown')
        return Entity(next(ids), text, int(start), int(end), info)

    @staticmethod
    def _insert_annotations(article, entities):
        if not entities:
            # Short circuit if possible.
            return

        doc = article.text
        Entity.sort(entities)  # sort by offset
        sentences = article.get_subelements('sentence')
        try:
            sent = next(sentences)
            for entity in entities:
                while entity.start >= sent.end:
                    sent = next(sentences)
                sent.entities.append(entity)
                if doc[entity.start:entity.end] != entity.text:
                    logging.warning('offset mismatch: doc %s', article.id_)
        except StopIteration:
            logging.warning('annotations outside character range')


class PubTatorFBKLoader(PubTatorLoader):
    '''
    Load FBK-flavored PubTator documents.
    '''
    @staticmethod
    def _entity(fields, ids):
        del ids  # unused argument
        id_, type_, start, end, text = fields
        try:
            id_ = int(id_.lstrip('T'))
        except ValueError:
            pass
        info = (type_, 'unknown', 'unknown', 'unknown', 'unknown')
        return Entity(id_, text, int(start), int(end), info)


class PubTatorFormatter(StreamFormatter):
    '''
    Create a mixture of pipe- and tab-separated plain-text.
    '''
    ext = 'txt'

    def write(self, stream, content):
        tsv = csv.writer(stream, **tsv_format)
        first = True
        for article in content.get_subelements('article', include_self=True):
            if first:
                first = False
            else:
                stream.write('\n')
            self._write_article(stream, tsv, article)

    def _write_article(self, stream, tsv, article):
        try:
            # Make sure the spans are relative to the start of the document.
            offset = -1 * next(article.get_subelements('sentence')).start
        except StopIteration:
            # Empty document (no sentences).
            offset = 0

        annotations = []
        for sec in article:
            text = self._single_line(sec.text)
            stream.write(self._textline(article.id_, sec.type_, text))
            annotations.extend(self._annotations(article.id_, sec, offset))
            offset += len(text) - len(sec.text)
        tsv.writerows(annotations)

    @staticmethod
    def _single_line(text):
        '''Remove internal newlines and trailing whitespace.'''
        text = text.replace('\n', ' ')
        text = text.replace('\r', ' ')
        text = text.rstrip()
        text = text + '\n'
        return text

    @staticmethod
    def _textline(id_, type_, text):
        id_ = id_ if id_ else 'unknown'
        t = type_[0].lower() if type_ else 'x'
        return '|'.join((id_, t, text))

    def _annotations(self, docid, sec, offset):
        for entity in sec.iter_entities():
            start, end = entity.start+offset, entity.end+offset
            yield self._select_anno_fields(docid, start, end, entity)

    @staticmethod
    def _select_anno_fields(docid, start, end, entity):
        return (docid, start, end, entity.text, entity.type, entity.cid)


class PubTatorFBKFormatter(PubTatorFormatter):
    '''
    FBK flavor of the PubTator format.
    '''
    @staticmethod
    def _select_anno_fields(docid, start, end, entity):
        try:
            id_ = int(entity.id_)
        except ValueError:
            id_ = entity.id_
        else:
            id_ = 'T{}'.format(id_)
        return (docid, id_, entity.type, start, end, entity.text)
