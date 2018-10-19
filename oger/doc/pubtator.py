#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2018


'''
Formatter for the PubTator format.
'''


import csv

from .export import StreamFormatter


class PubTatorFormatter(StreamFormatter):
    '''
    Create a mixture of pipe- and tab-separated plain-text.
    '''
    ext = 'txt'

    def write(self, stream, content):
        tsv = csv.writer(stream, delimiter='\t', quotechar=None)
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
