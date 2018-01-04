#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Loaders and formatters for the BeCalm TIPS formats.
'''


import json
import codecs
import logging
from urllib import request as url_request

from .document import Article, Section
from .load import DocIterator
from .export import StreamFormatter


# ======== #
# Loaders. #
# ======== #


class _BeCalmFetcher(DocIterator):
    '''
    Fetch documents from BeCalm's servers.
    '''
    domain = None
    url = None
    textfield = None

    def iter_documents(self, source):
        '''
        Iterate over documents from a BeCalm server.
        '''
        return self._iter_documents(source)

    def _iter_documents(self, docids):
        if not isinstance(docids, (tuple, list)):
            docids = list(docids)
        if not docids:
            raise ValueError('Empty doc-ID list.')
        query = json.dumps({self.domain: docids}).encode('ascii')
        headers = {'Content-Type': 'application/json'}
        logging.info("POST request to BeCalm's server with the query %s", query)
        req = url_request.Request(self.url, data=query, headers=headers)
        with url_request.urlopen(req) as f:
            docs = json.load(codecs.getreader('utf-8')(f))

        for doc in docs:
            yield self._document(doc)

    def _document(self, doc):
        id_ = doc['externalId']
        title = doc['title']
        text = doc[self.textfield]
        article = Article(id_, tokenizer=self.config.text_processor)
        article.add_section('Title', title)
        article.add_section('Abstract', text)
        return article


class BeCalmAbstractFetcher(_BeCalmFetcher):
    '''
    Fetch abstracts from BeCalm's abstract server.
    '''
    domain = 'abstracts'
    url = 'http://193.147.85.10:8088/abstractserver/json'
    textfield = 'text'


class BeCalmPatentFetcher(_BeCalmFetcher):
    '''
    Fetch patent abstracts from BeCalm's patent server.
    '''
    domain = 'patents'
    url = 'http://193.147.85.10:8087/patentserver/json'
    textfield = 'abstractText'


# =========== #
# Formatters. #
# =========== #


class _BeCalmFormatter(StreamFormatter):
    '''
    Common basis for BeCalm's specific output formats.
    '''
    fields = ('document_id', 'section', 'init', 'end', 'score',
              'annotated_text', 'type', 'database_id')

    @staticmethod
    def _iter_entries(content):
        '''
        Iterate over entries needed for BeCalm's output formats.
        '''
        for section in content.get_subelements(Section):
            article_id = section.article.id_
            section_type = 'T' if section.type_.lower() == 'title' else 'A'
            for entity in section.iter_entities():
                yield (
                    article_id,
                    section_type,
                    entity.start,
                    entity.end,
                    0.5,  # dummy score
                    entity.text,
                    entity.type,
                    entity.cid,
                )


class BeCalmTSVFormatter(_BeCalmFormatter):
    '''
    BeCalm's TSV format for the TIPS challenge.
    '''
    ext = 'tsv'
    template = '{}\t{}\t{}\t{}\t{}\t{}\t{}\n'

    def write(self, stream, content):
        if self.config.p.include_header:
            stream.write(self.template.format(*(f.upper()
                                                for f in self.fields)))
        for entry in self._iter_entries(content):
            stream.write(self.template.format(*entry))


class BeCalmJSONFormatter(_BeCalmFormatter):
    '''
    BeCalm's JSON format for the TIPS challenge.
    '''
    ext = 'json'

    def write(self, stream, content):
        stream.write('[\n')
        need_comma = False  # comma needed before all but the first entry
        for entry in self._iter_entries(content):
            if need_comma:
                stream.write(',\n')
            else:
                need_comma = True
            json.dump(dict(zip(self.fields, entry)), stream, indent=4)
        stream.write('\n]')
