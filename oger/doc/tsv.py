#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Formatter for TSV output (with/without context).
'''


import csv

from .document import Sentence
from .export import StreamFormatter
from ..util.iterate import CacheOneIter
from ..util.misc import tsv_format


class TSVFormatter(StreamFormatter):
    '''
    Compact TSV format for annotations and optional context.
    '''
    ext = 'tsv'

    def __init__(self, config, fmt_name):
        super().__init__(config, fmt_name)
        self.all_tokens = fmt_name == 'text_tsv'
        self.extra_fields = self.config.entity_fields[5:]
        self.extra_dummy = ('',) * len(self.extra_fields)

    def write(self, stream, content):
        writer = csv.writer(stream, **tsv_format)

        if self.config.p.include_header:
            self._write_header(writer)
        self._write_body(writer, content)

    def _write_header(self, writer):
        headers = ('DOCUMENT ID',
                   'TYPE',
                   'START POSITION',
                   'END POSITION',
                   'MATCHED TERM',
                   'PREFERRED FORM',
                   'ENTITY ID',
                   'ZONE',
                   'SENTENCE ID',
                   'ORIGIN',
                   'UMLS CUI')
        headers += self.extra_fields
        writer.writerow(headers)

    def _write_body(self, writer, content):
        for article in content.get_subelements('article', include_self=True):
            self._write_article(writer, article)

    def _write_article(self, writer, article):
        # For each token, find all recognized entities starting here.
        # Write a fully-fledged TSV line for each entity.
        # In all_tokens mode, also add sparse lines for non-entity tokens.
        if self.all_tokens:
            # A clever iterator that yields the intermediate tokens' lines.
            interlines = self._tsv_interlines
            # Make sure all sentences are tokenized.
            for sentence in article.get_subelements(Sentence):
                sentence.tokenize()
        else:
            # A dummy that always produces an empty sequence.
            interlines = lambda *_: ()

        for i, sentence in enumerate(article.get_subelements(Sentence), 1):
            # Use an ad-hoc counter for continuous sentence IDs.
            sent_id = 'S{}'.format(i)
            ids = article.id_, sent_id
            toks = CacheOneIter(sentence)
            section_type = sentence.get_section_type(default='')
            last_end = 0  # offset history

            for entity in sentence.iter_entities():
                # Add sparse lines for all tokens preceding the current entity.
                for row in interlines(last_end, entity.start, toks, ids):
                    writer.writerow(row)
                # Add a rich line for each entity (possibly multiple lines
                # for the same token(s)).
                writer.writerow((article.id_,
                                 entity.type,
                                 entity.start,
                                 entity.end,
                                 entity.text,
                                 entity.pref,
                                 entity.cid,
                                 section_type,
                                 sent_id,
                                 entity.db,
                                 entity.cui)
                                + entity.extra)
                last_end = max(last_end, entity.end)
            # Add sparse lines for the remaining tokens.
            for row in interlines(last_end, float('inf'), toks, ids):
                writer.writerow(row)

    def _tsv_interlines(self, start, end, tokens, ids):
        '''
        Iterate over tokens within the offset window start..end.
        '''
        if start >= end:
            # The window has length 0 (or less).
            return

        article_id, sent_id = ids
        for token in tokens:
            if token.start >= end:
                # The token has left the window.
                tokens.repeat()  # rewind the iterator
                break
            if token.end > start:
                # The token is (at least partially) inside the window.
                yield (article_id,
                       '',
                       token.start,
                       token.end,
                       token.text,
                       '',
                       '',
                       '',
                       sent_id,
                       '',
                       '') + self.extra_dummy
