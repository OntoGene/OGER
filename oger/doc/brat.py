#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Formatter for the brat stand-off format.
'''


import re
import logging
from collections import defaultdict

from .document import Collection, Section
from .export import StreamFormatter


class BratFormatter:
    '''
    Distributor for delegating to the actual formatters.
    '''
    def __init__(self, config, fmt_name):
        # Brat needs two output files.
        # Create a subformatter for each.
        self.txt = BratTxtFormatter(config, fmt_name)
        self.ann = BratAnnFormatter(config, fmt_name)

    def export(self, content):
        '''
        Write two disk files.
        '''
        self.txt.export(content)
        self.ann.export(content)

    def write(self, stream, content):
        '''
        Write text and annotations to the same stream.
        '''
        logging.warning('writing brat text and annotations to the same file')
        self.txt.write(stream, content)
        self.ann.write(stream, content)

    def dump(self, content):
        '''
        Export to a pair <str, str> of text and annotations.
        '''
        return (self.txt.dump(content),
                self.ann.dump(content))


class BratTxtFormatter(StreamFormatter):
    '''
    Plain text, on which brat's stand-off annotations are based.
    '''
    ext = 'txt'

    @staticmethod
    def write(stream, content):
        for s in content.get_subelements(Section):
            stream.write(s.text)


class BratAnnFormatter(StreamFormatter):
    '''
    Stand-off annotations for brat.
    '''
    ext = 'ann'
    _fieldname_pattern = re.compile(r'\W+')

    def write(self, stream, content):
        if isinstance(content, Collection):
            self._write_collection(stream, content)
        else:
            self._write_article(stream, content)

    def _write_collection(self, stream, coll):
        counters = (0, 0, 0)
        for article in coll:
            counters = self._ann_counters(stream, article, counters=counters)

    def _write_article(self, stream, article):
        self._ann_counters(stream, article)

    def _ann_counters(self, stream, article, counters=(0, 0, 0)):
        '''
        Write article-level annotations and capture continuous IDs.
        '''
        t, n, a = counters
        mentions = self._get_mentions(article)
        for t, (loc_type, entities) in enumerate(sorted(mentions.items()), t+1):
            stream.write('T{0}\t{3} {1} {2}\t{4}\n'.format(t, *loc_type))
            for n, e in enumerate(entities, n+1):
                # Add all remaining information as "AnnotatorNotes".
                info = '\t'.join(e.info[1:])
                stream.write('#{}\tAnnotatorNotes T{}\t{}\n'.format(n, t, info))
                for att, atype in self.config.p.brat_attributes:
                    value = getattr(e.info, att)
                    if value:
                        a += 1
                        stream.write(self._attribute(atype, att, value, a, t))
        return t, n, a

    def _get_mentions(self, article):
        mentions = defaultdict(list)
        for e in article.iter_entities():
            name = self._valid_fieldname(e.type)
            mentions[e.start, e.end, name, e.text].append(e)
        return mentions

    @classmethod
    def _valid_fieldname(cls, name):
        return cls._fieldname_pattern.sub('_', name)

    @staticmethod
    def _attribute(multivalue, key, value, n_a, n_t):
        if multivalue:
            # Multi-valued attributes.
            return 'A{}\t{} T{} {}\n'.format(n_a, key, n_t, value)
        else:
            # Binary attributes.
            return 'A{}\t{} T{}\n'.format(n_a, value, n_t)
