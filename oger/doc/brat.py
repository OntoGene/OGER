#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Formatter for the brat stand-off format.
'''


__all__ = ['DualFormatter',
           'TXTFormatter', 'BratAnnFormatter', 'BioNLPAnnFormatter']


import re
import logging
import itertools as it
from collections import defaultdict

from .export import StreamFormatter


class DualFormatter:
    '''
    Distributor for delegating to the actual formatters.
    '''
    def __init__(self, config, fmt_name):
        # Brat and BioNLP need two output files.
        # Create a subformatter for each.
        self.txt = TXTFormatter(config, fmt_name)
        self.ann = AnnFormatter(config, fmt_name)

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
        logging.warning('writing %s text and annotations to the same file',
                        self.txt.fmt_name)
        self.txt.write(stream, content)
        self.ann.write(stream, content)

    def dump(self, content):
        '''
        Export to a pair <str, str> of text and annotations.
        '''
        return (self.txt.dump(content),
                self.ann.dump(content))


class TXTFormatter(StreamFormatter):
    '''
    Plain text, on which the stand-off annotations are based.
    '''
    ext = 'txt'

    @staticmethod
    def write(stream, content):
        stream.writelines(content.iter_text())


def AnnFormatter(config, fmt_name):
    """Wrapper for brat and BioNLP constructors."""
    if fmt_name == 'brat':
        formatter = BratAnnFormatter
    elif fmt_name == 'bionlp':
        formatter = BioNLPAnnFormatter
    else:
        raise ValueError('unknown format: {}'.format(fmt_name))
    return formatter(config, fmt_name)


class BratAnnFormatter(StreamFormatter):
    '''
    Stand-off annotations for brat.
    '''
    ext = 'ann'
    _fieldname_pattern = re.compile(r'\W+')

    def __init__(self, config, fmt_name):
        super().__init__(config, fmt_name)
        self.attributes = list(self._attribute_indices(self.config))

    @staticmethod
    def _attribute_indices(config):
        for att, atype in config.p.brat_attributes:
            try:
                index = config.entity_fields.index(att)
            except ValueError:
                raise LookupError(
                    'brat attribute: unknown entity field: {}'.format(att))
            yield index, att, atype

    def write(self, stream, content):
        counters = [it.count(1) for _ in range(3)]
        for article in content.get_subelements('article', include_self=True):
            self._write_anno(stream, article, counters)

    def _write_anno(self, stream, article, counters):
        '''
        Write article-level annotations with continuous IDs.
        '''
        c_t, c_n, c_a = counters
        mentions = self._get_mentions(article)
        for (loc_type, entities), t in zip(sorted(mentions.items()), c_t):
            stream.write('T{0}\t{3} {1} {2}\t{4}\n'.format(t, *loc_type))
            for e, n in zip(entities, c_n):
                # Add all remaining information as "AnnotatorNotes".
                self._write_anno_note(stream, e, t, n, c_a)

    def _write_anno_note(self, stream, entity, t, n, c_a):
        info = '\t'.join(entity.info[1:])
        stream.write('#{}\tAnnotatorNotes T{}\t{}\n'.format(n, t, info))
        for i, att, atype in self.attributes:
            value = entity.info[i]
            if value:
                stream.write(self._attribute(atype, att, value, next(c_a), t))

    def _get_mentions(self, article):
        mentions = defaultdict(list)
        for e in article.iter_entities():
            name = self._valid_fieldname(e.type)
            mentions[e.start, e.end, name, e.text_wn].append(e)
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


class BioNLPAnnFormatter(StreamFormatter):
    '''
    Stand-off annotations for BioNLP.
    '''
    ext = 'ann'
    template = 'T{counter}\t{e.cid} {e.start} {e.end}\t{e.text_wn}\n'

    def write(self, stream, content):
        counter = it.count(1)
        for article in content.get_subelements('article', include_self=True):
            self._write_anno(stream, article, counter)

    def _write_anno(self, stream, article, counter):
        '''
        Write article-level annotations with continuous IDs.
        '''
        for entity, t in zip(article.iter_entities(), counter):
            stream.write(self.template.format(counter=t, e=entity))
