#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Formatter for the BeCalm TIPS TSV and JSON formats.
'''


import json

from .document import Section, Entity
from .export import StreamFormatter


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
                    Entity.TYPE(entity),
                    Entity.ID(entity),
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
