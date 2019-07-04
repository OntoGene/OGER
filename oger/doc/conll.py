#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2019


'''
Formatter for CoNLL format.

The format details are based on Sampo Pyysalo's converter:
https://github.com/spyysalo/standoff2conll
'''


__all__ = ['CoNLLFormatter']


import csv
from collections import deque

from .export import StreamFormatter
from ..util.misc import tsv_format
from ..util.iterate import context_coroutine


# Lookup constants.
I, O, B, E, S = range(5)
TAGSETS = {
    'IOBES': tuple('IOBES'),
    'IOB':   tuple('IOBIB'),
    'IO':    tuple('IOIII'),
}


class CoNLLFormatter(StreamFormatter):
    """Tab-separated verticalized text with annotations."""

    ext = 'conll'

    def __init__(self, config, fmt_name):
        super().__init__(config, fmt_name)

        self.tagset = TAGSETS[self.config.p.conll_tagset]

        options = dict.fromkeys(self.config.p.conll_include, True)
        self.include_docid = options.pop('docid', False)
        self.include_offsets = options.pop('offsets', False)
        if options:
            raise ValueError('unknown CoNLL option(s): {}'.format(list(options)))

    def tag(self, tag_id, label=None):
        """Map a general tag ID to the tagset-specific tag."""
        tag = self.tagset[tag_id]
        if label:
            tag = '{}-{}'.format(tag, label)
        return tag

    def write(self, stream, content):
        writer = csv.writer(stream, **tsv_format)

        for article in content.get_subelements('article', include_self=True):
            writer.writerows(self._article(article))

    def _article(self, article):
        if self.include_docid:
            yield ['# doc_id = {}'.format(article.id_)]
        for sentence in article.get_subelements('sentence'):
            yield from self._sentence(sentence)
            yield ()  # blank line separating sentences

    def _sentence(self, sentence):
        sentence.tokenize()
        labels = self._sequence_labels(sentence)
        for token, label in zip(sentence, labels):
            if self.include_offsets:
                yield token.text, token.start, token.end, label
            else:
                yield token.text, label

    def _sequence_labels(self, sentence):
        labels = ['', *self._tokenwise_labels(sentence), '']  # padding!
        for prev, current, next_ in zip(labels, labels[1:], labels[2:]):
            if not current:
                yield self.tag(O)           # outside
            elif prev == current == next_:
                yield self.tag(I, current)  # inside
            elif prev == current:
                yield self.tag(E, current)  # end
            elif current == next_:
                yield self.tag(B, current)  # beginning
            else:
                yield self.tag(S, current)  # single

    def _tokenwise_labels(self, sentence):
        with self._entities_by_pos(sentence.entities) as entities:
            for token in sentence:
                label = ';'.join(e.cid for e in entities.send(token))
                yield label

    @context_coroutine
    def _entities_by_pos(self, entities):
        """Coroutine for sequentially querying entities by position."""
        in_scope = deque()
        token = (yield None)
        for entity in entities:
            # Wait for the token to catch up.
            while token.end <= entity.start:
                token = (yield in_scope)
                # New target token: remove entities that are now out of scope.
                self._check_scope(token, in_scope)
            # Include the current entity if it overlaps with the target token.
            if max(entity.start, token.start) < min(entity.end, token.end):
                in_scope.append(entity)
        # The entities are exhausted, but the last batch still needs yielding.
        # After that, continue yielding empty batches forever.
        while True:
            token = (yield in_scope)
            self._check_scope(token, in_scope)

    @staticmethod
    def _check_scope(token, entities):
        for _ in range(len(entities)):
            if entities[0].end <= token.start:
                entities.popleft()   # remove first elem
            else:
                entities.rotate(-1)  # place first elem at the end
