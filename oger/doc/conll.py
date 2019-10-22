#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2019


'''
Loader and formatter for CoNLL format.

The format details are based on Sampo Pyysalo's converter:
https://github.com/spyysalo/standoff2conll
'''


__all__ = ['CoNLLLoader', 'CoNLLFormatter']


import csv
import itertools as it
from collections import deque

from .document import Article, Entity
from .load import DocIterator
from .export import StreamFormatter
from ..util.misc import tsv_format
from ..util.iterate import context_coroutine
from ..util.stream import text_stream


# Lookup constants.
I, O, B, E, S = range(5)
TAGSETS = {
    'IOBES': tuple('IOBES'),
    'IOB':   tuple('IOBIB'),
    'IO':    tuple('IOIII'),
}
OUTSIDE = ('O', 'O-NIL')
INSIDE = ('I', 'E')
BEGIN = ('B', 'S')


class CoNLLLoader(DocIterator):
    """Read verticalized, annotated text."""

    def iter_documents(self, source):
        with text_stream(source) as f:
            rows = csv.reader(f, **tsv_format)
            yield from self._iter_documents(rows)

    def _iter_documents(self, rows):
        ids = it.count(1)
        for docid, doc_rows in it.groupby(rows, _DocIDTracker()):
            if docid is not _DocIDTracker.DocumentSeparator:
                yield self._document(docid, doc_rows, ids)

    def _document(self, docid, rows, ids):
        article = Article(docid, tokenizer=self.config.text_processor)
        sentences = self._iter_sentences(rows, ids)
        try:
            title = next(sentences)
        except StopIteration:
            pass  # empty document
        else:
            self._add_section(article, 'title', [title])
            self._add_section(article, 'body', sentences)
        return article

    @staticmethod
    def _add_section(article, type_, sentences):
        article.add_section(type_, ())
        section = article[-1]
        for sentence, entities in sentences:
            section.add_sentences((sentence,))
            section[-1].entities.extend(entities)

    def _iter_sentences(self, rows, ids):
        for non_blank, sent_rows in it.groupby(rows, key=any):
            if non_blank:
                yield self._sentence(sent_rows, ids)

    def _sentence(self, rows, ids):
        text, labels = [], []
        first_start, last_end = None, None
        last_tag = OUTSIDE[0], None
        for token, start, end, tag, *_ in rows:
            start, end = int(start), int(end)
            if len(token) != end-start:
                raise ValueError(
                    'length mismatch: {} ({}..{})'.format(token, start, end))

            if last_end is None:
                first_start = start
            elif start > last_end:
                text.append(' ' * (start-last_end))
            text.append(token)
            last_end = end

            tag, label = last_tag = fix_tag(tag, last_tag)
            if tag in BEGIN:
                labels.append([(label, start, end)])
            elif tag in INSIDE:
                labels[-1].append(end)

        sentence = (''.join(text), first_start, last_end)
        entities = self._make_entities(labels, sentence[0], first_start, ids)
        return sentence, entities

    def _make_entities(self, labels, text, offset, ids):
        for annotation in labels:
            cid, start, end = annotation[0]
            if len(annotation) > 1:
                end = annotation[-1]
            term = text[start-offset:end-offset]
            yield self._entity(next(ids), term, start, end, cid)

    def _entity(self, id_, term, start, end, cid):
        info = ['unknown'] * len(self.config.entity_fields)
        info[3] = cid
        return Entity(id_, term, start, end, tuple(info))


def fix_tag(tag, last):
    """Ensure a valid IOB sequence."""
    if tag in OUTSIDE:
        tag = OUTSIDE[0]
        label = None
    else:
        # Definitely use B or I, including something like "O-chemical".
        tag, label = tag.split('-', 1)
        if tag in BEGIN or last[0] in OUTSIDE or last[1] != label:
            tag = BEGIN[0]
        else:
            tag = INSIDE[0]
    return tag, label


class _DocIDTracker:
    """Helper class for tracking IDs with it.groupby()."""

    DocumentSeparator = object()

    def __init__(self):
        self.docid = None

    def __call__(self, row):
        if row and row[0].startswith('# doc_id = '):
            self.docid = row[0].split('=', 1)[1].strip()
            return self.DocumentSeparator
        return self.docid


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
