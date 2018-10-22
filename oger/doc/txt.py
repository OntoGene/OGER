#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Loader for plain-text input.
'''


import os
import io
import json

from .document import Article
from .load import DocLoader, DocIterator
from ..util.stream import text_stream


class _TXTLoaderMixin:
    '''
    Base loader for plain-text documents.
    '''
    def _document(self, stream, docid):
        if self.config.p.single_section:
            # All text in a single section.
            sections = [self._reattach_blank(stream)]
        else:
            # Sections are separated by blank lines.
            sections = []
            for line in self._reattach_blank(stream, signal_boundaries=True):
                if line is None:
                    # Start a new section.
                    sections.append([])
                else:
                    sections[-1].append(line)

        if docid is None:
            # Resort to using the filename as an ID, if available.
            # (The stream might have an empty or no "name" attribute.)
            path = getattr(stream, 'name', None) or 'unknown'
            docid = os.path.splitext(os.path.basename(path))[0]

        article = Article(docid, tokenizer=self.config.text_processor)
        for text in sections:
            if not self.config.p.sentence_split:
                text = ''.join(text)
            article.add_section('', text)

        return article

    @staticmethod
    def _reattach_blank(lines, signal_boundaries=False):
        '''
        Reattach blank lines to the preceding non-blank line.

        Initial blank lines are prepended to the first non-
        blank line.

        If signal_boundaries is True, the position of the blank
        lines is signaled through yielding None.
        This boundary is always signaled at the beginning, even
        if there are no leading blank lines.
        '''
        # Consume all lines until the first non-blank line was read.
        last = ''
        for line in lines:
            last += line
            if line.strip():
                break

        # Unless the input sequence is empty, the first signal is now due.
        if signal_boundaries and last:
            yield None

        # Continue with the rest of the lines.
        # The loop variable is always ahead of the yielded value.
        boundary = False
        for line in lines:
            if not line.strip():
                # Blank line. Don't yield anything, but set a flag for yielding
                # the signal after the current line was yielded.
                boundary = True
                last += line
            else:
                # Non-blank line. Yield what was accumulated.
                yield last
                last = line
                if signal_boundaries and boundary:
                    yield None
                    boundary = False

        # Unless the input sequence was empty, the last line is now due.
        if last:
            yield last


class TXTLoader(DocLoader, _TXTLoaderMixin):
    '''
    Loader for single plain-text documents.
    '''
    def document(self, source, id_):
        '''
        Get a very simply structured article.
        '''
        with text_stream(source) as f:
            return self._document(f, id_)


class TXTJSONLoader(DocIterator, _TXTLoaderMixin):
    '''
    Loader for multiple plain-text documents embedded in JSON.
    '''
    def iter_documents(self, source):
        with text_stream(source) as f:
            docs = json.load(f)

        for doc in docs:
            stream = io.StringIO(doc['text'])
            id_ = doc['id']
            yield self._document(stream, id_)
