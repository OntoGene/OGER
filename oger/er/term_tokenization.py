#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Tokenization for biomedical terms.
'''


import re


class TermTokenizer(object):
    """
    A tokenizer for matching biomedical terms.

    The tokenization is lossy: The output contains only
    alphabetical and numerical tokens; punctuation symbols
    are removed.
    """

    # A token is a sequence of either numerical or alphabetical characters.
    token = r'\d+|[^\W\d_]+'
    # For acronym detection, a single parenthesis also forms a token.
    abbrev_token = r'\d+|[^\W\d_]+|[()]'

    def __init__(self, token=None, abbrev_detection=False):
        if token is None:
            token = self.abbrev_token if abbrev_detection else self.token
        self.pattern = re.compile(token)

    def tokenize_words(self, text):
        '''
        Split `text` into a list of tokens.
        '''
        return self.pattern.findall(text)

    def span_tokenize_words(self, text):
        '''
        Iterate over triples (token, start offset, end offset).
        '''
        for m in self.pattern.finditer(text):
            yield m.group(), m.start(), m.end()
