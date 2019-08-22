#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Nico Colic, June 2015


'''
Canonical interface for basic NLP tasks.
'''


import ast
import pickle

import nltk


class Text_processing(object):
    """
    Allows to do tokenisation and PoS tagging on a given text.

    For now, needs manual downloading in NLTK of tokenizers/
    punkt and maxent_treebank_pos_tagger before it works.

    Structure of tokens:
        [0]: token,
        [1]: start position,
        [2]: end position

    Structure of tagged tokens: same as tokens, and [3] is tag
    """

    WORD_TOKENIZERS = (
        'RegexpTokenizer',
        'RegexTokenizer',
        'TreebankTokenizer',
        'WordPunctTokenizer',
    )
    SENT_TOKENIZERS = (
        'PunktSentenceTokenizer',
    )

    def __init__(self, word_tokenizer, sentence_tokenizer):
        self.word_tokenizer = self._load_tokenizer(
            word_tokenizer, self.WORD_TOKENIZERS)
        self.sentence_tokenizer = self._load_tokenizer(
            sentence_tokenizer, self.SENT_TOKENIZERS)

    @staticmethod
    def _load_tokenizer(name, targets):
        """
        Load a tokenizer from NLTK or from a pickle.
        """
        if name is None:
            return None

        if name.startswith(targets):  # NLTK
            # Look for constructor arguments as part of the name.
            try:
                i = name.index('(')
            except ValueError:
                args = ()
            else:
                name, args = name[:i], name[i:]
                args = ast.literal_eval(args)
                if not isinstance(args, tuple):  # single argument
                    args = (args,)
                if name == 'RegexTokenizer':  # backwards compatibility
                    name = 'RegexpTokenizer'

            from nltk import tokenize
            cls = getattr(tokenize, name)
            return cls(*args)

        else:  # pickled object
            with open(name, 'rb') as f:
                return pickle.load(f)

    def span_tokenize_sentences(self, text, offset=0):
        """
        Iterate over sentence triples.

        Sentence triples:
            [0] sentence text,
            [1] begin position,
            [2] end position
        """
        # Use a trick to get spans that always include trailing whitespace.
        # Iterate over bigrams of spans, ie.
        #   <start_n, end_n>, <start_n+1, end_n+1>
        # and then use <start_n, start_n+1> as the span for n.
        # To get the last sentence right, use padding.
        spans = self.sentence_tokenizer.span_tokenize(text)
        spans = nltk.bigrams(spans, pad_right=True,
                             right_pad_symbol=(len(text), None))
        for (start, _), (end, _) in spans:
            yield text[start:end], start+offset, end+offset

    def tokenize_sentences(self, text):
        """Iterate over sentences, including trailing whitespace."""
        for sent, _, _ in self.span_tokenize_sentences(text):
            yield sent

    def span_tokenize_words(self, text, offset=0):
        """
        Iterate over token triples.

        Token triples:
            [0] token text,
            [1] begin position,
            [2] end position
        """
        for start, end in self.word_tokenizer.span_tokenize(text):
            yield text[start:end], start+offset, end+offset

    def tokenize_words(self, text):
        """Iterate over word tokens."""
        return self.word_tokenizer.tokenize(text)

    @staticmethod
    def pos_tag(span_tokens):
        """
        Takes as input token triples with position information,
        and returns a list of quadruples:
            [0] token,
            [1] start position,
            [2] end position,
            [4] pos-tag
        """

        # nltk.pos_tag() takes as argument a list of tokens, so we need to get
        # rid of positions first, then pos-tag, then reconcile with position
        # information
        tokens = [span_token[0] for span_token in span_tokens]

        tagged_tokens = nltk.pos_tag(tokens)

        # reconcile with position information
        span_tagged_tokens = [(tok, start, end, tag)
                              for (tok, start, end), (_, tag)
                              in zip(span_tokens, tagged_tokens)]

        return span_tagged_tokens
