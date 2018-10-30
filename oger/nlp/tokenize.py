#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Nico Colic, June 2015


'''
Canonical interface for basic NLP tasks.
'''


import os.path
import re
import ast
import pickle

import nltk
from lxml import etree as ET


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

    def __init__(self, word_tokenizer, sentence_tokenizer):
        self.word_tokenizer = self._create_word_tokenizer(word_tokenizer)
        self.sentence_tokenizer = self._create_sentence_tokenizer(sentence_tokenizer)

    @staticmethod
    def _create_word_tokenizer(name):
        """
        Here you can add supported word tokenizers.

        Note that it must implement the span_tokenize method.
        """
        if name == 'WordPunctTokenizer':
            from nltk.tokenize import WordPunctTokenizer
            return WordPunctTokenizer()

        elif name == 'PunktWordTokenizer':
            from nltk.tokenize import PunktWordTokenizer
            return PunktWordTokenizer()

        elif name.startswith('RegexTokenizer'):
            # name is a Python expression for constructing a RegexTokenizer,
            # eg. "RegexTokenizer(r'\w+|[^\W\S]+')\n".
            # Strip off the class name and parse the argument.
            arg = ast.literal_eval(name[len('RegexTokenizer'):])
            return RegexTokenizer(arg)

        else:
            raise ValueError('Unknown word tokenizer: {}'.format(name))

    @staticmethod
    def _create_sentence_tokenizer(name):
        """Here you can add supported sentence tokenizers."""
        if name == 'PunktSentenceTokenizer':
            from nltk.tokenize import PunktSentenceTokenizer
            return PunktSentenceTokenizer()

        else:
            # Try to open a pickled sentence tokenizer.
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
        return self.sentence_tokenizer.tokenize(text)

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
        return self.word_tokenizer.tokenize(text)

    @staticmethod
    def flatify(tokens_per_sentence):
        for sentence in tokens_per_sentence:
            for token in sentence:
                yield token

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

    @staticmethod
    def export_tokens_to_xml(id_, tokens_per_sentence, output_directory):
        root = ET.Element("root")
        for sentence_number, sentence in enumerate(tokens_per_sentence):
            S = ET.SubElement(root, "S")
            S.set('i', str(sentence_number))

            for word in sentence:
                W = ET.SubElement(S, "W")
                W.text = word[0]

                # Create the o1 and o2 attributes for the starting and ending
                # position of the word
                W.set('end', str(word[2]))
                W.set('begin', str(word[1]))

        # prepare printing
        directory = os.path.join(output_directory, 'text_processing')
        os.makedirs(directory, exist_ok=True)
        file_name = os.path.join(directory, id_ + '.xml')

        # write out with pretty_print
        with open(file_name, 'wb') as f:
            f.write(ET.tostring(root, method='xml', encoding="UTF-8",
                                xml_declaration=True, pretty_print=True))


class RegexTokenizer(object):
    '''
    Wrapper around re.findall()/re.finditer().
    '''

    def __init__(self, pattern):
        self.token = re.compile(pattern)

    def tokenize(self, text):
        '''
        Split `text` into a list of tokens.
        '''
        return self.token.findall(text)

    def span_tokenize(self, text):
        '''
        Iterate over pairs <start offset, end offset>.
        '''
        for m in self.token.finditer(text):
            yield m.start(), m.end()
