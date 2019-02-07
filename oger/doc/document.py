#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Nico Colic, September 2015
# Modified by Lenz Furrer, 2015--2018


"""
Representation of text and annotations.

The hierarchy is fixed:
    Collection
      Article
        Section
          Sentence
            Token (optional)

Matched entities are anchored at the sentence level.
Word-level tokenization can be skipped.
"""


import pickle
import itertools as it
from collections import namedtuple

from ..util.iterate import peekaheaditer


class Unit(object):
    """
    Base class for all levels of representation.
    """

    def __init__(self, id_):
        self.subelements = []
        self.id_ = id_
        self._metadata = None

    @property
    def metadata(self):
        '''Metadata imported from input documents.'''
        if self._metadata is None:
            self._metadata = {}
        return self._metadata

    @metadata.setter
    def metadata(self, value):
        self._metadata = value

    def __repr__(self):
        name = self.__class__.__name__
        elems = len(self.subelements)
        plural = '' if elems == 1 else 's'
        address = hex(id(self))
        return ('<{} with {} subelement{} at {}>'
                .format(name, elems, plural, address))

    def __iter__(self):
        return iter(self.subelements)

    def __getitem__(self, index):
        return self.subelements[index]

    def add_subelement(self, subelement):
        '''
        Check for type consistency before adding.
        '''
        # Checks that all elements in the subelements list are of the same type
        if self.subelements:
            if type(self.subelements[0]) != type(subelement):
                raise TypeError(
                    "Subelements list may only contain objects of same type")

        self.subelements.append(subelement)

    def get_subelements(self, subelement_type, include_self=False):
        """
        Iterate over subelements at any subordinate level.

        If include_self is True, retrieval starts at the
        root level already.

        Example use:
            my_article.get_subelements("sentence")
        for a flat iterator over all sentences of an article.
        """
        if isinstance(subelement_type, str):
            try:
                subelement_type = dict(
                    collection=Collection,
                    article=Article,
                    section=Section,
                    sentence=Sentence,
                    token=Token,
                )[subelement_type.lower()]
            except KeyError:
                raise TypeError('unknown subelement_type: {}'
                                .format(subelement_type))

        if include_self and isinstance(self, subelement_type):
            # The root level matches.
            return iter([self])

        if not self.subelements:
            # No subelements -- nothing to return.
            return iter([])

        if isinstance(self.subelements[0], subelement_type):
            # The first sub-level matches.
            return iter(self.subelements)

        else:
            # Recursively descend into sub-subelements.
            return (subsub
                    for sub in self.subelements
                    for subsub in sub.get_subelements(subelement_type))

    def iter_entities(self):
        '''
        Iterate over all entities, ordered by start offset.

        This method is defined for all units (at every level).
        '''
        for sentence in self.get_subelements(Sentence, include_self=True):
            for entity in sentence.iter_entities():
                yield entity


class Exporter(Unit):
    '''
    Base class for exportable units (Collection and Article).
    '''

    def __init__(self, id_, basename=None):
        super().__init__(id_)
        self.basename = basename

    @property
    def text(self):
        '''
        Plain text form for inspection and Brat output.
        '''
        return ''.join(self.iter_text())

    def iter_text(self):
        '''
        Iterate over all text segments, including separators.

        Separator whitespace is reconstructed from offsets,
        using "\n" between sections and " " between sentences.
        '''
        offset = 0
        for section in self.get_subelements(Section):
            if offset < section.start:
                # Insert space that was removed between sections.
                yield '\n' * (section.start-offset)
            yield from section.iter_text()
            offset = section.end

    def recognize_entities(self, entity_recognizer):
        '''
        Delegate entity recognition to the sentence unit.
        '''
        previous_ids = (int(e.id_) for e in self.iter_entities()
                        if isinstance(e.id_, int) or e.id_.isdigit())
        start_id = max(previous_ids, default=0) + 1
        ids = it.count(start_id)
        for article in self.get_subelements(Article, include_self=True):
            entity_recognizer.reset()
            for sentence in article.get_subelements(Sentence):
                sentence.recognize_entities(entity_recognizer, ids)

    def pickle(self, output_filename):
        '''
        Dump a pickle of this unit.
        '''
        with open(output_filename, 'wb') as f:
            pickle.dump(self, f)

    @classmethod
    def unpickle(cls, input_filename):
        """Use as x = Exporter.unpickle(path)"""
        with open(input_filename, 'rb') as f:
            return pickle.load(f)


class Collection(Exporter):
    """A collection of multiple articles."""
    @classmethod
    def from_iterable(cls, iterable, id_, basename=None):
        '''
        Construct a collection from an iterable of articles.
        '''
        coll = cls(id_, basename)
        for article in iterable:
            coll.add_article(article)
        return coll

    def add_article(self, article):
        '''
        Add an article (must be an Article object).
        '''
        if not isinstance(article, Article):
            raise TypeError(
                'Invalid type: {}, expected Article'.format(type(article)))
        self.add_subelement(article)


class Article(Exporter):
    '''An article with text, metadata and annotations.'''
    def __init__(self, id_, basename=None, tokenizer=None):
        super().__init__(id_, basename)
        # The tokenizer is used for sentence splitting and word tokenization.
        # Depending on input and output format, it may or may not be needed.
        self.tokenizer = tokenizer

        self.type_ = None
        self.year = None
        self._char_cursor = 0

    def add_section(self, section_type, text, offset=None):
        '''
        Append a section to the end.

        The text can be either a str or an iterable of str.
        '''
        id_ = len(self.subelements)
        if offset is None:
            offset = self._char_cursor
        section = Section(id_, section_type, text, self, offset)
        self.add_subelement(section)
        self._char_cursor = section.end


class Section(Unit):
    """Any unit of text between document and sentence level."""

    def __init__(self, id_, section_type, text, article, start=0):
        '''
        A section (eg. title, abstract, mesh list).

        The text can be a single string or a list of
        strings (sentences).
        '''
        super().__init__(id_)

        self.type_ = section_type
        self.article = article
        self._text = None

        if isinstance(text, str):
            # Single string element.
            self.add_sentences(
                self.article.tokenizer.span_tokenize_sentences(text, start))
            self._text = text
            # Use the length of the plain text avoids problems with
            # any trailing whitespace, which is reported inconsistently
            # by the Punkt sentence splitter.
            end = start + len(text)
        else:
            # Iterable of strings or <string, offset...> tuples.
            self.add_sentences(self._guess_offsets(text, start))
            try:
                # Do not rely on the `start` default argument.
                # If there were offsets in the sentences iterable,
                # they should have precedence.
                start = self.subelements[0].start
                end = self.subelements[-1].end
            except IndexError:
                # No text in this section.
                # Keep the start argument and set the length to 0.
                end = start

        # Character offsets:
        self.start = start
        self.end = end

    @property
    def text(self):
        '''
        Plain text form for inspection and Brat output.
        '''
        if self._text is None:
            self._text = ''.join(self.iter_text())
        return self._text

    def iter_text(self):
        '''
        Iterate over sentence text and blanks.
        '''
        offset = self.start
        for sent in self.subelements:
            if offset < sent.start:
                # Insert space that was removed in sentence splitting.
                yield ' ' * (sent.start-offset)
            yield sent.text
            offset = sent.end
        # Check for trailing whitespace.
        if offset < self.end:
            yield ' ' * (self.end-offset)

    @staticmethod
    def _guess_offsets(sentences, offset):
        '''
        Inspect the first elem to see if offsets are provided.
        If not, try to substitute them.
        '''
        sentences = peekaheaditer(iter(sentences))
        try:
            peek = next(sentences)
        except StopIteration:
            # Empty iterable.
            return

        if isinstance(peek, str):
            # Substitute the offsets.
            for sent in sentences:
                yield sent, offset
                offset += len(sent)
        else:
            # Propagate the sentence/offset tuples.
            yield from sentences

    def add_sentences(self, sentences):
        '''
        Add a sequence of sentences with start offsets.
        '''
        first_id = len(self.subelements)
        for id_, (sent, *span) in enumerate(sentences, first_id):
            self.add_subelement(Sentence(id_, sent, self, *span))


class Sentence(Unit):
    '''
    Central annotation unit.
    '''
    def __init__(self, id_, text, section=None, start=0, end=None):
        super().__init__(id_)
        self.text = text
        self.section = section
        self.entities = []
        # Character offsets:
        self.start = start
        self.end = end if end is not None else start + len(text)

    def tokenize(self):
        '''
        Word-tokenize this sentence.
        '''
        if not self.subelements and self.text:
            tokenizer = self.section.article.tokenizer
            toks = tokenizer.span_tokenize_words(self.text, self.start)
            for id_, (token, start, end) in enumerate(toks):
                self.add_subelement(Token(id_, token, start, end))

    def recognize_entities(self, entity_recognizer, ids=None):
        '''
        Run entity recognition and sort the results by offsets.
        '''
        if ids is None:
            ids = it.count()
        entities = entity_recognizer.recognize_entities(self.text)
        prev_len = len(self.entities)
        for ((start, end), info), id_ in zip(entities, ids):
            surface = self.text[start:end]
            entity = Entity(id_,
                            surface,
                            start+self.start,
                            end+self.start,
                            info)
            self.entities.append(entity)

        if prev_len and len(self.entities) > prev_len:
            # If the new annotations weren't the first ones, then they need
            # to be sorted in.
            self.entities.sort(key=Entity.sort_key)

    def iter_entities(self):
        '''
        Iterate over all entities, sorted by occurrence.
        '''
        yield from self.entities

    def get_section_type(self, default=None):
        '''
        Get the type of the superordinate section (if present).
        '''
        try:
            return self.section.type_
        except AttributeError:
            return default


# The token-level unit really has no functionality.
Token = namedtuple('Token', 'id_ text start end')


class Entity(object):
    '''
    Link from textual evidence to a concept identifier.
    '''
    __slots__ = ('id_', 'text', 'start', 'end', 'info', 'fields')

    # Default fields defined by the termlist.
    std_fields = ('type', 'preferred_form',
                  'original_resource', 'native_id', 'umls_cui')

    def __init__(self, id_, text, start, end, info):
        self.id_ = id_
        self.text = text
        self.start = start
        self.end = end
        self.info = info

    # Accessor methods for the standard fields:

    @property
    def type(self):
        'Entity-type field.'
        return self.info[0]

    @property
    def pref(self):
        'Preferred-form field.'
        return self.info[1]

    @property
    def db(self):
        'Original-resource field.'
        return self.info[2]

    @property
    def cid(self):
        'Concept-ID field (defined by DB).'
        return self.info[3]

    @property
    def cui(self):
        'UMLS CUI field.'
        return self.info[4]

    @property
    def extra(self):
        'Any additional fields.'
        return self.info[5:]

    @classmethod
    def map_fields(cls, extra, renaming):
        '''
        Extend and rename the default fields, if necessary.
        '''
        return tuple(renaming.get(name, name)
                     for name in it.chain(cls.std_fields, extra))

    def info_items(self, fields=None):
        '''
        Iterate over label-value pairs of entity.info.
        '''
        if fields is None:
            fields = self.std_fields
        for label, value in zip(fields, self.info):
            yield label, value

    @classmethod
    def sort(cls, entities):
        '''
        Sort a list of Entity instances by offsets, in-place.
        '''
        entities.sort(key=cls.sort_key)

    @staticmethod
    def sort_key(entity):
        '''
        Sort entities by offset.
        '''
        return entity.start, entity.end
