#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Nico Colic, September 2015
# Modified by Lenz Furrer, December 2015 -- May 2016


"""
Used for inter-module communication.

Stores articles in various stages of processing.

The hierarchy is fixed:
    Collection
      Article
        Section
          Sentence
            Token
Matched entities are anchored at the sentence level.
Word-level tokenization can be skipped.
"""


import re
import csv
import json
import pickle
from collections import defaultdict, namedtuple

from lxml import etree as ET
from lxml.builder import E

from ..util.iterate import CacheOneIter, peekaheaditer


# Number of standard annotation fields.
STDFLD = 6


class Unit(object):
    """Abstract unit which implements functions used by all structures."""

    def __init__(self, id_):
        '''
        Mainly initialises subelements list.
        '''
        self.subelements = list()
        self.id_ = id_
        self._metadata = None

    @property
    def metadata(self):
        'Metadata imported from input documents.'
        if self._metadata is None:
            self._metadata = {}
        return self._metadata

    @metadata.setter
    def metadata(self, value):
        self._metadata = value

    def __str__(self):
        '''
        Used so you can print(my_article), for example
        Useful for inspection (but use xml() etc for true outputting)
        '''
        string = ''
        if not self.subelements:
            return self.text

        for subelement in self.subelements:
            if hasattr(subelement, 'text'):
                if subelement.text is not None:
                    string += subelement.text + ' '
            else:
                string += repr(subelement)

        # get rid of final \n
        return string.rstrip('\r\n')

    def __repr__(self):
        name = self.__class__.__name__
        elems = len(self.subelements)
        plural = '' if elems == 1 else 's'
        address = hex(id(self))
        return ('<{} with {} subelement{} at {}>'
                .format(name, elems, plural, address))

    def __iter__(self):
        return iter(self.subelements)

    def add_subelement(self, subelement):
        '''
        Check for type consistency and ID before adding.
        '''
        # Checks that all elements in the subelements list are of the same type
        if self.subelements:
            if type(self.subelements[0]) != type(subelement):
                raise TypeError(
                    "Subelements list may only contain objects of same type")

        # if id_ in subelement is set, we use it
        # otherwise we create a new id_
        if subelement.id_ is None:
            # subelement has not yet been added to the subelements list
            subelement.id_ = len(self.subelements)

        self.subelements.append(subelement)

    def get_subelements(self, subelement_type):
        """
        Iterate over subelements at any subordinate level.

        Example use:
            my_article.get_sublements(article.Token)
        from outside, or
            my_article.get_subelements(Token)
        from within the module
        for a flat iterator over all of the article's
        tokens.
        """
        if not self.subelements:
            return iter([])

        if isinstance(self.subelements[0], subelement_type):
            return iter(self.subelements)

        else:
            return (subsub
                    for sub in self.subelements
                    for subsub in sub.get_subelements(subelement_type))

    def iter_entities(self):
        '''
        Iterate over all entities, ordered by start offset.

        This method is defined for all units (at every level).
        However, it will only find entities if a descendant
        of type Sentence can be reached.
        '''
        for sentence in self.get_subelements(Sentence):
            for entity in sentence.iter_entities():
                yield entity

    def xml(self):
        '''
        Export text and metadata to XML.
        '''
        raise NotImplementedError()

    def bioc(self, **flags):
        '''
        Export text, metadata and entities to BioC XML nodes.
        '''
        raise NotImplementedError()

    @staticmethod
    def bioc_infon(node, key, value):
        '''
        Add an <infons> element.
        '''
        node.append(E('infon', value, key=key))


class Exporter(Unit):
    """Abstract class for exportable units."""

    # The tokenizers are needed at different stages of processing.
    # Set this attribute to a textprocessing object before using this class.
    tokenizer = None
    # The non-standard fields need to be known by the exporter methods.
    extra_fields = ()

    def __init__(self, id_, basename=None):
        super().__init__(id_)
        self.basename = basename

    def recognize_entities(self, entity_recognizer):
        '''
        Delegate entity recognition to the sentence unit.
        '''
        entity_recognizer.reset()
        id_ = 1
        for sentence in self.get_subelements(Sentence):
            id_ = sentence.recognize_entities(entity_recognizer, id_)

    def write_tsv_legacy_format(self, w_file, include_header=True):
        '''
        TSV with all recognized entities.
        '''
        self.write_tsv(w_file, include_header, all_tokens=False)

    def write_text_tsv_format(self, w_file, include_header=True):
        '''
        TSV with all tokens (not only entities).
        '''
        self.write_tsv(w_file, include_header, all_tokens=True)

    def write_tsv(self, w_file, include_header, all_tokens=False):
        '''
        Write a TSV document for entities and (optionally) other text.
        '''
        raise NotImplementedError()

    BeCalmFields = ('document_id', 'section', 'init', 'end', 'score',
                    'annotated_text', 'type', 'database_id')
    BeCalmTSV = '{}\t{}\t{}\t{}\t{}\t{}\t{}\n'

    def write_becalm_tsv(self, w_file, include_header):
        '''
        Write entities according to BeCalm's TSV format.
        '''
        if include_header:
            w_file.write(self.BeCalmTSV.format(*(f.upper()
                                                 for f in self.BeCalmFields)))
        for entry in self._becalm_entries():
            w_file.write(self.BeCalmTSV.format(*entry))

    def write_becalm_json(self, w_file):
        '''
        Write entities according to BeCalm's JSON format.
        '''
        w_file.write('[\n')
        need_comma = False  # comma needed before all but the first entry
        for entry in self._becalm_entries():
            if need_comma:
                w_file.write(',\n')
            else:
                need_comma = True
            json.dump(dict(zip(self.BeCalmFields, entry)), w_file, indent=4)
        w_file.write('\n]')

    def _becalm_entries(self):
        '''
        Iterate over entries needed for BeCalm's output formats.
        '''
        for section in self.get_subelements(Section):
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
                    entity.extra.type,
                    entity.extra.original_id
                )

    def write_xml(self, wb_file):
        '''
        Serialise the text to XML (no entities).
        '''
        self._write_xml(self.xml(), wb_file)

    def write_entities_xml(self, wb_file):
        '''
        Serialise the recognised entities to XML (no text).
        '''
        self._write_xml(self.entities_xml(), wb_file)

    @staticmethod
    def _write_xml(node, wb_file):
        '''
        Write an XML node to a file.
        '''
        wb_file.write(ET.tostring(node, encoding="UTF-8",
                                  xml_declaration=True,
                                  pretty_print=True))

    def entities_xml(self):
        '''
        Export the recognized entities to XML.
        '''
        raise NotImplementedError()

    BioC_Doctype = '<!DOCTYPE collection SYSTEM "BioC.dtd">'

    def write_bioc(self, wb_file, **flags):
        '''
        Write a collection in BioC format.
        '''
        for chunk in self.bioc_iter_bytes(**flags):
            wb_file.write(chunk)

    def bioc_iter_bytes(self, **flags):
        '''
        Iterate over fragments of serialised BioC bytes.
        '''
        return iter([self.bioc_bytes(**flags)])

    def bioc_bytes(self, **flags):
        '''
        Serialise BioC XML.
        '''
        raise NotImplementedError()

    def write_odin(self, wb_file):
        '''
        Write to file ODIN's XML format.
        '''
        self._write_xml(self.odin(), wb_file)

    def odin(self):
        '''
        Reformat to ODIN's XML.
        '''
        raise NotImplementedError()

    def write_brat_txt(self, w_file):
        '''
        Write text in Brat format.
        '''
        w_file.write(self.brat_txt())

    def write_brat_ann(self, w_file, **flags):
        '''
        Write stand-off annotations in Brat format.
        '''
        w_file.write(self.brat_ann(**flags))

    def brat_txt(self):
        '''
        Export text in Brat format.
        '''
        raise NotImplementedError()

    def brat_ann(self, **flags):
        '''
        Export stand-off annotations in Brat format.

        The annotations are currently restricted to a fixed
        set including only annotation ID (not concept ID!),
        concept type, start/end offset and surface string.
        '''
        raise NotImplementedError()

    def pickle(self, output_filename):
        '''
        Dump a pickle of this unit.
        '''
        with open(output_filename, 'wb') as f:
            pickle.dump(self, f)

    @classmethod
    def unpickle(cls, input_filename):
        """Use as x = article.Exporter.unpickle(path)"""
        with open(input_filename, 'rb') as f:
            return pickle.load(f)


###################################
# actual classes begin here #######
###################################

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

    def recognize_entities(self, entity_recognizer):
        # Override the inherited method to clear the cache for each document.
        id_ = 1
        for article in self.subelements:
            entity_recognizer.reset()
            for sentence in article.get_subelements(Sentence):
                id_ = sentence.recognize_entities(entity_recognizer, id_)

    def xml(self):
        '''
        Export text and metadata to XML.
        '''
        coll = E('collection',
                 id=str(self.id_))

        for article in self.subelements:
            coll.append(article.xml())

        return coll

    def entities_xml(self):
        '''
        Export the recognized entities to XML.
        '''
        entities = E('entities',
                     collection_id=str(self.id_))

        for entity in self.iter_entities():
            entities.append(Entity.xml(entity))

        return entities

    def bioc(self, meta=None, **flags):
        if meta is None:
            meta = self.metadata

        coll = E('collection',
                 E('source', meta.get('source', '')),
                 E('date', meta.get('date', '')),
                 E('key', meta.get('key', '')))

        for k, v in meta.get('infon', {}).items():
            self.bioc_infon(coll, k, v)

        for article in self.subelements:
            coll.append(article.bioc(**flags))

        return coll

    def bioc_bytes(self, **flags):
        return ET.tostring(self.bioc(**flags),
                           encoding="UTF-8",
                           xml_declaration=True,
                           doctype=self.BioC_Doctype,
                           pretty_print=True)

    def bioc_iter_bytes(self, meta=None, **flags):
        '''
        Iterate over serialised BioC fragments.
        '''
        try:
            # Temporarily remove the documents.
            documents = self.subelements
            self.subelements = []

            # Serialise the outer shell and split off the closing tag.
            shell = self.bioc_bytes(meta=meta, **flags)
            tail = '</collection>\n'.encode('UTF-8')
            head = shell[:-len(tail)]

            # Yield fragment by fragment.
            yield head

            for doc in documents:
                frag = ET.tostring(doc.bioc(**flags),
                                   encoding='UTF-8',
                                   pretty_print=True,
                                   xml_declaration=False)
                yield frag

            yield tail

        finally:
            # Put the documents back in place.
            self.subelements = documents

    def odin(self):
        node = E('collection')
        sent_id, tok_id = 0, 0  # continuous IDs
        for article in self.subelements:
            art_node, sent_id, tok_id = article.odin_counters(sent_id, tok_id)
            node.append(art_node)
        return node

    def brat_txt(self):
        return ''.join(a.brat_txt() for a in self.subelements)

    def brat_ann(self, **flags):
        counters = (0, 0, 0)
        annotations = []
        for a in self.subelements:
            ann, counters = a.brat_ann_counters(**flags, counters=counters)
            annotations.append(ann)
        return ''.join(annotations)

    def write_tsv(self, w_file, include_header, all_tokens=False):
        for article in self.subelements:
            article.write_tsv(w_file, include_header, all_tokens=all_tokens)
            include_header = False  # no repeated header in subsequent articles


class Article(Exporter):
    '''An article with text, metadata and annotations.'''
    def __init__(self, id_, basename=None):
        super().__init__(id_, basename)

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

    def xml(self):
        '''
        Export text and metadata to XML.
        '''
        article = E('article',
                    id=self.id_)

        if self.year is not None:
            article.set('year', self.year)
        if self.type_ is not None:
            article.set('type', self.type_)

        for subelement in self.subelements:
            article.append(subelement.xml())

        return article

    def entities_xml(self):
        entities = E('entities',
                     article_id=str(self.id_))

        for entity in self.iter_entities():
            entities.append(Entity.xml(entity))

        return entities

    def bioc(self, **flags):
        '''
        Create a BioC document node of this article.
        '''
        doc = E('document',
                E('id', self.id_))

        if self.year is not None:
            self.bioc_infon(doc, 'year', self.year)
        if self.type_ is not None:
            self.bioc_infon(doc, 'type', self.type_)
        for key, value in self.metadata.items():
            self.bioc_infon(doc, key, value)

        for section in self.subelements:
            doc.append(section.bioc(**flags))

        return doc

    def bioc_bytes(self, **flags):
        """
        Serialise as a collection with a single document.
        """
        # Wrap this document in a collection.
        coll = Collection(self.id_, self.basename)
        coll.add_article(self)
        return coll.bioc_bytes(**flags)

    def odin(self):
        node, *_ = self.odin_counters()
        return node

    def odin_counters(self, sent_id=0, tok_id=0):
        '''
        ODIN XML node and continuous IDs.
        '''
        node = E('article', id=str(self.id_))

        # Add sections.
        for section in self.subelements:
            sec_node, sent_id, tok_id = section.odin(sent_id, tok_id)
            node.append(sec_node)

        # Add the (redundant?) stand-off annotations (concept level).
        anno = ET.SubElement(node, 'og-dict')
        seen = set()
        for entity in self.iter_entities():
            if entity.extra.original_id not in seen:
                anno.append(E('og-dict-entry',
                              cid=str(entity.extra.original_id),
                              cname=entity.extra.preferred_form,
                              type=entity.extra.type))
                seen.add(entity.extra.original_id)

        return node, sent_id, tok_id

    def brat_txt(self):
        return ''.join(s.text for s in self.subelements)

    def brat_ann(self, attributes=()):
        ann, _ = self.brat_ann_counters(attributes)
        return ann

    def brat_ann_counters(self, attributes=(), counters=(0, 0, 0)):
        '''
        Brat annotations and continuous IDs.
        '''
        mentions = defaultdict(list)
        for e in self.iter_entities():
            name = self._valid_brat_fieldname(e.extra.type)
            mentions[e.start, e.end, name, e.text].append(e)
        annotations = []
        t, n, a = counters
        for t, (loc_type, entities) in enumerate(sorted(mentions.items()), t+1):
            annotations.append('T{0}\t{3} {1} {2}\t{4}\n'
                               .format(t, *loc_type))
            for n, e in enumerate(entities, n+1):
                # Add all remaining information as "AnnotatorNotes".
                extra = '\t'.join(e.extra[2:])
                annotations.append('#{}\tAnnotatorNotes T{}\t{}\n'
                                   .format(n, t, extra))
                for att, atype in attributes:
                    value = getattr(e.extra, att)
                    if value:
                        a += 1
                        annotations.append(
                            self._brat_attribute(atype, att, value, a, t))
        return ''.join(annotations), (t, n, a)

    @staticmethod
    def _brat_attribute(multivalue, key, value, n_a, n_t):
        if multivalue:
            # Multi-valued attributes.
            return 'A{}\t{} T{} {}\n'.format(n_a, key, n_t, value)
        else:
            # Binary attributes.
            return 'A{}\t{} T{}\n'.format(n_a, value, n_t)

    brat_fieldname = re.compile(r'\W+')

    @classmethod
    def _valid_brat_fieldname(cls, name):
        return cls.brat_fieldname.sub('_', name)

    def write_tsv(self, w_file, include_header, all_tokens=False):
        writer = csv.writer(w_file, delimiter='\t', lineterminator='\n')
        if include_header:
            headers = ('DOCUMENT ID',
                       'TYPE',
                       'START POSITION',
                       'END POSITION',
                       'MATCHED TERM',
                       'PREFERRED FORM',
                       'ENTITY ID',
                       'ZONE',
                       'SENTENCE ID',
                       'ORIGIN')
            headers += self.extra_fields
            writer.writerow(headers)
        x = ('',) * len(self.extra_fields)

        # For each token, find all recognized entities starting here.
        # Write a fully-fledged TSV line for each entity.
        # In all_tokens mode, also add sparse lines for non-entity tokens.
        if all_tokens:
            # A clever iterator that yields the intermediate tokens' lines.
            interlines = self._tsv_interlines
            for sentence in self.get_subelements(Sentence):
                sentence.tokenize()
        else:
            # A dummy that always produces an empty sequence.
            interlines = lambda *_: ()

        for i, sentence in enumerate(self.get_subelements(Sentence), 1):
            # Use an ad-hoc counter for continuous sentence IDs.
            sent_id = 'S{}'.format(i)
            toks = CacheOneIter(sentence)
            section_type = sentence.get_section_type(default='')
            last_end = 0  # offset history

            for entity in sentence.iter_entities():
                # Add sparse lines for all tokens preceding the current entity.
                for row in interlines(last_end, entity.start, toks, sent_id, x):
                    writer.writerow(row)
                # Add a rich line for each entity (possibly multiple lines
                # for the same token(s)).
                writer.writerow((self.id_,
                                 entity.extra.type,
                                 entity.start,
                                 entity.end,
                                 entity.text,
                                 entity.extra.preferred_form,
                                 entity.extra.original_id,
                                 section_type,
                                 sent_id,
                                 entity.extra.original_resource)
                                + entity.extra[STDFLD:])
                last_end = max(last_end, entity.end)
            # Add sparse lines for the remaining tokens.
            for row in interlines(last_end, float('inf'), toks, sent_id, x):
                writer.writerow(row)

    def _tsv_interlines(self, start, end, tokens, sent_id, extra):
        '''
        Iterate over tokens within the offset window start..end.
        '''
        if start >= end:
            # The window has length 0 (or less).
            return
        for token in tokens:
            if token.start >= end:
                # The token has left the window.
                tokens.repeat()  # rewind the iterator
                break
            if token.end > start:
                # The token is (at least partially) inside the window.
                yield (self.id_,
                       '',
                       token.start,
                       token.end,
                       token.text,
                       '',
                       '',
                       '',
                       sent_id,
                       '') + extra


class Section(Unit):
    """Can be something like title or abstract"""

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
                Exporter.tokenizer.span_tokenize_sentences(text, start))
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
            self._text = ''
            offset = self.start
            for sent in self.subelements:
                if offset < sent.start:
                    # Insert space that was removed in sentence splitting.
                    self._text += ' ' * (sent.start-offset-1) + '\n'
                    offset = sent.start
                self._text += sent.text
                offset += len(sent.text)
        return self._text

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
        for id_, (sent, offset, *_) in enumerate(sentences, first_id):
            self.add_subelement(
                Sentence(id_, sent, section=self, start=offset))

    def xml(self):
        section = E('section',
                    id=str(self.id_),
                    type=self.type_)

        for subelement in self.subelements:
            section.append(subelement.xml())

        return section

    def bioc(self, sentence_level=False):
        passage = E('passage')
        if self.type_ is not None:
            self.bioc_infon(passage, 'type', self.type_)
        for key, value in self.metadata.items():
            self.bioc_infon(passage, key, value)
        passage.append(E('offset', str(self.start)))

        # BioC allows text at sentence or passage level.
        # The annotations are anchored at the same level.
        if sentence_level:
            for sentence in self.subelements:
                passage.append(sentence.bioc())
        else:
            passage.append(E('text', self.text))
            for entity in self.iter_entities():
                passage.append(Entity.bioc(entity))

        return passage

    odin_sections = {
        'title': 'article-title',
        'abstract': 'abstract',
        'mesh descriptor names': 'mesh',
    }

    def odin(self, sent_id, tok_id):
        '''
        ODIN format: Create an XML node and update the continuous IDs.
        '''
        tag = self.odin_sections.get(self.type_.lower(), 'section')
        node = E(tag, id=str(self.id_))
        for sent_id, sentence in enumerate(self.subelements, sent_id+1):
            sent_node, tok_id = sentence.odin(self.start, sent_id, tok_id)
            node.append(sent_node)
        return node, sent_id, tok_id


class Sentence(Unit):
    '''
    Central annotation unit.
    '''
    def __init__(self, id_, text, section=None, start=0):
        super().__init__(id_)
        self.text = text
        self.section = section
        self.entities = []
        # Character offsets:
        self.start = start
        self.end = start + len(text)

    def tokenize(self):
        '''
        Word-tokenize this sentence.
        '''
        if not self.subelements and self.text:
            toks = Exporter.tokenizer.span_tokenize_words(self.text, self.start)
            for id_, (token, start, end) in enumerate(toks):
                self.add_subelement(Token(id_, token, start, end))

    def recognize_entities(self, entity_recognizer, start_id=0):
        '''
        Run entity recognition and sort the results by offsets.
        '''
        entities = entity_recognizer.recognize_entities(self.text)
        nonempty = bool(self.entities)
        for id_, match in enumerate(entities, start_id):
            start, end = match.position
            surface = self.text[start:end]
            entity = EntityTuple(id_,
                                 surface,
                                 start+self.start,
                                 end+self.start,
                                 match)
            self.entities.append(entity)
        try:
            final_id = id_ + 1
        except NameError:
            # Undefined loop variable -- no entity found.
            final_id = start_id
        else:
            # If the new annotations weren't the first ones, then they need
            # to be sorted in.
            if nonempty:
                Entity.sort(self.entities)
        return final_id

    def xml(self):
        sentence = E('sentence',
                     id=str(self.id_))

        self.tokenize()

        for token in self.subelements:
            sentence.append(token.xml())

        return sentence

    def bioc(self):
        sentence = E('sentence')
        for key, value in self.metadata.items():
            self.bioc_infon(sentence, key, value)
        sentence.append(E('offset', str(self.start)))
        sentence.append(E('text', self.text))

        for entity in self.iter_entities():
            sentence.append(Entity.bioc(entity))

        return sentence

    def odin(self, section_offset, sent_id, tok_id):
        '''
        ODIN format: Create an XML node and update the continuous token ID.
        '''
        sent_node = E('S', i=str(self.id_), id='S{}'.format(sent_id))

        self.tokenize()

        intertoks = self._odin_itertoks(tok_id, section_offset)
        mentions = self._odin_itermentions(section_offset)
        try:
            tok_id, token, tok_node = next(intertoks)
            for term_start, term_end, term_node in mentions:
                while token.end <= term_start:
                    # Tokens preceding this term mention are added directly
                    # to the sentence node.
                    sent_node.append(tok_node)
                    tok_id, token, tok_node = next(intertoks)
                # Tokens belonging to the term mention are added
                # to the mention's node.
                sent_node.append(term_node)
                while token.start < term_end:
                    term_node.append(tok_node)
                    tok_id, token, tok_node = next(intertoks)
                # Move the tail spacing from the last token to the term node.
                if len(term_node):
                    # In some corner cases, term_node has no children.
                    term_node.tail = term_node[-1].tail
                    term_node[-1].tail = None
                # Add the ID attribute ex post
                # (now that the token IDs are known).
                tok_ids = (t.get('id') for t in term_node)
                term_node.set('id', 'T_{}'.format('_'.join(tok_ids)))
            sent_node.append(tok_node)
        except StopIteration:
            # Tokens exhausted inside a term mention
            # (or there is no text in this sentence).
            pass
        # Append any remaining tokens.
        for tok_id, _, tok_node in intertoks:  # redefine tok_id: up-reporting
            sent_node.append(tok_node)

        return sent_node, tok_id

    def _odin_itertoks(self, tok_id, section_offset):
        '''
        Iterate over tokens with proper spacing (tail text).
        '''
        # We don't know if we need to append a space until we see the next
        # token. Therefore, yielding is delayed by one iteration.
        itoks = enumerate(self.subelements, tok_id+1)
        try:  # respect PEP 479
            this_id, this_token = next(itoks)
        except StopIteration:
            return

        for next_id, next_token in itoks:
            node = this_token.odin(this_id, section_offset)
            if next_token.start > this_token.end:
                # Non-adjacent tokens -> add a space.
                node.tail = ' '
            yield this_id, this_token, node
            this_id, this_token = next_id, next_token
        yield this_id, this_token, this_token.odin(this_id, section_offset)

    def _odin_itermentions(self, offset):
        '''
        Lump together overlapping term spans.
        '''
        last_end = 0
        mention = []
        for entity in self.iter_entities():
            # Rely on the position-based sort order of the term mentions.
            if entity.start < last_end:
                # Overlapping term.
                mention.append(entity)
            else:
                # Disjunct term.
                if mention:
                    yield self._odin_term_node(mention, offset)
                mention = [entity]
            last_end = max(entity.end, last_end)
        if mention:
            yield self._odin_term_node(mention, offset)

    @staticmethod
    def _odin_term_node(entities, offset):
        '''
        Produce a term node for co-located entity mentions.
        '''
        start = min(e.start for e in entities)
        end = max(e.end for e in entities)
        values = '|'.join('{}:{}:{}'.format(e.extra.original_id,
                                            e.extra.type,
                                            e.text)
                          for e in entities)
        type_ = '|'.join(set(e.extra.type for e in entities))
        node = E('Term', allvalues=values, type=type_,
                 o1=str(start-offset), o2=str(end-offset))
        return start, end, node

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


class Token(Unit):
    """The most basic unit."""

    def __init__(self, id_, text, start, end):
        super().__init__(id_)
        self.text = text
        self.start = start
        self.end = end
        self.length = end - start

    def xml(self):
        token = E('token',
                  id=str(self.id_),
                  start=str(self.start),
                  end=str(self.end),
                  length=str(self.length))
        token.text = self.text
        return token

    def odin(self, cont_id, section_offset):
        '''
        ODIN format: offsets restart at each section.
        '''
        node = E('W', self.text,
                 id='W{}'.format(cont_id),
                 o1=str(self.start - section_offset),
                 o2=str(self.end - section_offset))
        return node

    def get_tuple(self):
        '''
        Get the triple <token, start offset, end offset>.
        '''
        return (self.text, self.start, self.end)


EntityTuple = namedtuple('EntityTuple', 'id_ text start end extra')


class Entity(object):
    '''
    Link from textual evidence to a concept identifier.

    Do not instantiate this class;
    instead, use its class methods with a namedtuple
    as first argument.
    '''
    @classmethod
    def xml(cls, entity):
        'XML representation.'
        node = E('entity',
                 id=str(entity.id_),
                 start=str(entity.start),
                 end=str(entity.end))

        for label, value in cls._items(entity.extra):
            node.set(label, value)

        node.text = entity.text

        return node

    @classmethod
    def bioc(cls, entity):
        'BioC XML representation.'
        annotation = E('annotation', id=str(entity.id_))

        for label, value in cls._items(entity.extra):
            Unit.bioc_infon(annotation, label, value)

        annotation.append(
            E('location',
              offset=str(entity.start),
              length=str(entity.end - entity.start)))

        annotation.append(E('text', entity.text))

        return annotation

    @classmethod
    def _items(cls, extra):
        '''
        Iterate over label-value pairs of entity.extra.
        '''
        for label, value in extra._asdict().items():
            # Generate "infons" entries for almost all fields
            # found in the term list.
            if label not in ('position', 'ontogene_id'):
                yield label, value

    @classmethod
    def sort(cls, entities):
        '''
        Sort a list of entity tuples by offsets, in-place.
        '''
        entities.sort(key=cls._sort_key)

    @staticmethod
    def _sort_key(entity):
        return entity.start, entity.end
