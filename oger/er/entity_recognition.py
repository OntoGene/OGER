#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Nico Colic, September 2015
# Modified by Lenz Furrer, 2015--2016


'''
Entity Recognition core.
'''


import re
import csv
import pickle
import os.path
import logging

from ..ctrl import parameters
from ..util import misc, stream
from . import term_normalization as normalization
from .term_tokenization import TermTokenizer


class EntityRecognizer(object):
    """
    Dictionary-based entity recognition.
    """

    def __init__(self, config=parameters.ERParams(), **kwargs):
        """
        Loads the terms from file or pickle.

        `term_token` is a regular expression pattern defining
        a token for constructing a term tokenizer.
        It does not have to be the same tokenizer that
        is used to tokenize the text in the article, since
        the entity recognizer does not rely on that
        tokenization.

        `cache` is the default folder in which
        we check for cached pickle files. A cached file has
        the same basename as the term list, plus ".pickle".
        If `force_reload` is set, it will load from file in
        any case. Use this when the term list has changed.
        When loading from file, the term list will be pickled
        automatically for faster (up to 20 times)
        loading in subsequent calls.

        `stopwords` is either an iterable of stopwords
        or a path to a list of stopwords (one per line).
        """
        self.tokenizer = TermTokenizer(config.term_token,
                                       config.abbrev_detection)
        self._normalizers = normalization.load(config.normalize)
        self.stopwords = self.import_stopwords(config.stopwords)
        self.term_first, self.full_terms = self.load_termlist(config, **kwargs)

    @classmethod
    def ensure_cache(cls, *args, **kwargs):
        '''
        Make sure there is a pickled version of the termlist.
        '''
        # Simply create a throw-away instance with the hidden (undocumented)
        # kwarg `skip_loading`, which makes the constructor look for the
        # pickle file, but doesn't load it.
        kwargs['skip_loading'] = True
        cls(*args, **kwargs)

    def import_stopwords(self, stopwords):
        '''
        Resolve the different ways the stopwords are provided.
        '''
        if isinstance(stopwords, str):
            # stopwords is a path:
            with open(stopwords) as f:
                stopwords = [l.strip() for l in f]
        # Any False-equivalent value is interpreted as no stopwords.
        stopwords = stopwords or []
        # The stopwords are saved and looked up in normalized form.
        return frozenset(self.normalize(self.tokenizer.tokenize_words(w))
                         for w in stopwords)

    def load_termlist(self, config, skip_loading=False):
        '''
        Check for a pickle, or else create one.

        After reading the term list into a dictionary,
        it has the following internal structure:
            key: first token of the term
            value: tuple(
              [0] = whole term,
              [1] = term_type (or category),
              [2] = term_preferred_form,
              [3] = resource of origin
              [4] = native ID (in the respective database),
              [5] = UMLS CUI
            )
        If additional fields were defined through `extra_fields`,
        then the value tuple is extended correspondingly.
        '''
        # Check if pickle with the same file name exists.
        if config.path is None:
            raise ValueError('no termlist specified')
        if config.cache is None:
            config.cache = os.path.dirname(config.path)
        basename = os.path.basename(config.path)
        pickle_file = os.path.join(config.cache, basename + '.pickle')
        n_fields = 5 + config.n_extra  # 5 std fields besides the term
        if os.path.exists(pickle_file) and not config.force_reload:
            if skip_loading:
                # Optimisation feature:
                # Only check for a pickle, but don't load it.
                terms = None, None
            else:
                terms = self.load_termlist_from_pickle(pickle_file, n_fields)

        # Load the termlist from file.
        else:
            try:
                parser = getattr(
                    self, 'termlist_format_{}'.format(config.field_format))
            except AttributeError:
                logging.error('No such termlist format: %s',
                              config.field_format)
                raise ValueError('Invalid termlist format')
            terms = self.load_termlist_from_file(config, parser, n_fields)
            try:
                self.write_terms_to_pickle(terms, pickle_file)
            except OSError as e:
                logging.warning('Cannot write termlist pickle: %s (%r)',
                                pickle_file, e)
        return terms

    @staticmethod
    def load_termlist_from_pickle(pickle_path, n_exp):
        '''
        Perform a shallow format check before loading.
        '''
        logging.info('Unpickling terms from %s', pickle_path)

        with open(pickle_path, 'rb') as f:
            terms = pickle.load(f)
        try:
            # Make sure we have the right format.
            term_first, full_terms = terms
        except ValueError:
            logging.exception(
                'Termlist pickle in obsolete format: %s\n  '
                'Delete the pickle file or run with force_reload=True.',
                pickle_path)
            raise
        try:
            n_found = len(next(iter(full_terms.values()))[0])
        except StopIteration:
            logging.warning('unpickling empty termlist')
        else:
            if n_found != n_exp:
                logging.error(
                    'Termlist pickle with wrong number of fields: '
                    'expected %d, found %d\n  '
                    'Pickle file: %s\n  '
                    'Delete the pickle file or run with force_reload=True.',
                    n_exp, n_found, pickle_path)
                raise ValueError('Termlist pickle with unexpected field count')

        logging.info('Terms loaded from pickle.')

        return term_first, full_terms

    @staticmethod
    def write_terms_to_pickle(terms, filename):
        '''
        Dump everything to disk.
        '''
        if filename.startswith(stream.REMOTE_PROTOCOLS):
            raise OSError('Cannot write pickle to remote location')

        os.makedirs(os.path.dirname(filename), exist_ok=True)

        with open(filename, 'wb') as f:
            pickle.dump(terms, f)

        logging.info('Terms written to pickle at %s', filename)

    def load_termlist_from_file(self, config, field_parser, n_fields):
        """
        Index the term DB.

        The terms are indexed by the first token of the term
        expression.
        These keys point to a list of entries.
        """

        logging.info("Loading terms from file %s", config.path)
        term_first, full_terms = {}, {}
        entry = ('',) * n_fields

        with stream.ropen(config.path, encoding='utf-8', newline='') as tsv:
            reader = csv.reader(tsv, escapechar='\\', **misc.tsv_format)
            if config.skip_header:
                next(reader)
            for line_no, line in enumerate(reader, 1+config.skip_header):
                term, std, extra = field_parser(line)

                # Apply text processing to the surface term.
                toks = tuple(self.tokenizer.tokenize_words(term))
                norm = self.normalize(toks)
                term = self.em_filter(norm, toks, None, None)

                try:
                    term_first[norm[0]].add(len(term))
                except KeyError:
                    term_first[norm[0]] = set([len(term)])
                except IndexError:
                    logging.warning(
                        "Skipping line %d: empty term field", line_no)

                entry = self._cached_entry(entry, std + extra)
                if len(entry) != n_fields:
                    logging.error(
                        'Line %d: Wrong field count: %d (expected %d)',
                        line_no, len(entry)+1, n_fields+1)
                    raise ValueError('Unexpected number of TSV fields')

                try:
                    full_terms[term].add(entry)
                except KeyError:
                    full_terms[term] = set([entry])

        # For memory reasons, replace the sets with tuples.
        for k, v in term_first.items():
            # Sort the length indicators, so that we can stop early
            # when reaching the end of a sentence.
            term_first[k] = tuple(sorted(v))
        for k, v in full_terms.items():
            full_terms[k] = tuple(v)

        logging.info("Finished loading termlist.")
        return term_first, full_terms

    @staticmethod
    def termlist_format_4(fields):
        '''
        Legacy format with 4 columns, native ID first.

        [0] ID, [1] term, [2] type, [3] preferred form
        '''
        term = fields[1]
        std = (fields[2], fields[3], 'unknown', fields[0], 'none')
        extra = tuple(fields[4:])
        return term, std, extra

    @staticmethod
    def termlist_format_6(fields):
        '''
        Like the legacy format, but including original DB and UMLS CUI.

        [0] native ID, [1] term, [2] type, [3] preferred form,
        [4] resource from which it comes, [5] UMLS CUI
        '''
        term = fields[1]
        std = (fields[2], fields[3], fields[4], fields[0], fields[5])
        extra = tuple(fields[6:])
        return term, std, extra

    @staticmethod
    def termlist_format_bth(fields):
        '''
        Format produced by the Bio Term Hub (UMLS CUI first).

        [0] UMLS CUI, [1] resource from which it comes,
        [2] native ID, [3] term, [4] preferred form, [5] type
        '''
        term = fields[3]
        std = (fields[5], fields[4], fields[1], fields[2], fields[0])
        extra = tuple(fields[6:])
        return term, std, extra

    @staticmethod
    def _cached_entry(previous, new):
        return tuple(p if p == n else n for p, n in zip(previous, new))

    def _normalize(self, token):
        '''
        Call all normalizer functions in a cascade.
        '''
        for n in self._normalizers:
            token = n(token)
        return token

    def normalize(self, tokens):
        '''
        Normalize a sequence of tokens.
        '''
        return tuple(self._normalize(t) for t in tokens)

    def em_filter(self, norm, exact, start, stop):
        '''
        Enforce exact match for stopwords.
        '''
        norm = norm[start:stop]
        if norm in self.stopwords:
            return exact[start:stop]
        return norm

    def recognize_entities(self, sentence):
        """
        Go through all words and try to match them to the terms.

        A sentence is an un-tokenized string.

        Iterates over the found entities, yielding named tuples:
            [0] position: a pair of offsets (start, end)

            [1] type
            [2] preferred_form
            [3] resource (from which it comes)
            [4] native_id
            [5] umls_cui

            * [3] and [5] are only useful if the termlist_format is 6 or bth.

            If additional fields were defined in the constructor,
            the tuples are extended appropriately.
        """
        span_toks = zip(*self.tokenizer.span_tokenize_words(sentence))
        try:
            toks, starts, ends = span_toks
        except ValueError:
            # No tokens in this sentence: exit early.
            return
        normalized = self.normalize(toks)
        for i, word in enumerate(normalized):
            # There might be multiple entries for the first token in terms:
            for ntoks in self.term_first.get(word, ()):
                j = i+ntoks
                if j > len(normalized):
                    # Not enough tokens remaining: Exit the inner loop early.
                    break
                candidate = self.em_filter(normalized, toks, i, j)
                if candidate in self.full_terms:
                    position = (starts[i], ends[j-1])
                    matches = self.full_terms[candidate]
                    for entry in matches:
                        yield position, entry
                    self._match_hook(matches,
                                     sentence, toks, normalized,
                                     position, i, j)

    # Some placeholder methods used in subclasses.

    @staticmethod
    def _match_hook(*_):
        'Do something with an entity match in context.'

    @staticmethod
    def reset():
        'Reset to initial state.'


class AbbrevDetector(EntityRecognizer):
    '''
    Entity recognizer capable of learning new abbreviations.
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.abbrevs = {}
        self.stopwords = set(self.stopwords)  # make this mutable again

    def _match_hook(self, *args):
        '''
        Check for a subsequent abbreviation definition.
        '''
        matches, _, toks, normalized, _, _, j = args
        if toks[j:j+3:2] == ('(', ')'):
            self.register_abbrev((toks[j+1],), (normalized[j+1],), matches)

    def register_abbrev(self, toks, norm, entries):
        '''
        Add an abbrev to the hash tables and keep track of the changes.
        '''
        mod_stopword, mod_first, mod_full = None, None, None

        # Enforce an exact match for abbreviations.
        if norm not in self.stopwords:
            mod_stopword = norm
            self.stopwords.add(norm)

        # Update the hash tables. There are 3 cases:
        # (1) unchanged, (2) new entry, (3) extend existing entry.
        # First-token hash:
        try:
            backup = self.term_first[norm[0]]
        except KeyError:
            # Case 2.
            mod_first = 'pop'
            self.term_first[norm[0]] = (len(toks),)
        else:
            if len(toks) not in backup:
                # Case 3.
                mod_first = backup
                self.term_first[norm[0]] = tuple(sorted((len(toks),) + backup))
        # Full-term hash:
        try:
            backup = self.full_terms[toks]
        except KeyError:
            # Case 2.
            mod_full = 'pop'
            self.full_terms[toks] = entries
        else:
            union = set(backup).union(entries)
            if len(union) > len(backup):
                # Case 3.
                mod_full = backup
                self.full_terms[toks] = tuple(union)

        # Register the changes.
        self.update_registry(toks, mod_stopword, mod_first, mod_full)

    def update_registry(self, toks, stpw, first, full):
        '''
        Merge the new change signature with any previous.
        '''
        if toks in self.abbrevs:
            p_stpw, p_first, p_full = self.abbrevs[toks]
            stpw = p_stpw or stpw
            first = p_first or first
            full = p_full or full
        self.abbrevs[toks] = (stpw, first, full)

    def clear_abbrev_cache(self):
        'Reset the hash tables for a new document.'
        for toks, (mod_stopword, mod_first, mod_full) in self.abbrevs.items():
            # Undo all the modifications from .register_abbrev().
            if mod_stopword:
                self.stopwords.remove(mod_stopword)
            if mod_first == 'pop':
                self.term_first.pop(toks[0], None)
            elif mod_first:
                self.term_first[toks[0]] = mod_first
            if mod_full == 'pop':
                self.full_terms.pop(toks, None)
            elif mod_full:
                self.full_terms[toks] = mod_full
        self.abbrevs.clear()

    def reset(self):
        'Clear the abbreviation cache.'
        self.clear_abbrev_cache()


class RegexAbbrevDetector(AbbrevDetector):
    '''
    Regex-based, tokenisation-independet abbreviation detector.
    '''
    def __init__(self, *args, abbrevpattern=r'\s+\((\w+)\)', **kwargs):
        super().__init__(*args, **kwargs)
        self.abbrevpattern = re.compile(abbrevpattern)

    def _match_hook(self, *args):
        matches, sentence, _, _, position, _, _ = args
        m = self.abbrevpattern.match(sentence[position[1]:])
        if m:
            toks = tuple(self.tokenizer.tokenize_words(m.group(1)))
            norm = self.normalize(toks)
            self.register_abbrev(toks, norm, matches)

    def recognize_entities(self, sentence):
        for entity in super().recognize_entities(sentence):
            yield entity
