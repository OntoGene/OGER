#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Skeleton of the pipeline.

Make sure all components are connected with each other
according to the parameters.

This module:
- provides iterators for looping through paths/IDs/documents
- defines a class for running the pipeline in server mode
- takes care of feeding the right parameters to the right places

The pipeline is controlled by the following classes, sorted from
highest level of abstraction to lowest:
- PipelineServer: a wrapper around Router.
    Can be used as a server (items are provided at run time one
    by one), or for iterating over a series of items.
- Router: translates configurations into the right calls.
    Provides iterators over pointers (file names or IDs) and
    loaded documents/collections, as well as an export() method
    which is connected to the right export method(s).
- Params (module parameters): merges config from all layers.
    Provides hard-coded defaults for each parameter, which are
    overridden by settings in .ini files, which are again over-
    ridden by keyword-arg parameters.
    Some type-casting is applied, but no memory- or CPU-
    intensive initialisations are performed here.
    The resulting values are accessible as instance attributes.
- ERParams (module parameters): Entity Recognition config.
    Similarly to Params, ER-specific parameters are handled
    and stored on a separate object for each entity recogniser.
'''


import os.path
import glob
import string
import logging
from datetime import datetime

from . import parameters
from ..doc.document import Collection, Entity
from ..doc import EXPORTERS, LOADERS
from ..nlp.tokenize import Text_processing
from ..er.entity_recognition import EntityRecognizer, AbbrevDetector
from ..util.iterate import iter_chunks


class PipelineServer(object):
    '''
    Organise text processing and entity recognition.

    By initialising an object without conf (a Router instance),
    default settings are used.
    '''
    def __init__(self, conf=None, lazy=True):
        self._conf = conf
        if not lazy:
            _ = conf.postfilters
            _ = conf.entity_recognizers

    @property
    def conf(self):
        '''A Router object holding configurations.'''
        if self._conf is None:
            self._conf = Router()  # initialize with default values
        return self._conf

    @property
    def ers(self):
        '''All entity recognizers.'''
        return self.conf.entity_recognizers

    def iter_contents(self, pointers=None):
        'Iterate over input articles/collections.'
        return self.conf.iter_contents(pointers)

    def load_one(self, data, fmt, id_=None, **params):
        'Load a single article/collection from arbitrary format.'
        loader = self._get_loader(fmt, params)
        return loader.load_one(data, id_)

    def iter_load(self, data, fmt, **params):
        'Iterate over articles from arbitrary format.'
        loader = self._get_loader(fmt, params)
        if hasattr(loader, 'iter_documents'):
            yield from loader.iter_documents(data)
        else:
            yield loader.load_one(data, id_=None)

    def _get_loader(self, fmt, params):
        conf = self._param_overload(params)
        return LOADERS[fmt](conf)

    def process(self, content):
        '''Run NER+linking on one article/collection.'''
        for er in self.ers:
            content.recognize_entities(er)

    def postfilter(self, content):
        'Postfilter an article/collection.'
        for pf in self.conf.postfilters:
            pf(content)

    def export(self, content, **params):
        '''Write an article/collection to disk.'''
        conf = self._param_overload(params)
        conf.export(content)

    def write(self, content, fmt, stream, **params):
        '''Write this article/collection to an open file.'''
        exporter = self._get_exporter(fmt, params)
        exporter.write(stream, content)

    def dump(self, content, fmt, **params):
        '''Serialise the article/collection to str or bytes.'''
        exporter = self._get_exporter(fmt, params)
        return exporter.dump(content)

    def _get_exporter(self, fmt, params):
        conf = self._param_overload(params)
        return EXPORTERS[fmt](conf, fmt)

    def _param_overload(self, params):
        '''Create a new Router instance if necessary.'''
        if params:
            # If self._conf is None, forward it anyway.
            return Router(self._conf, **params)
        else:
            return self.conf


class Router(object):
    '''
    Control structure of the pipeline.
    '''
    def __init__(self, config=None, **kwargs):
        '''
        Allowed call signatures:
          - `Router(config)` with a Router, a Params instance, or a mapping
          - `Router(key=value, ...)` with named parameters
          - `Router(config, key=value, ...)` with both

        On conflicts, named parameters take precedence.
        '''
        self.p = self._resolve_call_signature(config, kwargs)

        # Register the entity fields and the exporter methods.
        self.entity_fields = Entity.map_fields(self.p.extra_fields,
                                               self.p.field_names)
        self._exporters = self._get_exporters()

        # External objects with lazy initialisation.
        self._postfilters = None
        self._text_processor = None
        self._entity_recognizers = None

    @staticmethod
    def _resolve_call_signature(config, params):
        if isinstance(config, Router):
            # In case of a Router, extract its Params attribute.
            config = config.p
        if not params and isinstance(config, parameters.Params):
            # Only a Params instance is given, no need to create a new one.
            return config
        elif config is not None:
            # If config is given, merge it with params, just in case.
            # (In case of conflicts, params takes precedence.)
            params = parameters.Params.merged(config, params)
        return parameters.Params(**params)

    # ============== #
    # SETUP methods. #
    # ============== #

    @property
    def postfilters(self):
        '''
        Externally defined postfilters.
        '''
        if self._postfilters is None:
            self._postfilters = self._get_postfilters(self.p.postfilter)
        return self._postfilters

    @property
    def text_processor(self):
        '''
        Get a handler for tokenization, tagging etc.
        '''
        if self._text_processor is None:
            self._text_processor = self._create_text_processor()
        return self._text_processor

    def _create_text_processor(self):
        return Text_processing(self.p.word_tokenizer, self.p.sentence_tokenizer)

    @property
    def entity_recognizers(self):
        '''
        Get all entity recognizer instances.
        '''
        if self._entity_recognizers is None:
            self._entity_recognizers = self._create_entity_recognizers()
        return self._entity_recognizers

    def _create_entity_recognizers(self):
        '''
        Properly instantiate all entity recognizers.
        '''
        constr = (EntityRecognizer, AbbrevDetector)
        return tuple(constr[params.abbrev_detection](params)
                     for params in self.p.recognizers)

    def ensure_cached_termlist(self):
        '''
        Make sure there is a pickled term list for fast loading.
        '''
        for params in self.p.recognizers:
            EntityRecognizer.ensure_cache(params)


    # ================ #
    # INPUT iterators. #
    # ================ #

    def iter_contents(self, pointers=None):
        '''
        Iterate over loaded documents or collections.
        '''
        ctxt = LoadContext(self.p.ignore_load_errors,
                           bool(self.p.fallback_format))
        loader = LOADERS[self.p.article_format](self)

        if self.p.iter_mode == 'collection':
            return self._iter_collections(pointers, ctxt, loader)
        else:
            return self._iter_documents(pointers, ctxt, loader)

    def _iter_collections(self, pointers, ctxt, loader):
        '''
        Iterate over input collections.

        With bioc, pubtator and pxml.gz input, each pointer/path
        is a collection.
        With pubmed/pmc, there is only one collection in total.
        Otherwise, each subdirectory below input_directory
        contains one collection.
        '''
        if self.p.article_format in ('bioc', 'pubtator', 'pubtator_fbk'):
            # Each path node is a collection.
            for path, id_ in self.iter_path_ID(pointers):
                if id_ is None:
                    id_ = os.path.splitext(os.path.basename(path))[0]
                with ctxt.setcurrent(id_):
                    yield loader.collection(path, id_)
        elif self.p.article_format in ('pxml.gz', 'txt_json'):
            # Each path node is a collection.
            for path, id_ in self.iter_path_ID(pointers):
                if id_ is None:
                    id_ = os.path.splitext(os.path.splitext(
                        os.path.basename(path))[0])[0] # split away .xml.gz
                # Avoid repeated pointer parsing: make absolute and wrap.
                path = [os.path.abspath(path)]
                yield self._collection(id_, (path, ctxt, loader))
        elif self.p.article_format in ('pubmed', 'pmc',
                                       'becalmabstracts', 'becalmpatents'):
            # All documents belong to the same collection.
            id_ = 'collection_{:%Y-%m-%d_%H%M%S}'.format(datetime.now())
            yield self._collection(id_, (pointers, ctxt, loader))
        else:
            # Each subdirectory is a collection.
            # If there is no subdirectory, everything is one collection.
            for name, paths in self._iter_subdirs(pointers):
                yield self._collection(name, (paths, ctxt, loader))

        yield from self._handle_missing_files(ctxt.pop())

    def _collection(self, id_, args):
        '''
        Construct a collection from single documents.
        '''
        coll = Collection(id_)
        for article in self._iter_documents(*args):
            coll.add_article(article)
        return coll

    def _iter_documents(self, pointers, ctxt, loader):
        '''
        Iterate over input documents.
        '''
        if self.p.article_format in ('pubmed', 'pmc',
                                     'becalmabstracts', 'becalmpatents'):
            it = self._iter_ids(pointers)  # use IDs, regardless of type
            for chunk in iter_chunks(it, self.p.efetch_max_ids):
                with ctxt.setcurrent():
                    yield from self._check_ids(chunk, loader)
        elif self.p.article_format in ('bioc_xml', 'bioc_json', 'pxml.gz',
                                       'txt_json', 'pubtator', 'pubtator_fbk'):
            for path, id_ in self.iter_path_ID(pointers):
                with ctxt.setcurrent(id_):
                    yield from loader.iter_documents(path)
        else:
            for path, id_ in self.iter_path_ID(pointers):
                with ctxt.setcurrent(id_):
                    article = loader.document(path, id_)
                    article.basename = os.path.splitext(
                        os.path.basename(path))[0]
                    yield article

        yield from self._handle_missing_files(ctxt.pop())

    def _check_ids(self, ids, loader):
        '''
        Check that an article is returned for each ID.
        '''
        # Make sure we have two independent copies of the IDs.
        ids = list(ids)
        if self.p.article_format == 'pmc':
            # Efetch accepts an optional ID prefix "PMC", but this isn't
            # included in the XML, so it needs to be removed in order to
            # match later.
            remaining = [i.upper().lstrip('PMC') for i in ids]
        else:
            remaining = list(ids)

        # Yield each article while updating the list of remaining IDs.
        try:
            for a in loader.iter_documents(ids):
                try:
                    remaining.remove(a.id_)
                except ValueError:
                    logging.warning('Unexpected article ID: %s', a.id_)
                yield a
            # When all articles have been yielded, remaining should be empty.
            if remaining:
                head = ', '.join(remaining[:5]+['...'] if len(remaining) > 5
                                 else remaining)
                raise ValueError('{} articles missing ({})'
                                 .format(len(remaining), head))
        # If anything goes wrong (eg. some articles were skipped),
        # the remaining IDs are given to the Exception, so there's
        # a chance to handle them in a fallback method.
        except Exception as e:
            e.ids = remaining
            raise

    def _handle_missing_files(self, missing):
        '''
        Call iter_contents again with the fallback input-method.
        '''
        if not missing:
            return
        logging.info('Fall back to %s format for %d elements...',
                     self.p.fallback_format, len(missing))
        try:
            backup = self.p.article_format, self.p.fallback_format
            self.p.article_format = self.p.fallback_format
            self.p.fallback_format = None  # avoid recursive fallback
            yield from self.iter_contents(missing)
        finally:
            self.p.article_format, self.p.fallback_format = backup

    def iter_pointers(self, pointers=None):
        '''
        Iterate over the pointers.

        Depending on the pointer_type, these are either IDs
        or paths relative to input_directory.
        '''
        if self.p.pointer_type == 'id':
            yield from self._iter_ids(pointers)
        else:
            yield from self._iter_relpaths(pointers)

    def iter_path_ID(self, pointers=None):
        '''
        Iterate over pairs <input path, ID>.

        IDs might be None.
        The paths are either absolute or relative to the
        current working directory.
        '''
        if self.p.pointer_type == 'id':
            # ID-based path: both path and ID are known.
            for id_ in self._iter_ids(pointers):
                yield self.get_in_path(id_), id_
        else:
            # Directory walk: no ID info.
            for path in self._iter_paths(pointers):
                yield path, None

    def _iter_paths(self, paths):
        '''
        Iterate over expanded paths (absolute or relative to the CWD).
        '''
        for path in self._iter_relpaths(paths):
            yield os.path.join(self.p.input_directory, path)

    def _iter_relpaths(self, paths):
        '''
        Iterate over paths relative to input_directory.
        '''
        try:
            yield from self._parse_pointers(paths)
        except ParsePointerException as e:
            # Treat paths as a glob.
            yield from glob.glob1(self.p.input_directory, e.pointers)

    def _iter_ids(self, ids):
        '''
        Iterate over stripped ID strings.
        '''
        try:
            yield from self._parse_pointers(ids)
        except ParsePointerException as e:
            # Treat ids as a file name.
            # Open the file and try again.
            with open(e.pointers) as f:
                yield from self._parse_pointers(f)

    def _iter_subdirs(self, paths):
        '''
        Iterate over pairs <subdir, paths>.

        In both input and output, `paths` can be a glob or
        an iterable of path strings.
        They are all relative to input_directory.
        '''
        try:
            i = self._parse_pointers(paths)
        except ParsePointerException as e:
            # Treat paths as a glob.
            try:
                subs, rest = e.pointers.split(os.sep, 1)
            except ValueError:
                # No subdirectories.
                yield 'unknown', e.pointers
            else:
                # Iterate over a partial glob.
                for s in self._iter_paths(subs):
                    s = os.path.basename(s)
                    yield s, os.path.join(s, rest)
        else:
            # Not a glob: Sort and aggregate.
            subdirs = {}
            for p in i:
                s = p.split(os.sep, 1)[0]
                if s == p:
                    s = 'unknown'
                try:
                    subdirs[s].append(p)
                except KeyError:
                    subdirs[s] = [p]
            yield from subdirs.items()

    def _parse_pointers(self, pointers):
        '''
        Resolve the different ways of specifying the pointers.
        '''
        # Note: This method returns an iterator, rather than being a generator.
        # This makes sure that the ParsePointerException is raised instantly,
        # not only when iteration starts (cf. the try block in _iter_subdirs).
        if pointers is None:
            pointers = self.p.pointers

        # Case 1: str -- treated differently for IDs and paths.
        if isinstance(pointers, str):
            raise ParsePointerException(pointers)
        # Case 2: open file.
        elif hasattr(pointers, 'read'):
            return (line.strip() for line in pointers)
        # Case 3: iterable of string.
        else:
            return iter(pointers)


    # =============== #
    # OUTPUT methods. #
    # =============== #

    def export(self, content):
        '''
        Use the configured output method for exporting this article/collection.
        '''
        for exporter in self._exporters:
            exporter.export(content)

    def _get_exporters(self):
        '''
        Create all required exporters.
        '''
        return [EXPORTERS[fmt](self, fmt) for fmt in self.p.export_format]

    def _get_postfilters(self, paths):
        return [self._load_postfilter(p) for p in paths if p is not None]

    @staticmethod
    def _load_postfilter(path):
        '''
        Import an external postfiltering function from an arbitrary module.
        '''
        # Get the function name, if defined.
        func = 'postfilter'  # default function name
        try:
            p, f = path.rsplit(':', 1)
        except ValueError:
            pass
        else:
            if f.isidentifier():
                path, func = p, f
        # Import the module and access the respective function.
        if path == 'builtin':
            import oger.post as m
        else:
            from importlib.machinery import SourceFileLoader
            m = SourceFileLoader('postfilter', path).load_module()
        return getattr(m, func)

    def get_in_path(self, id_):
        '''
        Construct the input path for this ID.
        '''
        fn = pfmt.format(self.p.fn_format_in, id=id_, ext=self.p.article_format)
        return os.path.join(self.p.input_directory, fn)

    def get_out_path(self, id_, base, fmt, ext):
        '''
        Construct the output path for this ID.
        '''

        # Try to mutually substitute missing values.
        if id_ is None or id_ == 'unknown':
            id_ = base
        elif base is None:
            base = id_
        fn = pfmt.format(self.p.fn_format_out,
                         id=id_, base=base, fmt=fmt, ext=ext)
        return os.path.join(self.p.output_directory, fn)


class LoadContext:
    '''
    A handler for missed files/documents.

    A missed file/document is an input element that can't be
    loaded, for whatever reason (file not found, bad format,
    service unavailable...).

    The handler serves two purposes:
    - collecting the IDs of missed files/documents
      (so they can later be got through the fallback format)
    - suppression of exceptions for missed files/documents
      (issue only a warning instead)

    There are two ways for specifying IDs for collecting:
    - Register a single ID when invoking a context:

        ctxt = LoadContext(True, True)
        with ctxt.setcurrent(id_):
            <attempt to load>

    - Include a list of IDs in the exception raised by the
      loader, given as an attribute called `ids`.
    '''
    def __init__(self, ignore_load_errors, collect_missing):
        self._ignore_load_errors = ignore_load_errors
        self._collect_missing = collect_missing
        self._missed = []
        self._current_id = None

    def setcurrent(self, id_=None):
        '''
        Register the current ID.

        This methods returns its object instance (self),
        so it can be used with the "with" statement.
        '''
        self._current_id = id_
        return self

    def pop(self):
        '''
        Return and reset the list of missed IDs.
        '''
        missed, self._missed = self._missed, []
        return missed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Unset the current ID (avoid leaking this information across calls).
        id_, self._current_id = self._current_id, None

        # Interesting things happen only if there was an exception.
        if exc_type is not None:
            # Check if we should and can collect missing IDs.
            # If so, ignore_load_errors doesn't apply (collecting is silent).
            if self._collect_missing:
                if id_ is not None:
                    self._missed.append(id_)
                    return True  # suppress exception
                elif hasattr(exc_val, 'ids'):
                    self._missed.extend(exc_val.ids)
                    return True  # suppress exception

            # No collecting: either warn or let the exception bubble up.
            if self._ignore_load_errors:
                logging.warning('Cannot load item with ID %s (%s: %s)',
                                id_, exc_type, exc_val)
                return True  # suppress exception

        return False  # if there was an exception, raise it now


class PaddingFormatter(string.Formatter):
    '''
    String formatter with additional !p conversion.

    The !p conversion causes an integer to be zero-padded
    to length 8.
    '''
    def convert_field(self, value, conversion):
        if conversion == 'p':
            return format(int(value), '08d')
        return super().convert_field(value, conversion)

pfmt = PaddingFormatter()


class ParsePointerException(Exception):
    'Cannot handle this pointer value.'
    def __init__(self, pointers):
        super().__init__()
        self.pointers = pointers
