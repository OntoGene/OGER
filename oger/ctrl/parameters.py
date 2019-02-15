#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Parameter handling.

This module provides:
- a description of all parameters and their defaults
- parsers for command-line arguments and .ini-file settings
- a config object initialised with parameter attributes
'''


import re
import sys
import json
import logging
import argparse
import configparser as cp
from collections import defaultdict

from .. import __version__
from ..doc import INFMTS, OUTFMTS
from ..util.misc import BackwardsCompatibility


# Number of parallel processes.
# This isn't in Params, because it should not be set in the settings files.
WORKERS = 1


class ParamBase(object):
    'Abstract base class for parameter holders.'
    def __init__(self, raw_config):
        '''
        In order to copy or override paramater holder objects,
        their original input is saved and can be accessed
        through the __getitem__() and keys() methods.
        This allows to use such an object as a mapping
        in eg. `dict(params)` or `dict(**params)`,
        which is very useful for cascading configurations
        (eg. `Params(**dict(defaults, **specialized))`).

        Subclasses of ParamBase are required to provide all
        of their raw input to this constructor as a single
        dictionary.
        '''
        self._raw_config = raw_config

    def __getitem__(self, key):
        return self._raw_config[key]

    def keys(self):
        '''
        A view on the keys of the raw config input.
        '''
        return self._raw_config.keys()

    @classmethod
    def iterdefaults(cls):
        '''
        Iterate over default key-value pairs.
        '''
        for param in dir(cls):
            if not param.startswith('_'):
                value = getattr(cls, param)
                if not callable(value):
                    yield param, value

    def iterparams(self):
        '''
        Iterate over key-value pairs of this configuration.
        '''
        for param, _ in self.iterdefaults():
            yield param, getattr(self, param)

    @staticmethod
    def canonicalize(params):
        '''
        Convert dashes in parameter names to underscores.

        Change the `params` dict in-place.
        '''
        changes = []
        for name in params:
            if '-' in name:
                changes.append((name, name.replace('-', '_')))
        for dashed, canonical in changes:
            if canonical in params:
                # A collision like this is most probably a mistake.
                raise TypeError('multiple occurrences of parameter {}'
                                .format(canonical))
            params[canonical] = params.pop(dashed)

    @classmethod
    def canonicalized(cls, params):
        '''
        Create a canonicalized copy of params.
        '''
        params = dict(params)
        cls.canonicalize(params)
        return params

    @classmethod
    def merged(cls, *args, **kwargs):
        '''
        Cascade param overrides, taking care of dash normalisation.
        '''
        params = {}
        for level in (*args, kwargs):
            params.update(cls.canonicalized(level))
        return params

    @staticmethod
    def split(arg):
        '''
        If arg is a string, parse it as a JSON array or
        split it on whitespace.
        '''
        if isinstance(arg, str):
            try:
                arg = json.loads(arg)
            except json.JSONDecodeError:
                arg = arg.split()
        # It can't hurt to have hashable config values where possible.
        return tuple(arg)

    @staticmethod
    def mapping(arg, allow_None=False):
        '''
        If arg is a string, parse it as a JSON object.

        If allow_None is True, None may be returned instead
        of a dict instance.
        '''
        if isinstance(arg, str):
            arg = json.loads(arg)
        if allow_None and arg is None:
            return None
        return dict(arg)

    @staticmethod
    def bool(arg):
        '''
        Boolean arguments from the command-line need special care.
        '''
        if isinstance(arg, str):
            a = arg.lower()
            if a in ('0', 'false', 'off', 'no'):
                return False
            elif a in ('1', 'true', 'on', 'yes'):
                return True
            else:
                raise ValueError('Invalid boolean flag: {}'.format(arg))
        return bool(arg)


class Params(ParamBase):
    """
    Holds configuration for the pipeline.

    Modify the class variables to change the defaults.
    Override the defaults at runtime through keyword args
    in the constructor.
    """

    # Settings files (none by default).
    settings = ()

    # LOGGING parameters.
    log_level = logging.WARNING
    log_format = '%(process)d - %(asctime)s - %(levelname)s: %(message)s'
    log_datefmt = '%Y-%m-%d %H:%M:%S'
    log_file = None  # None: log to STDERR


    # INPUT parameters.
    # =================

    # Iteration basis: iterate over documents or collections.
    # Valid mode names: "document" and "collection".
    iter_mode = 'document'
    # Pointer type: construct file names from IDs or use globbing.
    # Valid type names: "id" and "glob".
    pointer_type = 'id'
    # Basic iter-elements: either IDs or a glob expression.
    pointers = ()
    # pointers is interpreted differently, depending on its type.
    # If it has a read() method, it is treated as an open file,
    # containing a newline-separated list of IDs or paths.
    # Otherwise, if it is not a string, it is treated as an iterable
    # of IDs/paths.
    # If it is a string, it also depends on the pointer_type:
    # For "id", it is interpreted as a path to a file containing IDs.
    # For "glob", it is treated as a glob for matching files in the
    # `input_directory`.
    # In collection mode, each path (after resolving any globs) corresponds
    # to a collection in bioc and pxml.gz format;
    # with other formats, each subdirectory (below input_directory) is seen
    # as a collection.
    # All paths are interpreted relative to input_directory.

    # Read on-disk files from this location.
    input_directory = None
    # File name format: called with `.format(id=id, ext=article_format)`,
    # thus the string formatting syntax can be used to construct a file name
    # (including subdirectories) based on ID and format.
    # Besides the built-in syntax, the formatter used supports an additional
    # conversion flag "p", which produces a zero-padded, 8-digit version of
    # the ID. Use it like so: '{id!p}'
    # -- or, to get a 4-digit prefix: '{id!p:.4}'.
    fn_format_in = '{id}.{ext}'
    # Input article format: see oger.doc.INFMTS for a list of accepted names.
    article_format = 'pxml'
    # Fallback for the input format: If a document can't be loaded,
    # try again with this alternative format (using IDs as pointers, not paths)
    # Works well with pubmed|pmc|becalmabstracts|becalmpatents
    fallback_format = None
    # If a document can't be loaded, an exception is raised, which interrupts
    # the whole process.
    # If ignore_load_errors is True, a warning is issued only, instead.
    ignore_load_errors = False

    # Include the MeSH list, as a separate section (pxml, pxml.gz, pubmed).
    include_mesh = False
    # In addition to the descriptor name, add an Entity with the MeSH ID (UI).
    mesh_as_entities = False
    # Conflate all sections into one section (pxml, pxml.gz, pubmed, txt).
    # For txt, if single_section is False, blank lines separate sections.
    single_section = False
    # Rely on given sentence splitting (one sentence per line) (txt only).
    sentence_split = False

    # Maximum number of IDs per request to the efetch API.
    efetch_max_ids = 1000

    # Interpret offsets wrt bytes instead of codepoints (bioc only).
    # If you need byte offsets in the (BioC) output, then use the
    # byte_offsets_out parameter.
    byte_offsets_in = False


    # OUTPUT parameters.
    # ==================

    # Write output files to this directory.
    output_directory = None
    # The output format is interpolated with the ID and a default value
    # for `ext` ("tsv" or "xml").
    # It is applied like fn_format_in (see above).
    fn_format_out = fn_format_in
    # Output format: see oger.doc.OUTFMTS for a list of accepted names.
    # A space-separated list is also accepted (multiple files per article).
    export_format = 'tsv'
    # Additional fields in the termlist TSV.
    # This is a list of field labels.
    # If there are multiple termlists, all must have the same length.
    extra_fields = ()
    # Map the default fields to custom names.
    # Must be a mapping or a string with a JSON object.
    field_names = ()

    # Format-specific output flags:
    # - TSV formats:
    include_header = False
    # - BioC format: anchor text at passage (default) or sentence level:
    sentence_level = False
    # - BioC format: collection-level metadata.
    #   Must be a mapping or a string with a JSON object.
    #   Keys different from "source", "date", and "key" are put into
    #   <infon> elements.
    bioc_meta = None
    # - BioC format: produce offsets wrt bytes instead of codepoints.
    #   If byte offsets are already given through (BioC) input, then use
    #   the byte_offsets_in parameter.
    byte_offsets_out = False
    # - Brat format: fields that go into attribute annotations.
    brat_bin_attributes = ()  # binary attributes
    brat_mv_attributes = ()  # multi-valued attributes

    # Hook for postfiltering an article or collection.
    # Path(s) to a module, optionally followed by a function name,
    # separated by a colon, eg. "path/to/module.py:exclude_short".
    # This function is called with an Article or Collection object,
    # for modifying it in-place before writing the result.
    postfilter = ()


    # TEXT PROCESSING.
    # ================

    # Tokenizers used in text_processing and for entity_recognition.
    # Currently, word_tokenizer can be WordPunctTokenizer or PunktWordTokenizer
    # sentence_tokenizer currently can only be PunktSentenceTokenizer
    word_tokenizer = 'WordPunctTokenizer'
    sentence_tokenizer = 'PunktSentenceTokenizer'


    def __init__(self, settings=None, **kwargs):
        """
        Override default values through keyword arguments.
        """
        # Replace dashes and save a copy of the raw settings on the superclass.
        self.canonicalize(kwargs)
        super().__init__(dict(kwargs, settings=settings))

        # Load any settings file and override them with any keyword args.
        params = self.load_ini_file(settings or self.settings)
        params.update(kwargs)

        # Create instance variables which hide the class defaults.
        er_params = []
        backw_comp = BackwardsCompatibility({
            ('article_format', 'bioc'): 'bioc_xml',
            ('export_format', 'bioc'): 'bioc_xml',
            'termlist_extra_fields': 'extra_fields',
            ('termlist_field_format', 'hub'): 'bth',
        })
        for key, value in backw_comp.items(params):
            if hasattr(self, key):
                setattr(self, key, value)
            elif key.startswith('termlist'):
                er_params.append((key, value))
            else:
                raise ValueError('Invalid settings key: {}'.format(key))

        # Setup logging.
        logargs = dict(level=self.log_level,
                       format=self.log_format,
                       datefmt=self.log_datefmt)
        if self.log_file:
            logargs['filename'] = self.log_file
        logging.basicConfig(**logargs)

        # Sanity-checks.
        incompatible = {
            'glob': ['pubmed', 'pmc', 'becalmabstracts', 'becalmpatents'],
            'id': [],
        }
        try:
            if self.article_format in incompatible[self.pointer_type]:
                logging.warning(
                    'Input format %r is not designed for pointer-type "%s"',
                    self.article_format, self.pointer_type)
            if self.fallback_format and self.pointer_type != 'id':
                logging.warning(
                    'Fallback format only works with "id" pointers')
        except KeyError as e:
            logging.exception('Invalid pointer-type: %r', e)
            raise
        # Pending compatibility warnings.
        backw_comp.warnings()

        # Some parameter values need preprocessing.
        self.ignore_load_errors = self.bool(self.ignore_load_errors)
        self.include_mesh = self.bool(self.include_mesh)
        self.single_section = self.bool(self.single_section)
        self.sentence_split = self.bool(self.sentence_split)
        self.efetch_max_ids = int(self.efetch_max_ids)
        self.export_format = self.split(self.export_format)
        self.extra_fields = self.split(self.extra_fields)
        self.field_names = self.mapping(self.field_names)
        self.include_header = self.bool(self.include_header)
        self.sentence_level = self.bool(self.sentence_level)
        self.bioc_meta = self.mapping(self.bioc_meta, allow_None=True)

        self.byte_offsets_in = self.bool(self.byte_offsets_in)
        self.byte_offsets_out = self.bool(self.byte_offsets_out)

        self.brat_bin_attributes = self.split(self.brat_bin_attributes)
        self.brat_mv_attributes = self.split(self.brat_mv_attributes)
        self.brat_attributes = [(n, m)
                                for a, m in ((self.brat_bin_attributes, False),
                                             (self.brat_mv_attributes, True))
                                for n in a]
        self.postfilter = self.split(self.postfilter)

        self.recognizers = tuple(self.parse_ER_settings(er_params))

    @classmethod
    def iterdefaults(cls):
        yield from super().iterdefaults()
        yield 'recognizers', (tuple(ERParams.iterdefaults()),)

    def iterparams(self):
        yield from super().iterparams()
        ers = tuple(tuple(ep.iterparams()) for ep in self.recognizers)
        yield 'recognizers', ers

    @classmethod
    def load_ini_file(cls, fns):
        '''
        Parse settings from an INI file.
        '''
        parser = cp.ConfigParser(interpolation=cp.ExtendedInterpolation())
        parser.read(fns)
        params = {}
        for sec_name, section in parser.items():
            sec_name = sec_name.lower()
            if sec_name == 'main':
                # General parameters.
                params.update(section)
            elif sec_name.startswith('termlist'):
                # ER params, found in separate sections.
                for key, value in section.items():
                    params['{}_{}'.format(sec_name, key)] = value
                # Ignore any other sections.
        # Finally, be polite and accept dashed parameter keys,
        # just like they are spelt in the command-line options.
        cls.canonicalize(params)
        return params

    keypattern = re.compile(r'termlist(\d*)_(\w+)$')

    def parse_ER_settings(self, rawparams):
        '''
        Parse and distribute settings for (multiple) entity recognizers.
        '''
        instances = defaultdict(dict)
        for key, value in rawparams:
            try:
                n, k = self.keypattern.match(key).groups()
            except AttributeError:
                raise ValueError(
                    'Invalid termlist settings key: {}'.format(key))
            instances[int(n or 0)][k] = value  # n or 0: missing number == 0

        shared_params = instances.pop(0, {})
        shared_params['_n_extra'] = len(self.extra_fields)
        if not instances:
            # With no parameter groups besides the shared ones, just use those.
            instances[1] = {}
        for _, params in sorted(instances.items()):
            args = dict(shared_params, **params)
            yield ERParams(**args)


class ERParams(ParamBase):
    '''
    Settings container for an entity recognizer.
    '''
    # Path to a list of entities to be used for entity recognition.
    path = None
    # Fields of the termlist TSV:
    # Possible values:
    #   4 (old format)
    #   6 (extended old format, native ID first)
    #   bth (Bio-Term-Hub format, UMLS CUI first)
    field_format = 'bth'
    # Is the first line a column header that should be skipped?
    skip_header = False
    # Non-public config: number of extra fields (derived from global option).
    _n_extra = 0

    # Location of the cached termlists (same directory as `path`, by default).
    cache = None
    # Force loading the terms from TSV, even if a cached pickle exists.
    force_reload = False

    # Regular expression defining a token, as used in the ER process.
    term_token = None

    # Local abbreviation detection (document-wise memory).
    abbrev_detection = False

    # Normalization for term lookup.
    # This must be a name loadable from the term_normalization module.
    # If multiple names are given (separated by blanks),
    # all corresponding methods are applied sequentially.
    # Some methods take parameters; join these with dashes onto the name,
    # eg. "stem-lancaster" or "unicode-NFKC".
    normalize = 'lowercase'

    # Stopwords: terms that are not normalized.
    stopwords = None

    def __init__(self, **kwargs):
        """
        Override default values through keyword arguments.
        """
        super().__init__(kwargs)
        # Create instance variables which hide the class defaults.
        for key, value in kwargs.items():
            if not hasattr(self, key):
                raise ValueError(
                    'Invalid termlist settings key: {}'.format(key))
            setattr(self, key, value)

        # Make this parameter publicly readable.
        self.n_extra = self._n_extra

        # Some parameter values need preprocessing.
        self.skip_header = self.bool(self.skip_header)
        self.force_reload = self.bool(self.force_reload)
        self.abbrev_detection = self.bool(self.abbrev_detection)
        self.normalize = self.split(self.normalize)


def parse_cmdline(args=None):
    '''
    Parse commandline arguments into a dict of parameters.
    '''
    ap = argparse.ArgumentParser(
        description='Run OGER locally.',
        usage='%(prog)s [OPTIONS] [-t (id|glob)] [POINTERS]',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        argument_default=argparse.SUPPRESS,
        add_help=False)

    gg = ap.add_argument_group(
        title='General options')
    gg.add_argument(
        '-h', '--help', action='help',
        help='show this help message and exit')
    gg.add_argument(
        '-V', '--version', action='version', version=__version__)
    gg.add_argument(
        '-s', '--settings', metavar='PATH', nargs='+',
        help='Load settings from a config file in INI format. '
             'These settings override the defaults, '
             'and are themselves overridden by the command-line args. '
             'Multiple files are accepted. '
             '(default: {})'.format(Params.settings))
    gg.add_argument(
        '-j', '--parallel-workers', dest='n_workers',
        type=int, default=WORKERS, metavar='N',
        help='run N parallel processes (default: %(default)s)')

    pg = ap.add_argument_group(
        title='Pipeline parameters',
        description='These arguments override the settings '
                    'in the config file(s).')
    pg.add_argument(
        '-t', '--pointer-type', choices=('id', 'glob'),
        help='How to interpret the POINTERS: IDs or glob expression(s)?')
    pg.add_argument(
        'pointers', metavar='POINTERS', nargs='?',
        type=lambda a: sys.stdin if a == '-' else a,
        help='"id" type: path to a list of IDs (one per line). -- '
             '"glob" type: glob expression for filtering the input files. -- '
             'Provide "-" to read a newline-separated list of '
             'IDs/paths from STDIN.')
    pg.add_argument(
        '-m', '--iter-mode', choices=('document', 'collection'),
        help='Iterate over single documents, or collections.')
    pg.add_argument(
        '-i', '--input-directory', metavar='PATH',
        help='location of the raw articles '
             '(default: {})'.format(Params.input_directory))
    pg.add_argument(
        '-f', '--article-format', metavar='FMT', choices=INFMTS,
        help='format of the input articles. '
             'Valid formats are: %(choices)s. '
             '(default: {})'.format(Params.article_format))
    pg.add_argument(
        '-b', '--fallback-format', metavar='FMT', choices=INFMTS,
        help='if a file cannot be found based on its ID, '
             'fall back to using this format. '
             'This makes only sense for ID pointers! '
             '(default: {})'.format(Params.fallback_format))
    pg.add_argument(
        '-o', '--output-directory', metavar='PATH',
        help='target location '
             '(default: {})'.format(Params.output_directory))
    pg.add_argument(
        '-e', '--export-format', nargs='+', metavar='FMT', choices=OUTFMTS,
        help='format of the output files (multiple fomats are allowed). '
             'Valid formats are: %(choices)s. '
             '(default: {})'.format(Params.export_format))
    pg.add_argument(
        '-p', '--postfilter', nargs='+', metavar='PATH[:FUNC]',
        help='use function FUNC in the Python3 module at PATH '
             'for post-editing. '
             'This function is called with each Article/Collection object '
             'after ER, but before writing to the different output formats. '
             'FUNC must be a top-level name; it defaults to "postfilter". '
             'If multiple postfilters are given, '
             'they are applied sequentially. ')
    pg.add_argument(
        '-v', '--verbose', dest='log_level', action=IntervalCountAction,
        start=logging.WARNING, interval=-10,
        help='increase verbosity to see progress info '
             '(supply -v twice to see debug information)')
    pg.add_argument(
        '-q', '--quiet', dest='log_level', action=IntervalCountAction,
        start=logging.WARNING, interval=10,
        help='suppress warnings')
    pg.add_argument(
        '-c', '--config', nargs=2, action='append', default=[],
        metavar=('KEY', 'VALUE'),
        help='any other setting, passed on directly to the config '
             '(repeat option -c for multiple key-value pairs)')

    # Argument preprocessing.
    args = vars(ap.parse_args(args))
    # Raise -c args to the top level.
    args.update(args.pop('config'))
    ParamBase.canonicalize(args)

    return args


class IntervalCountAction(argparse.Action):
    '''
    Configurable argument counter.

    Basically works like argparse's standard "count" action,
    but allows for setting start, interval, minimum and
    maximum values.

    If start is not given, it defaults to `default`.
    If minimum and/or maximum are given, the counted value
    is silently capped at the corresponding extremum.
    '''

    def __init__(self,
                 option_strings,
                 dest,
                 start=None,
                 interval=1,
                 minimum=None,
                 maximum=None,
                 default=None,
                 required=False,
                 help=None):
        super().__init__(
            option_strings=option_strings,
            dest=dest,
            nargs=0,
            default=default,
            required=required,
            help=help)
        if start is None:
            start = self.default
        self.start = start
        self.interval = interval
        self.minimum = minimum
        self.maximum = maximum

    def __call__(self, parser, namespace, values, option_string=None):
        new_count = getattr(namespace, self.dest, self.start) + self.interval
        if self.minimum is not None:
            new_count = max(new_count, self.minimum)
        if self.maximum is not None:
            new_count = min(new_count, self.maximum)
        setattr(namespace, self.dest, new_count)


class NestedNamespace(argparse.Namespace):
    '''
    Namespace class with nesting through dot notation.
    '''
    def __setattr__(self, name, value):
        '''
        Recursively create subnamespaces for each dotted level.
        '''
        try:
            group, elem = name.split('.', 1)
        except ValueError:
            # Recursion ends: no more dots found.
            super().__setattr__(name, value)
        else:
            subspace = self._ensure_subspace(group)
            setattr(subspace, elem, value)

    def __getattr__(self, name):
        '''
        Recursively access subnamespaces.
        '''
        try:
            group, elem = name.split('.', 1)
        except ValueError:
            # If there is no dot, then the attribute should have been found
            # already without calling __getattr__() in the first place.
            raise AttributeError('{!r} object has no attribute {!r}'
                                 .format(self.__class__.__name__, name))
        else:
            subspace = getattr(self, group)
            return getattr(subspace, elem)

    def _ensure_subspace(self, name):
        try:
            subspace = getattr(self, name)
        except AttributeError:
            subspace = NestedNamespace()
            super().__setattr__(name, subspace)
        return subspace
