#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016--2019


'''
A RESTful API for OGER.
'''


import os
import json
import logging
import hashlib
import argparse
import datetime
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

from lxml import etree as ET
from bottle import get, post, delete, response, request, error, HTTPError
from bottle import run as run_bottle, view, ERROR_PAGE_TEMPLATE

from ..ctrl import router, parameters
from ..util.misc import log_exc
from .expfmts import EXPORT_FMTS, export
from .client import ParamHandler, sanity_check


# ============= #
# Server setup. #
# ============= #

BOTTLE_HOST = '0.0.0.0'
BOTTLE_PORT = 12321

INPUT_FORM = os.path.join(os.path.dirname(__file__), 'static', 'form.html')


def main():
    '''
    Run the servers.
    '''
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    bottle = ap.add_argument_group(title='server configuration')
    bottle.add_argument(
        '-i', '--host', dest='bottle.host', metavar='IP', default=BOTTLE_HOST,
        help='host IP')
    bottle.add_argument(
        '-p', '--port', dest='bottle.port', metavar='N', default=BOTTLE_PORT,
        type=int,
        help='port number')
    bottle.add_argument(
        '-n', '--annotators', metavar='N', default=3, type=int,
        help='maximum number of non-default annotation dictionaries')
    bottle.add_argument(
        '-d', '--debug', dest='bottle.debug', action='store_true',
        help='display exceptions in the served responses')

    ann = ap.add_argument_group(title='OGER configuration')
    ann.add_argument(
        '-s', '--settings', dest='ann.settings', metavar='PATH', nargs='+',
        help='load OGER settings from one or more .ini config files')
    ann.add_argument(
        '-c', '--config', nargs=2, action='append', default=[],
        metavar=('KEY', 'VALUE'),
        help="any other setting, passed on directly to OGER's config "
             '(repeat option -c for multiple key-value pairs)'
             '%(default).0s')

    # Argument preprocessing.
    args = ap.parse_args(namespace=parameters.NestedNamespace())
    bottle_args = vars(args.bottle)
    ann_args = parameters.Params.merged(vars(args.ann), args.config)

    init(ann_args, bottle_args, args.annotators)


def init(ann_conf, bottle_conf, annotators):
    '''
    Setup and start the servers.
    '''
    # A global variable is needed here because the routes are mapped
    # to top-level functions.
    global ann_manager

    # Pipeline config.
    ann_params = parameters.Params(**ann_conf)
    # Organise logging after basicConfig was called in the Params constructor,
    # but before anything interesting happens (like termlist loading).
    setup_logging()
    # Get the default OGER server.
    ann_manager = AnnotatorManager(ann_params, n=annotators)

    # Bottle: request handling.
    run_bottle(**bottle_conf)


def setup_logging():
    '''
    Make sure there is at least a handler for STDERR on the root logger.
    '''
    root = logging.getLogger()
    if not any(h.stream.name == '<stderr>' for h in root.handlers):
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(
            logging.Formatter('%(asctime)s: %(message)s'))
        root.addHandler(console_handler)


# =============== #
# Route handling. #
# =============== #

ann_manager = None  # this global variable is set in init()

ANN = '/<ann:re:[0-9a-f]+>'

FETCH = '/fetch'
UPLOAD = '/upload'

FETCH_SOURCES = ('pubmed', 'pmc')
UPLOAD_FMTS = ('txt', 'txt_json', 'bioc', 'pxml', 'nxml', 'pxml.gz')

SOURCE = '/<source:re:{}>'.format('|'.join(FETCH_SOURCES))
IN_FMT = '/<in_fmt:re:{}>'.format('|'.join(UPLOAD_FMTS))
OUT_FMT = '/<out_fmt:re:{}>'.format('|'.join(EXPORT_FMTS))
DOCID_WILDCARD = '/<docid:re:[1-9][0-9]*>'

USAGE_MSG = '''\
Valid "fetch" request (GET or POST method):
/fetch/:SOURCE/:OUT_FMT/:DOC_ID

Valid "upload" requests (POST method only):
/upload/:IN_FMT/:OUT_FMT
/upload/:IN_FMT/:OUT_FMT/:DOC_ID

Valid SOURCE values:
{sources}

Valid IN_FMT values:
{in_fmt}

Valid OUT_FMT values:
{out_fmt}

Valid DOC_ID values:
[1-9][0-9]*
'''.format(sources='|'.join(FETCH_SOURCES),
           in_fmt='|'.join(UPLOAD_FMTS),
           out_fmt='|'.join(EXPORT_FMTS))


# Root: serve an HTML page for the web UI.

@get('/')
def web_ui(target=None):
    'Serve an HTML page with an input form.'
    if target is None:
        # Check the params for a target annotator.
        target = request.params.get('dict')
    return build_web_page(INPUT_FORM, target)


@post('/')
def web_ui_post():
    'POST request allows annotator loading on page request.'
    target = None
    # If there is a payload, try to load an annotator.
    if request.json:
        # Preferred way is to use JSON.
        target = _load_annotator(request.json)
    elif 'json' in request.params:
        # Work-around for the BTH:
        # it uses an HTML form, so the JSON snippet is embedded in form data.
        try:
            payload = json.loads(request.params['json'])
        except Exception as e:
            raise HTTPError(400, e)
        target = _load_annotator(payload)
    return web_ui(target)


def build_web_page(source, target):
    'Customise the entry page with available annotators.'
    page = ET.parse(source, parser=HTMLParser)
    radio_group = page.find('.//div[@id="div-ann-radios"]')
    ann_manager.purge()
    for name in ann_manager.additional:
        ann = ann_manager.active[name]
        # Create a new radio button for each non-default annotator.
        args = dict(type='radio', name='annotator', value=name)
        node = ET.SubElement(radio_group, 'input', **args)
        # Add a label with the description.
        node.tail = ' '
        label = ET.SubElement(radio_group, 'label', id='label-'+name)
        label.text = ann.description
        ET.SubElement(radio_group, 'br')
        # If this annotator is targeted, pre-select it.
        if name == target:
            radio_group[0].attrib.pop('checked', None)
            node.set('checked', 'checked')
        # Disable this annotator if not ready yet (even if pre-selected).
        if not ann.is_ready():
            node.set('disabled', 'disabled')
        # If the targeted annotator is not ready yet,
        # disable the submit button as well.
        if node.get('checked') and node.get('disabled'):
            submit_button = page.find('.//input[@id="btn-ann-submit"]')
            submit_button.set('disabled', 'disabled')
    return ET.tostring(page, method='html')

HTMLParser = ET.HTMLParser()


# Status: status information of the whole service.

@get('/status')
def system_status():
    '''
    Check if the whole service is running.
    '''
    ann_manager.purge()
    return {
        'status': 'running',
        'active annotation dictionaries': len(ann_manager.active),
        'default dictionary': ann_manager.default,
    }


# Dict: create/check/remove an annotator.

@post('/dict')
def load_annotator():
    '''
    Load a new annotator, if necessary.

    The settings are expected in the JSON payload.
    '''
    name = _load_annotator(request.json)
    response.status = '202 Accepted'
    response.headers['Location'] = '/dict/{}'.format(name)
    return {'dict_id': name}

def _load_annotator(payload):
    try:
        settings = payload.get('settings', {})
        desc = payload.get('description')
        name = ann_manager.add(settings, desc=desc)
    except Exception as e:
        raise HTTPError(400, e)
    return name


@get('/dict' + ANN)
def get_annotator(ann):
    '''
    Obtaining an annotator is not (yet) possible.
    '''
    try:
        ann_manager.get(ann)
    except KeyError as e:
        raise HTTPError(404, 'unknown dict: {}'.format(e), exception=e)
    raise HTTPError(403, 'cannot export dictionary')


@get('/dict' + ANN + '/status')
def check_annotator(ann):
    '''
    Check if this annotator is ready.
    '''
    try:
        annotator = ann_manager.get(ann)
        status = 'ready' if annotator.is_ready() else 'loading'
    except KeyError as e:
        raise HTTPError(404, 'unknown dict: {}'.format(e), exception=e)
    except RuntimeError:
        ann_manager.remove(ann)
        status = 'crashed'

    return {'description': annotator.description, 'status': status}


@delete('/dict' + ANN)
def remove_annotator(ann):
    '''
    Dispose of this annotator.
    '''
    try:
        ann_manager.remove(ann)
    except IllegalAction as e:
        raise HTTPError(403, str(e).replace('annotator', 'dictionary'))
    except KeyError as e:
        raise HTTPError(404, 'unknown dict: {}'.format(e), exception=e)


# Fetch/upload: annotate documents.

@get(FETCH + SOURCE + OUT_FMT + DOCID_WILDCARD)
@post(FETCH + SOURCE + OUT_FMT + DOCID_WILDCARD)
def fetch_article(source, out_fmt, docid):
    'Fetch and process one article.'
    logging.info('GET request: article %s from %s in %s format',
                 docid, source, out_fmt)
    return load_process_export([docid], source, out_fmt, docid=None)


@post(UPLOAD + IN_FMT + OUT_FMT)
@post(UPLOAD + IN_FMT + OUT_FMT + DOCID_WILDCARD)
def upload_article(in_fmt, out_fmt, docid=None):
    'Process one uploaded article.'
    logging.info('POST request: %s -> %s (DOCID: %s)', in_fmt, out_fmt, docid)
    return load_process_export(request.body, in_fmt, out_fmt, docid)


def load_process_export(data, in_fmt, out_fmt, docid):
    'Load, process, and export an article (or collection).'
    try:
        params = ParamHandler(request.query)
    except ValueError as e:
        raise HTTPError(400, e)
    try:
        annotator = ann_manager.get(params.dict)
    except KeyError as e:
        raise HTTPError(404, 'unknown dict: {}'.format(e), exception=e)

    in_params = dict(data=data, fmt=in_fmt, id_=docid, **params.in_params)
    out_params = dict(fmt=out_fmt, **params.out_params)
    try:
        return annotator.process(in_params, out_params, params.postfilters)
    except Exception as e:
        data = in_params.pop('data')
        logging.exception('Fatal: data: %.40r, params: %r, %r, %r',
                          data, in_params, out_params, params.postfilters)
        raise HTTPError(400, e)


# Legacy interface.

@get(OUT_FMT + DOCID_WILDCARD)
def legacy_fetch(out_fmt, docid):
    'Obsolete route for fetching.'
    return fetch_article('pubmed', out_fmt, docid)

@post(IN_FMT + OUT_FMT)
@post(IN_FMT + OUT_FMT + DOCID_WILDCARD)
def legacy_upload(in_fmt, out_fmt, docid=None):
    'Obsolete route for uploading.'
    return upload_article(in_fmt, out_fmt, docid)


# Error formatting.

@error(404)
@view(ERROR_PAGE_TEMPLATE)
def error404(err):
    '''
    Without specific exception, insert a usage message.
    '''
    if err.exception is None:
        err.body = 'Invalid resource locator.\n\n{}'.format(USAGE_MSG)
    return dict(e=err)


# =================== #
# Annotator handling. #
# =================== #

class AnnotatorManager:
    '''
    Container for a limited number of active annotation servers.
    '''
    def __init__(self, default, n=3):
        '''
        Args:
            default (Params instance or dict of parameters):
                parameters for the default server
            n (int): max number of additional servers
        '''
        self._default_settings = router.Router(default)
        self.n = n
        self.default = self.key(self._default_settings)  # default server name
        self.active = {}                  # all active servers
        self.additional = []              # names of additional servers

        logging.info('Starting default annotator %s', self.default)
        self.active[self.default] = Annotator(self._default_settings,
                                              desc='default', blocking=True)

    def add(self, params, desc=None, blocking=False):
        '''
        Create a new annotator and give it a name.

        In case an existing one has the same settings,
        return its name instead.
        '''
        # Sanitize user-specified parameters for security reasons.
        for p, v in params.items():
            sanity_check(p, v)
        config = router.Router(self._default_settings, **params)

        key = self.key(config)
        self.purge()
        if key not in self.active:
            logging.info('Starting new annotator %s', key)
            self.active[key] = Annotator(config, desc, blocking)
            self.additional.append(key)
            # Dispose of surplus annotators.
            while len(self.additional) > self.n:
                self.remove()
        return key

    def remove(self, name=None):
        '''
        Remove and destroy an annotator.

        If name is not given or None, remove the oldest annotator.
        '''
        if name is None:
            name = self.additional.pop(0)
        else:
            try:
                self.additional.remove(name)
            except ValueError:
                if name == self.default:
                    raise IllegalAction('cannot remove default annotator')
                raise KeyError(name)
        logging.info('Removing annotator %s', name)
        del self.active[name]

    def get(self, name):
        '''
        Find an annotator by its name.

        If `bool(name)` is False, return the default annotator.
        If the name is not found, a KeyError is raised.
        '''
        if not name:
            name = self.default
        return self.active[name]

    def purge(self):
        """Remove any dead annotators."""
        for name, ann in list(self.active.items()):
            try:
                ann.is_ready()
            except RuntimeError:
                self.remove(name)

    @classmethod
    def key(cls, conf):
        'Select the relevant parts of this configuration.'
        struct = tuple(tuple(ep.iterparams()) for ep in conf.p.recognizers)
        return cls.hashtoken(struct)

    @classmethod
    def hashtoken(cls, structure):
        'Create a hex token from a hashable structure.'
        h = hashlib.sha1(repr(structure).encode()).hexdigest()
        return h[-16:]  # 64 bits (same as Python's hash()) is enough


class Annotator:
    """
    Wrapper for a PipelineServer with termlist loading in a separate thread.
    """

    def __init__(self, config, desc, blocking=False):
        if desc is None:
            desc = 'Annotator created at {}'.format(datetime.datetime.utcnow())
        self.config = config
        self.description = desc
        self._postfilters = None  # accessible by name
        self._pls = router.PipelineServer(self.config, lazy=True)

        # Load the termlist asynchronously.
        executor = ThreadPoolExecutor(max_workers=1)
        self._loading = executor.submit(
            log_exc, self._pls.get_ready, 'loading annotator failed')
        self._ready = False
        executor.shutdown(wait=blocking)
        self.is_ready()  # trigger an exception if loading failed.

    def is_ready(self):
        '''
        Has this annotator finished loading the termlist?
        '''
        if not self._ready and self._loading.done():
            # Wasn't ready before, but is now.
            if self._loading.exception() is not None:
                raise RuntimeError('annotator has died')
            self._ready = True
        return self._ready

    @property
    def postfilters(self):
        """Postfilters by name."""
        if self._postfilters is None:
            if not self.is_ready():
                raise RuntimeError('annotator not yet loaded')
            # Index the filters by their function name.
            self._postfilters = OrderedDict()
            for func in self._pls.conf.postfilters:
                self._postfilters[func.__name__] = func
        return self._postfilters

    def process(self, in_params, out_params, postfilters):
        '''
        Load, process, and export one document or collection.
        '''
        document = self._get_annotated(in_params)
        self._postfilter(document, postfilters)
        ctype, data = export(document, self.config, **out_params)
        response.content_type = ctype
        return data

    def _get_annotated(self, params):
        '''
        Load and annotate one document or collection.
        '''
        if not self.is_ready():
            raise RuntimeError('annotator not yet loaded')
        document = self._pls.load_one(**params)
        self._pls.process(document)
        return document

    def _postfilter(self, document, filternames):
        '''
        Call each postfilter on the document.
        '''
        # Special values: true/false enable/disable all filters.
        # Specifying no filters explicitly defaults to enabling all too.
        if 'true' in filternames or not filternames:
            filternames = self.postfilters.keys()
        elif 'false' in filternames:
            filternames = []

        for name in filternames:
            try:
                postfilter = self.postfilters[name]
            except KeyError:
                raise ValueError('unknown postfilter: {}'.format(name))
            postfilter(document)


# ============== #
# Miscellaneous. #
# ============== #

class IllegalAction(Exception):
    '''
    Attempt to perform a forbidden operation.
    '''
