#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016--2017


'''
A RESTful API for OGER.
'''


import os
import json
import logging
import hashlib
import argparse
import datetime
import multiprocessing as mp
from collections import OrderedDict

from lxml import etree as ET
from bottle import get, post, delete, response, request, error, HTTPError
from bottle import run as run_bottle, view, ERROR_PAGE_TEMPLATE

from ..ctrl import router, parameters
from .expfmts import EXPORT_FMTS, export
from .client import ParamHandler


# ============= #
# Server setup. #
# ============= #

BOTTLE_HOST = '0.0.0.0'
BOTTLE_PORT = 12321
# Bottle's tempfile solution for large files interferes with queue pickling
# for multiprocessing. Increase the threshold to 100M, which should be enough,
# since the allowed payload max size is far less.
request.__class__.MEMFILE_MAX = 100 * 1024 * 1024

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

ANN = '/<ann:re:[0-9A-F]+>'

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
    for name in list(ann_manager.additional):
        ann = ann_manager.active[name]
        # Check for dead annotators.
        try:
            ready = ann.is_ready()
        except RuntimeError:
            ann_manager.remove(name)
            continue

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
        if not ready:
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
        ann = ann_manager.get(ann)
        status = 'ready' if ann.is_ready() else 'loading'
    except KeyError as e:
        raise HTTPError(404, 'unknown dict: {}'.format(e), exception=e)
    except RuntimeError:
        ann_manager.remove(ann)
        status = 'crashed'

    return {'description': ann.description, 'status': status}


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
        self.postfilters = OrderedDict()  # all postfilters

        logging.info('Starting default annotator %s', self.default)
        self.active[self.default] = self.start(self._default_settings,
                                               desc='default', blocking=True)
        for func in self._default_settings.postfilters:
            self.postfilters[func.__name__] = func

    def add(self, params, desc=None, blocking=False):
        '''
        Create a new annotator and give it a name.

        In case an existing one has the same settings,
        return its name instead.
        '''
        # Remove any postfilter for security reasons.
        config = router.Router(self._default_settings,
                               postfilter=(), **params)

        key = self.key(config)
        if key not in self.active:
            logging.info('Starting new annotator %s', key)
            self.active[key] = self.start(config, desc, blocking)
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

    def start(self, config, desc, blocking):
        'Initiate a new annotation server.'
        ann_type = BlockingAnnotator if blocking else AsyncAnnotator
        return ann_type(config, desc, self.postfilters)

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


class _Annotator:
    def __init__(self, config, desc, postfilters):
        if desc is None:
            desc = 'Annotator created at {}'.format(datetime.datetime.utcnow())
        self.config = config
        self.description = desc
        self.postfilters = postfilters

    def is_ready(self):
        '''
        Is this annotator ready for processing documents?
        '''
        raise NotImplementedError

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
        Get an annotated document or collection.
        '''
        raise NotImplementedError

    @staticmethod
    def _annotate(server, params):
        '''
        Load and annotate one document or collection.
        '''
        document = server.load_one(**params)
        server.process(document)
        return document

    def _postfilter(self, document, filternames):
        '''
        Call each postfilter on the document.
        '''
        if 'true' in filternames:
            # Special value: use all filters.
            filternames = self.postfilters.keys()

        for name in filternames:
            try:
                postfilter = self.postfilters[name]
            except KeyError:
                raise ValueError('unknown postfilter: {}'.format(name))
            postfilter(document)

class BlockingAnnotator(_Annotator):
    '''
    Wrapper for a PipelineServer with simplified interface.
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._server = router.PipelineServer(self.config, lazy=False)

    @staticmethod
    def is_ready():
        return True

    def _get_annotated(self, params):
        return self._annotate(self._server, params)

class AsyncAnnotator(_Annotator):
    '''
    Wrapper for communicating with an annotator child process.
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._downstream = mp.Queue()
        self._upstream = mp.Queue()
        self._proc = mp.Process(
            target=self._run,
            args=(self.config, self._downstream, self._upstream))
        self._ready = False

        self._proc.start()
        self._downstream.put(None)  # start signal

    def __del__(self):
        self._downstream.put(None)  # sentinel
        self._proc.join(2)  # don't wait long for garbage collection
        if self._proc.exitcode is None:
            self._proc.terminate()

    def is_ready(self):
        '''
        Has the child process finished loading the termlist?
        '''
        if not self._proc.is_alive():
            raise RuntimeError('annotator has died')
        if not self._ready:
            if self._downstream.empty():
                self._ready = True
        return self._ready

    def _get_annotated(self, params):
        if not self.is_ready():
            raise RuntimeError('annotator not yet loaded')
        self._downstream.put(params)
        result = self._upstream.get()
        if isinstance(result, Exception):
            raise result
        return result

    @classmethod
    def _run(cls, config, requests, responses):
        '''
        Run a PipelineServer instance in a child process.
        '''
        server = router.PipelineServer(config, lazy=False)
        # Consume the start signal -- empty queue means ready.
        requests.get()

        for params in iter(requests.get, None):
            try:
                result = cls._annotate(server, params)
            except Exception as e:
                result = e
            responses.put(result)


# ============== #
# Miscellaneous. #
# ============== #

class IllegalAction(Exception):
    '''
    Attempt to perform a forbidden operation.
    '''
