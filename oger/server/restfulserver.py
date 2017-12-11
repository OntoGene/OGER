#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016--2017


'''
A RESTful API for OGER.
'''


import os
import sys
import json
import logging
import argparse
import datetime
import multiprocessing as mp

from lxml import etree as ET
from bottle import get, post, response, request, error, HTTPError
from bottle import run as run_bottle

from ..ctrl import router, parameters
from .expfmts import EXPORT_FMTS, export


# ============= #
# Server setup. #
# ============= #

BOTTLE_HOST = '0.0.0.0'
BOTTLE_PORT = 12321

PL_SETTINGS = 'restful-settings.ini'

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
        '-d', '--debug', dest='bottle.debug', action='store_true',
        help='display exceptions in the served responses')

    pl = ap.add_argument_group(title='OGER configuration')
    pl.add_argument(
        '-s', '--settings', dest='pl.settings', metavar='PATH', nargs='+',
        default=PL_SETTINGS,
        help='load OGER settings from one or more .ini config files')
    pl.add_argument(
        '-c', '--config', nargs=2, action='append', default=[],
        metavar=('KEY', 'VALUE'),
        help="any other setting, passed on directly to OGER's config "
             '(repeat option -c for multiple key-value pairs)'
             '%(default).0s')

    # Argument preprocessing.
    args = ap.parse_args(namespace=parameters.NestedNamespace())
    pl_args, bottle_args = vars(args.pl), vars(args.bottle)
    # Raise -c args to the top level.
    pl_args.update((k.replace('-', '_'), v) for k, v in args.config)

    init(pl_args, bottle_args)


def init(pl_conf, bottle_conf):
    '''
    Setup and start the servers.
    '''
    # A global variable is needed here because the routes are mapped
    # to top-level functions.
    global ann_manager

    # Pipeline config.
    pl_params = parameters.Params(**pl_conf)
    # Organise logging after basicConfig was called in the Params constructor,
    # but before anything interesting happens (like termlist loading).
    setup_logging()
    # Get the default OGER server.
    ann_manager = AnnotatorManager(pl_params)

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

MODE = '/{}<ann:re:(-[0-9A-F]+)?>'
FETCH = MODE.format('fetch')
UPLOAD = MODE.format('upload')

FETCH_SOURCES = ('pubmed', 'pmc')
UPLOAD_FMTS = ('txt', 'bioc', 'pxml', 'nxml', 'pxml.gz')

SOURCE = '/<source:re:{}>'.format('|'.join(FETCH_SOURCES))
IN_FMT = '/<in_fmt:re:{}>'.format('|'.join(UPLOAD_FMTS))
OUT_FMT = '/<out_fmt:re:{}>'.format('|'.join(EXPORT_FMTS))
DOCID_WILDCARD = '/<docid:re:[1-9][0-9]*>'


@get(FETCH + SOURCE + OUT_FMT + DOCID_WILDCARD)
@post(FETCH + SOURCE + OUT_FMT + DOCID_WILDCARD)
def fetch_article(ann, source, out_fmt, docid):
    'Fetch and process one article.'
    logging.info('GET request: article %s from %s in %s format',
                 docid, source, out_fmt)
    return load_process_export(ann, [docid], source, out_fmt, docid=None)


@post(UPLOAD + IN_FMT + OUT_FMT)
@post(UPLOAD + IN_FMT + OUT_FMT + DOCID_WILDCARD)
def upload_article(ann, in_fmt, out_fmt, docid=None):
    'Process one uploaded article.'
    logging.info('POST request: %s -> %s (DOCID: %s)', in_fmt, out_fmt, docid)
    return load_process_export(ann, request.body, in_fmt, out_fmt, docid)


def load_process_export(ann, data, in_fmt, out_fmt, docid):
    'Load, process, and export an article (or collection).'
    try:
        annotator = ann_manager.get(ann.lstrip('-'))
    except KeyError:
        raise HTTPError(400, 'unknown annotator')
    try:
        return annotator.process((data, in_fmt, out_fmt, docid))
    except Exception:
        logging.exception('Fatal: data: %.40r, fmt: %s -> %s, ID: %s',
                          data, in_fmt, out_fmt, docid)
        raise


@get('/')
@post('/')
def web_ui():
    'Serve a HTML page with an input form.'
    if 'settings' in request.params:
        # If there is a settings param, try to load an annotator.
        try:
            settings = json.loads(request.params['settings'])
            desc = request.params.get('description')
            target = ann_manager.add(settings, desc=desc)
        except Exception as e:
            raise HTTPError(400, e)
    else:
        # Otherwise, check the params for a target annotator.
        target = request.params.get('target')
    return build_web_page(INPUT_FORM, target)


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


@error(404)
def error404(err):
    'Return a usage message.'
    msg = '''\
Invalid resource locator.

Valid "fetch" request (GET or POST method):
/fetch/SOURCE/OUT_FMT/DOC_ID

Valid "upload" requests (POST method only):
/upload/IN_FMT/OUT_FMT
/upload/IN_FMT/OUT_FMT/DOC_ID

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
    msg = '<html><body><p>{}</p></body></html>\n'.format(
        msg.replace('\n\n', '</p><p>').replace('\n', '<br/>'))
    # raise HTTPError(400, msg)
    return msg


# ================= #
# Legacy interface. #
# ================= #

@get(OUT_FMT + DOCID_WILDCARD)
def legacy_fetch(out_fmt, docid):
    'Obsolete route for fetching.'
    return fetch_article(None, 'pubmed', out_fmt, docid)

@post(IN_FMT + OUT_FMT)
@post(IN_FMT + OUT_FMT + DOCID_WILDCARD)
def legacy_upload(in_fmt, out_fmt, docid=None):
    'Obsolete route for uploading.'
    return upload_article(None, in_fmt, out_fmt, docid)


# ============================= #
# Handling multiple annotators. #
# ============================= #

@post('/ann/load')
def load_annotator():
    '''
    Load a new annotator, if necessary.

    The settings are expected in the JSON payload.
    '''
    try:
        settings = request.json['settings']
        desc = request.json.get('description')
        name = ann_manager.add(settings, desc=desc)
    except Exception as e:
        raise HTTPError(400, e)
    return {'name': name}

@post('/ann/check/<ann:re:[0-9A-f]+>')
def check_annotator(ann):
    '''
    Check if this annotator is ready.
    '''
    try:
        ann = ann_manager.get(ann)
    except Exception as e:
        raise HTTPError(400, e)
    if ann.is_ready():
        return ann.description
    else:
        return 'Loading...'


class AnnotatorManager(object):
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
        self._default_settings = default
        self.n = n
        self.active = {}                  # all active servers
        self.additional = []              # names of additional servers
        self.default = self.add({}, blocking=True)  # default server name
        self.additional.remove(self.default)

    def add(self, params, desc=None, blocking=False):
        '''
        Create a new annotator and give it a name.

        In case an existing one has the same settings,
        return its name instead.
        '''
        config = router.Router(self._default_settings, **params)
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
            self.additional.remove(name)
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

    @staticmethod
    def start(config, desc, blocking):
        'Initiate a new annotation server.'
        if blocking:
            return BlockingAnnotator(config, desc)
        else:
            return AsyncAnnotator(config, desc)

    @classmethod
    def key(cls, conf):
        'Select the relevant parts of this configuration.'
        struct = (conf.p.word_tokenizer,
                  conf.p.sentence_tokenizer,
                  tuple(tuple(ep.iterparams()) for ep in conf.p.recognizers))
        return cls.hashtoken(struct)

    _hash_mask = (1 << sys.hash_info.width) - 1

    @classmethod
    def hashtoken(cls, structure):
        'Create a hex token from a hashable structure.'
        h = hash(structure)
        return '{:X}'.format(h & cls._hash_mask)  # get rid of the sign


class _Annotator:
    def __init__(self, desc):
        if desc is None:
            desc = 'Annotator created at {}'.format(datetime.datetime.utcnow())
        self.description = desc

    def is_ready(self):
        '''
        Is this annotator ready for processing documents?
        '''
        raise NotImplementedError

    def process(self, args):
        '''
        Load, process, and export one document or collection.
        '''
        ctype, data = self._process(args)
        response.content_type = ctype
        return data

    def _process(self, args):
        raise NotImplementedError

    @staticmethod
    def _load_process_export(server, data, in_fmt, out_fmt, docid):
        article = server.load_one(data, in_fmt, docid=docid)
        server.process(article)
        return export(server.conf, article, out_fmt)

class BlockingAnnotator(_Annotator):
    '''
    Wrapper for a PipelineServer with simplified interface.
    '''
    def __init__(self, config, desc):
        super().__init__(desc)
        self._server = router.PipelineServer(config)

    @staticmethod
    def is_ready():
        return True

    def _process(self, args):
        return self._load_process_export(self._server, *args)

class AsyncAnnotator(_Annotator):
    '''
    Wrapper for communicating with an annotator child process.
    '''
    def __init__(self, config, desc):
        super().__init__(desc)
        self._downstream = mp.Queue()
        self._upstream = mp.Queue()
        self._proc = mp.Process(
            target=self._run, args=(config, self._downstream, self._upstream))
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

    def _process(self, args):
        if not self.is_ready():
            raise RuntimeError('annotator not yet loaded')
        self._downstream.put(args)
        result = self._upstream.get()
        if isinstance(result, Exception):
            raise result
        return result

    @classmethod
    def _run(cls, config, requests, responses):
        '''
        Run a PipelineServer instance in a child process.
        '''
        server = router.PipelineServer(config)
        # Consume the start signal -- empty queue means ready.
        requests.get()

        for args in iter(requests.get, None):
            try:
                result = cls._load_process_export(server, *args)
            except Exception as e:
                result = e
            responses.put(result)
