#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2015--2016


'''
Run OGER from the command line.

This module provides:
- invocation of the command-line interface
- multiprocessing logic (usable as a module as well)
- progress info through logging
'''


import multiprocessing as mp
import logging

from . import parameters
from . import router


def main():
    '''
    Run as script: parse command-line arguments.
    '''
    params = parameters.parse_cmdline()
    run(**params)


def run(n_workers=parameters.WORKERS, **params):
    '''
    Run the pipeline with parsed arguments.
    '''
    try:
        _run(n_workers, params)
    except Exception:
        logging.exception('Top-level crash:')
        raise


def _run(n_workers, params):
    '''
    Run the pipeline with parsed arguments (wrapped function).
    '''
    # Get a central router instance.
    # This sets up the logger and gives access to the ultimate configs
    # after processing the whole stack of config levels.
    master_conf = router.Router(**params)

    # Short-cut: Reduce overhead for single-thread execution.
    if n_workers <= 1:
        logging.info('Run in single-thread mode.')
        run_serial(master_conf)
        logging.info('Finished processing.')
        return

    # Avoid parallel term-list loadings by ensuring a pickled version.
    master_conf.ensure_cached_termlist()
    params['termlist_force_reload'] = False  # don't reload in the workers

    # Set up and start the parallel workers.
    logging.info('Start %d parallel workers.', n_workers)
    q = mp.Queue()
    workers = []
    for i in range(n_workers):
        p = mp.Process(target=run_worker,
                       args=(params, q, i+1))
        p.start()
        workers.append(p)

    # Iterate over the pointers.
    logging.info('Feed %s sequence to the workers.', master_conf.p.iter_mode)
    for pointer in master_conf.iter_pointers():
        q.put(pointer)

    # Tell the workers to stop and wait for them to finish.
    for _ in workers:
        q.put(None)
    for p in workers:
        p.join()
    logging.info('Joined all workers.')


def run_worker(params, q, n):
    '''
    Process articles with pointers from a queue.
    '''
    conf = router.Router(**params)
    try:
        run_serial(conf, iter(q.get, None))
    except Exception:
        logging.exception('Worker %d crashed:', n)
        raise
    else:
        logging.info('Worker %d finished.', n)


def run_serial(conf, pointers=None):
    '''
    Run the pipeline for a series of articles or collections.
    '''
    server = router.PipelineServer(conf, lazy=False)
    level = 'collection' if conf.p.iter_mode == 'collection' else 'article'
    for content in server.iter_contents(pointers):
        logging.info('Processing %s %s', level, content.id_)
        server.process(content)
        server.postfilter(content)
        server.export(content)
