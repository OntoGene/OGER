#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Nico Colic, XII 2016


'''
Test runs for the top-level call.
'''


# TODO
# * cover more dimensions of parametrisation:
#   - termlist settings (normalisation, abbrev detection, extra fields)
#   - postfilter hook
#   - parallelisation
#   - input/output-format dependent options (single_section etc.)
# * consider server mode (esp. PipelineServer's load_one() method)
# * ease the construction of test units
#   (not all of them have to go through the command-line interface)
# * add an output validator (check BioC DTD etc.)
# * add evaluation set (using the term_coverage module)

# OUTPUT OPTIONS
# * xml, bioc, tsv for entities
# * brat txt, brat ann
# * articles as xml, odin, bioc

#########
# IMPORTS
#########

import sys
import shlex
import logging
import argparse
import tempfile
from os.path import join, dirname, realpath
import os
from datetime import datetime

from ..ctrl.run import run
from ..ctrl import parameters
from .. import doc


#############################
# SETUP, HELPERS, GLOBALS ETC
#############################

HERE = dirname(realpath(__file__))
TESTFILES = join(HERE, 'testfiles')
TERMLIST = join(TESTFILES, 'test_terms.tsv')
CACHE = tempfile.TemporaryDirectory()
IDFILES = join(TESTFILES, 'idfiles')
OUTPUT = 'oger-test'
OUTPUT_FORMATS = doc.OUTFMTS

TESTCASES = [
    'txt_directory',
    'txt_id',
    'txt_collection',
    'txt_json',
    'pubtator',
    'pubtator_fbk',
    'pxmlgz',
    'pxml_directory',
    'pxml_id',
    'bioc_xml',
    'bioc_json',
    'download_pubmed',
    'download_pmc',
    'download_bad_pmc',
    'download_fictious_pmc',
    'download_random_pmc',
]

testlogger = logging.getLogger('test')


def main():
    '''
    Run one or more test cases from the command line.
    '''
    ap = argparse.ArgumentParser(
        description=__doc__)
    ap.add_argument(
        '-o', '--output', default=OUTPUT, metavar='PATH',
        help='directory for the test output (default: %(default)s)')
    ap.add_argument(
        '-v', '--verbose', action='store_true',
        help='include detailed progress info on STDERR')
    ap.add_argument(
        'testcases', nargs='+', choices=['all'] + TESTCASES,
        metavar='TESTCASE',
        help='any selection of the following, or "all" to run all tests: '
        + ', '.join(TESTCASES))
    args = ap.parse_args()
    run_tests(**vars(args))


def run_tests(testcases, output, verbose):
    '''
    Run test cases.
    '''
    # Setup logging and output directories.
    outputdir = join(output, datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
    os.makedirs(outputdir)
    setup_logging(verbose, outputdir)

    outcomes = []
    if 'all' in testcases:
        testcases = TESTCASES

    # Run the tests.
    for name in testcases:
        testlogger.info("Testing %s", name)
        try:
            globals()[name](outputdir)

        except Exception as e:
            testlogger.info('%s failed (%s)', name, e)
            # Put this on the root logger, so we don't clobber non-verbose
            # output with the stack trace.
            logging.exception("Stack trace:")
            outcomes.append((name, repr(e)))

        else:
            testlogger.info("%s passed", name)
            outcomes.append((name, 'ok'))

    testlogger.info(summary(outcomes))


def setup_logging(verbose, outputdir):
    '''
    Two handlers: a detailed one for the logfile, a configurable one for STDERR.
    '''
    logfile = logging.FileHandler(join(outputdir, 'test.log'))
    logfile.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    stderr = logging.StreamHandler()
    stderr.setFormatter(logging.Formatter('%(message)s'))

    # The pipeline's progress info all goes to the root logger.
    rootlogger = logging.getLogger()
    rootlogger.setLevel(logging.INFO)
    rootlogger.addHandler(logfile)

    # The test module's messages go here.
    testlogger.setLevel(logging.INFO)

    if verbose:
        # Everything is printed to STDERR.
        rootlogger.addHandler(stderr)
    else:
        # Only the test module's messages are printed to STDERR.
        testlogger.addHandler(stderr)


def summary(outcomes):
    '''
    Produce a nicely formatted summary of the test results.
    '''
    colwidth = max(len(n) for n, _ in outcomes)
    successful = 0
    msg = ['\n*** SUMMARY ***']
    for name, result in outcomes:
        msg.append('  {}  {}'.format(name.ljust(colwidth), result))
        successful += int(result == 'ok')
    msg.append('\n{}/{} test cases successful'
               .format(successful, len(outcomes)))
    msg.append('***************')
    return '\n'.join(msg)


def make_arguments(format, output,
                   pointers=None, pointer_type='glob', mode='document',
                   **kwargs):
    """Generates argument string to call the pipeline with"""
    args = dict(
        pointer_type=pointer_type,
        mode=mode,
        format=format,
        output=output,
        input=join(TESTFILES, format),
        termlist=TERMLIST,
        cache=CACHE.name,
        miscellaneous='',
        )
    args.update(kwargs)

    arguments = """
        -t {pointer_type}
        -m {mode}
        -f {format}
        -i {input}
        -o {output}
        -c termlist1_path {termlist}
        -c termlist_cache {cache}
        -c termlist_skip_header true
        -e {export}
        {miscellaneous}
        """.format(**args)

    if pointers is None and pointer_type == 'glob':
        pointers = '*'
    if pointers:
        arguments += '-- ' + pointers

    return arguments

def run_with_arguments(arguments_string):
    argument_list = shlex.split(arguments_string.strip())
    arguments = parameters.parse_cmdline(argument_list)
    run(**arguments)


def outdir(outputdir, *args):
    'Create an output directory name with datetime and test-case name.'
    # Get the name of the calling function.
    testcase = sys._getframe(1).f_code.co_name
    subcase = '_'.join((testcase,) + args)
    return join(outputdir, subcase)


##########################
# ACTUAL TESTING FUNCTIONS
##########################

def txt_directory(outputdir):
    for output_format in OUTPUT_FORMATS:
        testlogger.info('-> %s', output_format)
        output = join(outdir(outputdir), output_format)
        arguments = make_arguments(format='txt',
                                   output=output,
                                   export=output_format,
                                   pointers='*.txt')
        run_with_arguments(arguments)

    # It doesn't crash when there's an empty file,
    # or a file in a different format
    # but it crashes if the encoding is messed up
    # (this is why I added *.txt for testing)

def txt_id(outputdir):
    pointers = join(IDFILES, 'txt_ids.txt')
    for output_format in OUTPUT_FORMATS:
        testlogger.info('-> %s', output_format)
        output = join(outdir(outputdir), output_format)
        arguments = make_arguments(pointer_type='id',
                                   format='txt',
                                   output=output,
                                   export=output_format,
                                   pointers=pointers,
                                   miscellaneous='-b pubmed')
        run_with_arguments(arguments)

def txt_collection(outputdir):
    for output_format in OUTPUT_FORMATS:
        testlogger.info('-> %s', output_format)
        output = join(outdir(outputdir), output_format)
        arguments = make_arguments(format='txt',
                                   output=output,
                                   mode='collection',
                                   export=output_format,
                                   pointers='*.txt')
        run_with_arguments(arguments)

    # TODO output is just called 'None'
    # Is there a way to make that nicer?

def txt_json(outputdir):
    _multiple_outfmts(outdir(outputdir), 'txt_json')

def pubtator(outputdir):
    _multiple_outfmts(outdir(outputdir), 'pubtator')

def pubtator_fbk(outputdir):
    _multiple_outfmts(outdir(outputdir), 'pubtator_fbk')

def pxmlgz(outputdir):
    _multiple_outfmts(outdir(outputdir), 'pxml.gz')

def pxml_directory(outputdir):
    _multiple_outfmts(outdir(outputdir), 'pxml')

def _multiple_outfmts(outputdir, fmt):
    for mode in ['collection', 'document']:
        export = ' '.join(OUTPUT_FORMATS)
        testlogger.info('-> %s (%s mode)', export, mode)
        output = '_'.join((outputdir, mode))
        misc = '-c fn-format-out {fmt}/{id}.{ext}'
        arguments = make_arguments(format=fmt,
                                   output=output,
                                   mode=mode,
                                   export=export,
                                   miscellaneous=misc)
        run_with_arguments(arguments)

def pxml_id(outputdir):
    testlogger.info('-> tsv (ID pointers)')
    pointers = join(IDFILES, 'pxml_pmids.txt')
    arguments = make_arguments(format='pxml',
                               output=outdir(outputdir, 'pmid'),
                               pointers=pointers,
                               pointer_type='id',
                               export='tsv')
    run_with_arguments(arguments)

    # missing ids are downloaded
    testlogger.info('-> tsv (ID pointers, pubmed fallback)')
    pointers = join(IDFILES, 'pxml_pmids_dl.txt')
    arguments = make_arguments(format='pxml',
                               output=outdir(outputdir, 'pmid', 'dl'),
                               pointers=pointers,
                               pointer_type='id',
                               export='tsv',
                               miscellaneous='-b pubmed')
    run_with_arguments(arguments)

def bioc_xml(outputdir):
    output = join(outputdir, 'bioc_xml')
    arguments = make_arguments(format='bioc_xml',
                               output=output,
                               export='xml')
    run_with_arguments(arguments)

def bioc_json(outputdir):
    output = join(outputdir, 'bioc_json')
    arguments = make_arguments(format='bioc_json',
                               output=output,
                               export='pubtator')
    run_with_arguments(arguments)

def download_pubmed(outputdir):
    pointers = join(IDFILES, 'pubmed_pmids.txt')
    output = join(outputdir, 'pubmed')
    arguments = make_arguments(format='pubmed',
                               output=output,
                               pointers=pointers,
                               pointer_type='id',
                               export='odin')
    run_with_arguments(arguments)

def download_pmc(outputdir):
    # running with a PMCID we know works
    good_pmcids = join(IDFILES, 'good_pmcids.txt')
    output = join(outputdir, 'pmc')
    arguments = make_arguments(format='pmc',
                               output=output,
                               pointers=good_pmcids,
                               pointer_type='id',
                               export='xml')
    run_with_arguments(arguments)

def download_bad_pmc(outputdir):
    # some PMC articles do not allow DLing the whole article
    bad_pmcids = join(IDFILES, 'bad_pmcids.txt')
    output = join(outputdir, 'pmc')
    arguments = make_arguments(format='pmc',
                               output=output,
                               pointers=bad_pmcids,
                               pointer_type='id',
                               export='xml')
    run_with_arguments(arguments)

def download_fictious_pmc(outputdir):
    # running with PMCIDs that don't exist
    # raises a ValueError with a list of missed IDs.
    fictious_pmcids = join(IDFILES, 'fictious_pmcids.txt')
    output = join(outputdir, 'pmc')
    arguments = make_arguments(format='pmc',
                               output=output,
                               pointers=fictious_pmcids,
                               pointer_type='id',
                               export='xml')
    with open(fictious_pmcids) as f:
        ids = [i.strip() for i in f]
    try:
        run_with_arguments(arguments)
    except ValueError as e:
        if getattr(e, 'ids', None) != ids:
            raise

def download_random_pmc(outputdir):
    # generate random list of pmcids to be dowloaded
    # to make testing deterministic, this is commented
    # import random
    random_pmcids = join(IDFILES, 'random_pmcids.txt')
    # with open(random_pmcids, 'w+') as f:
    #    for r in random.sample(range(1, 5000000), 100):
    #        f.write(str(r) + "\n")

    output = join(outputdir, 'pmc')
    misc = '-c ignore-load-errors true'
    arguments = make_arguments(format='pmc',
                               output=output,
                               pointers=random_pmcids,
                               pointer_type='id',
                               export='xml',
                               miscellaneous=misc)
    run_with_arguments(arguments)
