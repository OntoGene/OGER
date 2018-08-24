#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Central entry point to all OGER executables.
'''


import sys


def main():
    '''
    Delegate calls to the appropriate scripts.
    '''
    try:
        # Extract and match the first command-line argument.
        action = commands[sys.argv[1]]
    except (IndexError, KeyError):
        msg = 'usage: %s {%s} [OPTIONS...]' % (sys.argv[0], ','.join(commands))
        sys.exit(msg)
    else:
        # Modify sys.argv to not interfere with the commands' argument parsing.
        sys.argv[:2] = [' '.join(sys.argv[:2])]
        action()


def run():
    '''
    Command-line interface.
    '''
    import oger.ctrl.run
    oger.ctrl.run.main()


def serve():
    '''
    Server for the REST API.
    '''
    from oger.server import restfulserver
    restfulserver.main()


def coverage():
    '''
    Annotation coverage evaluation.
    '''
    from oger.eval import term_coverage
    term_coverage.main()


def version():
    '''
    Print the version number.
    '''
    import oger
    print('OGER', oger.__version__)


def test():
    '''
    Run tests.
    '''
    from oger.test import tester
    tester.main()


commands = dict(
    run=run,
    serve=serve,
    eval=coverage,
    test=test,
    version=version,
)


if __name__ == '__main__':
    main()
