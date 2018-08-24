#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


from setuptools import setup, find_packages

import oger


with open('README.md') as f:
    long_description = f.read()

setup(
    name='OGER',
    version=oger.__version__,
    description="OntoGene's Biomedical Entity Recogniser",
    long_description=long_description,
    author='Lenz Furrer',
    author_email='furrer@cl.uzh.ch',
    packages=find_packages(exclude='examples'),
    package_data={
        'oger.server': ['static/form.html'],
        'oger.test': ['testfiles/test_terms.tsv', 'testfiles/*/*'],
    },
    entry_points={
        'console_scripts': [
            'oger = oger.__main__:main',
        ],
    },
    install_requires=[
        'bottle',
        'lxml',
        'nltk',
    ],
)
