# OGER: OntoGene's Biomedical Entity Recogniser

A flexible, dictionary-based system for extracting biomedical terms from scientific articles.


## Installation

Install OGER from its repository using [pip](https://pip.pypa.io/):

    pip install git+https://gitlab.cl.uzh.ch/ontogene/pyogpipeline.git

Make sure you use Python 3's pip (eg. `pip3`).
Python 2.x is not supported.


## Usage

The installation process should provide you with an executable `oger`, which is the common starting point for a number of command-line tools.
Type `oger `_`CMD`_, followed by command-specific options, to run the desired tool:

```
oger run       # run an annotation job
oger serve     # start a REST API server
oger eval      # determine annotation accurateness
oger test      # run software tests
oger version   # print the version number
```

To see a list of available options for each command, run eg. `oger serve -h`.

As an alternative to the `oger` executable, you may run `python3 -m oger `_`CMD`_.


## Documentation

Documentation is maintained in our [gitlab wiki](https://gitlab.cl.uzh.ch/ontogene/pyogpipeline/wikis/home).


## Prerequisites

The pipeline runs on Python 3 only.
It was tested with Python 3.4 and 3.5.

The following third-party libraries need to be installed (pip should take care of this):

* [bottle](http://bottlepy.org) (needed only for the RESTful server)
* [lxml](http://lxml.de)
* [NLTK](http://www.nltk.org)


## Credits

Lenz Furrer  
furrer@cl.uzh.ch

Nico Colic  
ncolic@gmail.com

Adrian van der Lek  
adrian.vanderlek@uzh.ch

Tilia Ellendorff  
ellendorff@cl.uzh.ch
