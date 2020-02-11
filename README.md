# OGER: OntoGene's Biomedical Entity Recogniser

A flexible, dictionary-based system for extracting biomedical terms from scientific articles.


## Demo

A demo version of OGER is hosted at <https://pub.cl.uzh.ch/purl/OGER>.


## Installation

Install OGER from its repository using [pip](https://pip.pypa.io/):

    pip install git+https://github.com/OntoGene/OGER.git

Make sure you use Python 3's pip (eg. `pip3`).
Python 2.x is not supported.  
Note: By default, `pip` installs Python packages at the system level, which typically requires root/admin privileges.
To install OGER to a user-owned location, set the `--user` flag.


## Usage: Command-line Tool

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


## Usage: Python Library

Get config and start a pipeline server:
```pycon
>>> from oger.ctrl.router import Router, PipelineServer
>>> conf = Router(termlist_path='testfiles/test_terms.tsv')
>>> pl = PipelineServer(conf)
```
Note: test files can be downloaded [here](oger/test/testfiles).

Load a text document from disk (using the included test suite):
```pycon
>>> doc = pl.load_one('testfiles/txt/13373697.txt', 'txt')
>>> doc
<Article with 1 subelement at 0x7f8b2135d860>
>>> doc.text
'[The kind and the measure of ventilation disorders in tuberculous bronchostenosis in relation to its localization]. \n'
```

Download a collection of articles from PubMed:
```pycon
>>> coll = pl.load_one(['21436587', '21436588'], fmt='pubmed')
>>> coll
<Collection with 2 subelements at 0x7f8b215f4cc0>
>>> coll[0]
<Article with 2 subelements at 0x7f8b2156a5f8>
>>> coll[0][0]
<Section with 1 subelement at 0x7f8b2156a358>
>>> coll[0][0].text
'Human prostate cancer metastases target the hematopoietic stem cell niche to establish footholds in mouse bone marrow.\n'
```

Run entity recognition:
```pycon
>>> pl.process(coll)
>>> entity = next(coll[0].iter_entities())
>>> entity.text, entity.start, entity.end
('Human', 0, 5)
>>> entity.cid
'9606'
>>> entity.info
('organism', 'Homo sapiens', 'NCBI Taxonomy', '9606', 'DC')
```

Export to disk:
```pycon
>>> with open('output/collection.json', 'w', encoding='utf8') as f:
...     pl.write(coll, 'bioc_json', f)
```

The second argument specifies the output format.
OGER supports BioC (XML and JSON), PubTator, PubAnnotation JSON, BioNLP stand-off, and CoNLL, among others.
A full list of available formats is given [here](https://github.com/OntoGene/OGER/wiki/run#output-parameters) (see the `export-format` parameter).


## Documentation

Documentation is maintained in the [GitHub wiki](https://github.com/OntoGene/OGER/wiki).


## Prerequisites

OGER runs on Python 3.4+.

The following third-party libraries need to be installed (pip should take care of this):

* [bottle](http://bottlepy.org) (needed only for the RESTful server)
* [lxml](http://lxml.de)
* [NLTK](http://www.nltk.org)


## Publications

If you use OGER in an academic context, please cite us:

Lenz Furrer, Anna Jancso, Nicola Colic, and Fabio Rinaldi (2019):
**OGER++: hybrid multi-type entity recognition**.
In: *Journal of Cheminformatics* 11:7.
DOI: [10.1186/s13321-018-0326-3](https://doi.org/10.1186/s13321-018-0326-3)
| [PDF](https://jcheminf.biomedcentral.com/track/pdf/10.1186/s13321-018-0326-3)
| [bibtex](https://github.com/OntoGene/OGER/wiki/attachments/furrer-et-al-2019.bib) |

Lenz Furrer and Fabio Rinaldi (2017):
**OGER: OntoGene's Entity Recogniser in the BeCalm TIPS Task**.
In: *Proceedings of the BioCreative V.5 Challenge Evaluation Workshop*, pp. 175–182.
| [PDF](https://github.com/OntoGene/OGER/wiki/attachments/furrer-rinaldi-2017.pdf)
| [bibtex](https://github.com/OntoGene/OGER/wiki/attachments/furrer-rinaldi-2017.bib) |

Marco Basaldella, Lenz Furrer, Carlo Tasso, and Fabio Rinaldi (2017):
**Entity recognition in the biomedical domain using a hybrid approach**.
In: *Journal of Biomedical Semantics* 8:51.
DOI: [10.1186/s13326-017-0157-6](https://doi.org/10.1186/s13326-017-0157-6)
| [PDF](https://jbiomedsem.biomedcentral.com/track/pdf/10.1186/s13326-017-0157-6)
| [bibtex](https://github.com/OntoGene/OGER/wiki/attachments/basaldella-et-al-2017.bib) |


## License

OGER offers a dual licensing model.

You can redistribute OGER and/or modify it under the terms
of the GNU Affero General Public License as published by the
Free Software Foundation, either version 3 of the License,
or (at your option) any later version.

The GNU Affero General Public License is designed to ensure
that if a modified version is distributed or made accessible
on a server (e.g. in a SaaS offering), the modified source
code becomes available to the community.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

If you wish to use OGER under alternate terms, you may obtain
a commercial license to OGER.  Please contact us for more
information (<http://www.ontogene.org>).
