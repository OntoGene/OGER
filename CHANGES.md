# Changes to the OG Python3 Pipeline


## Version 1.2.1

- bug fixes in input/output formats


## Version 1.2

- new input and output formats *pubtator* and *pubtator_fbk*
- new normalization method *mask*
- new parameters `byte-offsets-in` and `byte-offsets-out` for interpreting and producing BioC documents with byte-based offsets
- improved PipelineServer class for library usage
- fixed a security hole involving eval()
- allow specifying a pretrained sentence splitter


## Version 1.1

- parameter additions:
  * multiple postfilters allowed
  * new parameter `field-names` interacts with ex-termlist parameter `extra-fields`
  * new output formats *bioc_json* and *pubanno_json*
- REST service:
  * improved API (more consistent)
  * fetch/upload requests accept input/output parameters (query params)
  * postfilters can be selected through query params


## Version 1.0

- parameter changes:
  * more intuitive input parameters: `iter-mode` distinguishes *document* and *collection*, while `pointer-type` is *glob* or *id*
  * parameter `elements` (positional in CLI) renamed to `pointer`
  * new parameters `ignore-load-errors` and `sentence-split`
  * BioC metadata specified using JSON
- OGER refactored to an importable Python package; can be installed with pip
- web interface: multiple annotators (can be sent from the BTH)
- termlist can be provided over HTTP/HTTPS/FTP
- more and parametrisable normalisation methods (choose stemmer/Unicode normalisation)
- BioC loader preserves \<infon\> elements at all levels
- various minor improvements and bugfixes


## Version 0.8

- changed and extended the syntax of the REST API (the old one still works)
- added a browser interface for the REST API
- a few bugfixes


## Version 0.7

- added a test suite
- PMC now works without crashing the runtime
- new input sources: *becalmabstracts* and *becalmpatents* (requests to BeCalm API)
- new output formats: *becalm_tsv* and *becalm_json*
- various bugfixes
- many improvements of the server mode


## Version 0.6

- new output format: ODIN XML (iat2)
- options for printing attributes in Brat output
- new example filter: suppress submatches
- allow multiple entity recognisers (entails some changes to the option naming)
- optionally do locally-cached abbreviation detection
- new normalisation method: stemming (Lancaster)
- start/stop functionality for the RESTful server
- numerous bugfixes and minor optimisations


## Version 0.5

- new input format: Pubmed Central full-text through the efetch API
- Brat annotation enriched with the rest of the termlist fields, using the "Annotator notes" tag
- dropped PyBioC dependency; BioC XML is now directly parsed/written
- collection mode is now also available for the input format "pubmed" (efetch)
- new feature: postfilter hook (specify a Python function that modifies the annotated articles before writing)
- new feature: fallback format (eg. try to use on-disk files first, but for those unavailable fall back to a pubmed request)
- more NLP methods registered: RegexTokenizer (for output filters), greektranslit (ER normalisation)
- and various minor extensions and bugfixes (see the commit messages)


## Version 0.4

- new switch "iter_mode" for explicitly choosing iteration mode (PMID vs. directory vs. collection), rather than a flag for overriding an implicit default
- new iter_mode "collection", which produces a single output file for a collection of articles
- faster term look-up (using two dicts now)
- term matching is based on a special (non-NLTK), customisable tokenisation
- additional termlist fields are exported to TSV and XML as well
- separate flags for controlling output details (include\_headers in TSV, sentence_level annotation in BioC), instead of the overloaded pretty\_print flag
- encoding: rather than being controlled by the locale, encoding is now always UTF-8 for all non-XML input and output documents


## Version 0.3

- flexible term list: extra columns can be specified through an option (`termlist_extra_fields`)
- articles can be processed in directory mode, ie. the input set is defined through directory contents (with optional glob filtering) rather than through a list of PMIDs.
- faster PubMed download by requesting multiple articles at once (up to 1000 by default); also the dependency from BioPython is removed in that the efetch API is directly accessed with urllib (however, the pickle-caching was dropped)
- multiple output formats per run can be specified
- new output format "Brat" (creates a plain-text file along with a standoff annotation file)
- new input format "pxml.gz", which parses the Medline chunks (gzipped PubMed abstracts)


## Version 0.2

- new input format: BioC (reads and writes at the collection level)
- PMIDs can be specified in the INI config file, even when running the pipeline from run.py.
- default-settings.ini is read by default, if present (that way, the pipeline can be configured to be run without command-line args, while still avoiding conflicts with upstream repos)
- run.py is easier to use as a module through the top-level function `run()`, which accepts any keyword args and runs the same way as in script mode
