## General structure
Use the `make_arguments` function to produce a arguments string given the parameters that can be used to run the pipeline from the command line, or using the `run_with_arguments` function. `make_arguments` makes some minor simplifications to writing an arguments string, such as setting default directories for input and output.

Currently, all the test use the same term list, which is indicated in the `TERMLIST` global variable and used by `make_arguments`.

Every test function tests one combination of mode, input and output format, using the aforementioned functions. The test functions are then called by `main`. Within name, a list of `test_cases` (containing the names of the test functions) indicates which functions will be tested (thought this will be changed, since this means you need to change the code to test different combinations of parameters on the pipeline).

## How to run
To execute test file, change to the `test` directory (`cd test`), then run `python tester.py`.

As explained before, add the names of the test functions (such as `bioc` or `download_pmc`) to the `test_cases` list in `main` to control which functions are executed.

## How to add test cases
To conform with the other test cases, it would be good if the output of your new test case ends up in the `OUTPUT_TODAY` directory. Apart from that, it is not necessary that you use `make_arguments`, but it does make writing new cases quickly much easier.
