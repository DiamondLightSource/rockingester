rockingest
=======================================================================

XChem Business Knowledge Unit.  Service, Client, API, persistent store.

Installation
-----------------------------------------------------------------------
::

    pip install rockingest

    rockingest --version

Documentation
-----------------------------------------------------------------------

See https://www.cs.diamond.ac.uk/rockingest for more detailed documentation.

Building and viewing the documents locally::

    git clone git+https://gitlab.diamond.ac.uk/scisoft/bxflow/rockingest.git 
    cd rockingest
    virtualenv /scratch/$USER/venv/rockingest
    source /scratch/$USER/venv/rockingest/bin/activate 
    pip install -e .[dev]
    make -f .rockingest/Makefile validate_docs
    browse to file:///scratch/$USER/venvs/rockingest/build/html/index.html

Topics for further documentation:

- TODO list of improvements
- change log


..
    Anything below this line is used when viewing README.rst and will be replaced
    when included in index.rst

