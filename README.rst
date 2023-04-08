rockingester
=======================================================================

XChem Business Knowledge Unit.  Service, Client, API, persistent store.

Installation
-----------------------------------------------------------------------
::

    pip install rockingester

    rockingester --version

Legacy information
-----------------------------------------------------------------------

The formulatrix pipeline code is here:

    /dls/science/groups/i04-1/software/luigi_pipeline/imager_pipe
    
This is a working directory of 

    https://github.com/xchem/formulatrix_pipe

The formulatrix pipeline puts images here:

    /dls/science/groups/i04-1/software/luigi_pipeline/imager_pipe/SubwellImages/9b5j_2023-03-23_RI1000-0276-3drop

Does this have anything useful?

    https://gitlab.diamond.ac.uk/cnp64921/dlsformulatrix.uploader/-/blob/master/formulatrix_uploader.py


As for the luigi stuff, one of the scripts in the /dls/science/groups/i04-1/software/luigi_pipeline/imager_pipe/ creates the filename after extracting info from the SQL server on the Formulatrix. I think the 9b5j part is the barcode, yes, but you might have to dig into the scripts there to find out the logic behind it all.

By the way if you want to access the FOrmulatrix /Rockmaker from Windows you can brose to access the RockMaker Imager \\cs04r-nas01-02\rockimager

You see like:
\\cs04r-nas01-02\rockimager\rockimager\RockMakerStorage\WellImages\539\plateID_14539\batchID_72673

The batch ID proably relates to the inspection... a plate is inspected multiple times and each inspection has a batch ID.
There may be some clues in the get_barcodes.py script in the imager_pipe directory.

Documentation
-----------------------------------------------------------------------

See https://www.cs.diamond.ac.uk/rockingester for more detailed documentation.

Building and viewing the documents locally::

    git clone git+https://gitlab.diamond.ac.uk/scisoft/bxflow/rockingester.git 
    cd rockingester
    virtualenv /scratch/$USER/venv/rockingester
    source /scratch/$USER/venv/rockingester/bin/activate 
    pip install -e .[dev]
    make -f .rockingester/Makefile validate_docs
    browse to file:///scratch/$USER/venvs/rockingester/build/html/index.html

Topics for further documentation:

- TODO list of improvements
- change log


..
    Anything below this line is used when viewing README.rst and will be replaced
    when included in index.rst

