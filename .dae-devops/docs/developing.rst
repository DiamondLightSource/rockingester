.. # ********** Please don't edit this file!
.. # ********** It has been generated automatically by dae_devops version 0.5.3.
.. # ********** For repository_name rockingester

Developing
=======================================================================

If you plan to make change to the code in this repository, you can use the steps below.

Clone the repository::

    $ cd <your development area>
    $ git clone https://github.com/diamondlightsource/rockingester/rockingester.git

It is recommended that you install into a virtual environment so this
installation will not interfere with any existing Python software.
Make sure to have at least python version 3.10 then::

    $ python3 -m venv /scratch/$USER/myvenv
    $ source /scratch/$USER/myvenv/bin/activate
    $ pip install --upgrade pip

Install the package in edit mode which will also install all its dependencies::

    $ cd rockingester
    $ pip install -e .[dev,docs]

Now you may begin modifying the code.


.. # dae_devops_fingerprint 96d41b708572d1d5049fab9b6abfb97e
