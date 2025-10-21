# Setup Instructions

## Software prerequisites
To make it really easy for candidates to do this exercise regardless of their operating system, we've provided a containerised
way to run the exercise. To make use of this, you'll need to have [Docker](https://www.docker.com/products/docker-desktop/) (or a free equivalent like [Rancher](https://rancherdesktop.io/) or [Colima](https://github.com/abiosoft/colima)) installed
on your system.

To start the process, there is a `Dockerfile` in the root of the project.  This defines a linux-based container that includes Python 3.11,
Poetry and DuckDB.

To build the container:
```shell
docker build -t ee-data-engineering-challenge:0.0.1 .
```

To run the container after building it:

_Mac or Linux, or Windows with WSL:_
```shell
docker run --mount type=bind,source="$(pwd)/",target=/home/dataeng -it ee-data-engineering-challenge:0.0.1 bash
```

_Windows (without WSL):_
```shell
docker run --mount type=bind,source="%cd%",target=/home/dataeng -it ee-data-engineering-challenge:0.0.1 bash
```

Running the container opens up a terminal in which you can run the poetry commands that we describe next.

You could also proceed without the container by having Python 3.11 and [Poetry](https://python-poetry.org/)
directly installed on your system. In this case, just run the poetry commands you see described here directly in your
own terminal.

## Poetry

This exercise has been written in Python 3.11 and uses [Poetry](https://python-poetry.org/) as a dependency manager.
If you're unfamiliar with Poetry, don't worry -- the only things you need to know are:

1. Poetry automatically updates the `pyproject.toml` file with descriptions of your project's dependencies to keep 
   track of what libraries are being used.
2. To add a dependency for your project, use `poetry add thelibraryname`
3. To add a "dev" dependency (e.g. a test library or linter rather than something your program depends on)
   use `poetry add --dev thelibraryname`
4. To resolve dependencies and find compatible versions, use `poetry lock`
5. *Please commit any changes to your `pyproject.toml` and `poetry.lock` files and include them in your submission*
   so that we can replicate your environment.


In the terminal (of the running docker image), start off by installing the dependencies:
```shell
poetry install --with dev
```

> **Warning**
> If you're a Mac M1/M2 (arm-based) user, note that
> as of June 2023, DuckDB doesn't release pre-built `aarch64` linux
> wheels for the Python `duckdb` library (yet). This
> means that the dependency installation in the running container can take 
> some time (10mins?) as it compiles `duckdb` from source. 
> If you don't feel like waiting, you can build an `amd64` Docker container with
> by adding the `--platform amd64` flag to both the `docker build` and
> `docker run` commands above (i.e. you'll have to re-run those). 
> This image will run seamlessly on your Mac in emulation mode.


Now type
```shell
poetry run exercise --help
```
    
to see the options in CLI utility we provide to help you run the exercise. For example, you could do

```shell
poetry run exercise ingest-data
```

to run the ingestion process you will write in `ingest.py`


## Bootstrap solution

This repository contains a bootstrap solution that you can use to build upon. 

You can make any changes you like, as long as the solution can still be executed using the `exercise.py` script. 

The base solution uses [DuckDB](https://duckdb.org/) as the database, and for your solution we want you to treat it like a real (OLAP) data warehouse.
The database should be saved in the root folder of the project on the local disk as `warehouse.db`, as shown in the `tests/db_test.py` file.

We also provide the structure for:
* `equalexperts_dataeng_exercise/ingest.py` -  the entry point for running the ingestion process.
* `equalexperts_dataeng_exercise/outliers.py` - the entry point for running the outlier detection query.
* `equalexperts_dataeng_exercise/db.py` - is empty, but the associated test demonstrates interaction with an DuckDB database.
