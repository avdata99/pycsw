# Data.gov pycsw fork


## Features

- CKAN import
- Dynamically pull keywords from CKAN

## Usage

### Virtual Environment

To use various pieces of the system, you will need to activate the virtual environment:

    $ . .venv/bin/activate

### Database Setup

The database needs to be initialized with schema from our fork. This needs to be done with a database already in place and synced with the variables noted in `pycsw-collection.cfg`, default being `pycsw`.

    $ pycsw-ckan.py -c setup_db -f /etc/pycsw/pycsw-collection.cfg


## Development


### Prerequisites

- Docker v18
- Docker Compose


### Setup

Build the docker container.

    $ docker-compose build

If you are on a Linux system and your uid is not 1000 (`id -u`), you will need
to pass a build argument so the container can write to the local directory.

    $ docker-compose build --build-arg UID=$(id -u)


### Update dependencies

Update and regenerate the requirements file.

    $ docker-compose run --rm app .datagov/update-dependencies.sh
