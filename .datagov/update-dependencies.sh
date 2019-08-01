#!/bin/sh

set -eu

pipenv update
pipenv lock -r > requirements-datagov.txt
