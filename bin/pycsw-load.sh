#!/bin/sh

set -e

export PYCSW_CONFIG=${PYCSW_CONFIG:-/etc/ckan/pycsw-all.cfg}

virtualenv_bin=$(dirname "$0")
"${virtualenv_bin}/pycsw-ckan.py" -c load -f "$PYCSW_CONFIG"
"${virtualenv_bin}/pycsw-admin.py" optimize_db /etc/ckan/pycsw-all.cfg;
