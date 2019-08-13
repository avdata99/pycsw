FROM ubuntu:bionic

ARG APP_DIR=/app

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV PIPENV_VENV_IN_PROJECT=1
ENV PYCSW_CONFIG=/etc/pycsw/pycsw.cfg

RUN apt-get update && apt-get install -y \
  build-essential \
  libpq-dev \
  libxslt-dev \
  python-dev \
  python-pip \
  python-virtualenv

RUN pip install pipenv

WORKDIR $APP_DIR

COPY .datagov/pycsw.cfg $PYCSW_CONFIG

# Install dependencies
# setup.py does a lot of dynamic stuff which requires a handful of files.
COPY Pipfile Pipfile.lock setup.py VERSION.txt README.rst requirements.txt $APP_DIR/
COPY bin/* $APP_DIR/bin/
RUN pipenv install --dev

EXPOSE 8000

CMD ["pipenv", "run", "gunicorn", "pycsw.wsgi:application", "--bind", "0.0.0.0:8000", "--reload"]
