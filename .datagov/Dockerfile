FROM ubuntu:bionic

ARG APP_DIR=/home/pycsw/pycsw
ARG UID=1000

ENV PATH=/home/pycsw/.local/bin:$PATH
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

RUN apt-get update && apt-get install -y \
  build-essential \
  libpq-dev \
  libxslt-dev \
  python3-dev \
  python3-pip \
  python3-venv

RUN useradd --uid $UID --create-home pycsw

WORKDIR $APP_DIR
USER pycsw

COPY --chown=pycsw:pycsw . $APP_DIR

RUN pip3 install --user pipenv

RUN /home/pycsw/.local/bin/pipenv sync
