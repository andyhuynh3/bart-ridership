FROM ubuntu:18.04

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -yqq && \
    apt-get install -yqq python3-pip python3-dev postgresql postgresql-contrib libpq-dev

COPY ./Pipfile /app/Pipfile
COPY ./Pipfile.lock /app/Pipfile.lock

RUN pip3 install pipenv

WORKDIR /app

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

RUN pipenv install --system --deploy

COPY . /app

ADD /bin/entrypoint.sh entrypoint.sh

RUN chmod +x entrypoint.sh

WORKDIR /app

EXPOSE 8050

ENTRYPOINT [ "./entrypoint.sh" ]
