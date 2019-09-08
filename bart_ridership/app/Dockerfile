FROM ubuntu:18.04

ENV DEBIAN_FRONTEND noninteractive
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

RUN apt-get update -yqq && apt-get install -yqq python3-dev python3-pip postgresql postgresql-contrib libpq-dev

COPY ./bart_ridership/app/Pipfile usr/src/app/Pipfile
COPY ./bart_ridership/app/Pipfile.lock usr/src/app/Pipfile.lock

RUN pip3 install pipenv

WORKDIR usr/src/app

RUN pipenv install --skip-lock --system --deploy

COPY ./bart_ridership/app/entrypoint.sh /usr/src/app/entrypoint.sh

RUN chmod +x /usr/src/app/entrypoint.sh

COPY ./bart_ridership /usr/src/app/bart_ridership

RUN ls

EXPOSE 8000

ENTRYPOINT [ "./entrypoint.sh" ]
