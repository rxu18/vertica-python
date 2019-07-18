FROM python:3.7-alpine

COPY . /vp
WORKDIR /vp
RUN apk add build-base krb5-dev
RUN pip install kerberos pytz python-dateutil tox
ENTRYPOINT ["tox","-e py37"]

