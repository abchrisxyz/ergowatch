FROM postgres

ARG SCHEMA=https://raw.githubusercontent.com/ergoplatform/explorer-backend/8.1.0/modules/explorer-core/src/main/resources/db/V8__Schema.sql

ENV POSTGRES_DB ergo
ENV POSTGRES_USER ergo

ADD $SCHEMA /docker-entrypoint-initdb.d
RUN chown postgres /docker-entrypoint-initdb.d/*sql

EXPOSE 5432