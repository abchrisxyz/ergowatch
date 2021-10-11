FROM postgres:14

ARG SCHEMA=https://raw.githubusercontent.com/ergoplatform/explorer-backend/9.4.3/modules/explorer-core/src/main/resources/db/V9__Schema.sql

ENV POSTGRES_DB ergo
ENV POSTGRES_USER ergo

ADD $SCHEMA /docker-entrypoint-initdb.d
RUN chown postgres /docker-entrypoint-initdb.d/*sql

EXPOSE 5432