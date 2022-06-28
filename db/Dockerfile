FROM postgres:14

ENV POSTGRES_DB ergo
ENV POSTGRES_USER ergo

ADD ./schema.sql /docker-entrypoint-initdb.d
RUN chown postgres /docker-entrypoint-initdb.d/*sql

EXPOSE 5432
