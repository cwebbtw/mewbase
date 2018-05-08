FROM postgres
ENV POSTGRES_USER mewbase
ENV POSTGRES_PASS mewbase
ADD ./mewbase-core/src/main/resources /opt/mewbase/resources
RUN cp /opt/mewbase/resources/postgres-schema.sql /docker-entrypoint-initdb.d/postgres-schema.sql
