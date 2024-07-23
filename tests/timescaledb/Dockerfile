FROM pagila

WORKDIR /usr/src/pgcopydb

COPY ./copydb.sh copydb.sh
COPY ./rides.sql rides.sql
COPY ./fares.sql fares.sql
COPY ./tsdb_grants.sql tsdb_grants.sql
COPY ./nyc_data_rides.10k.csv nyc_data_rides.10k.csv
COPY ./000000010000000000000002.json 000000010000000000000002.json
COPY ./000000010000000000000002.sql 000000010000000000000002.sql

USER docker
CMD /usr/src/pgcopydb/copydb.sh
