FROM pagila

WORKDIR /usr/src/pgcopydb

COPY ./copydb.sh copydb.sh
COPY ./rides.sql rides.sql
COPY ./fares.sql fares.sql
COPY ./tsdb_grants.sql tsdb_grants.sql
COPY ./nyc_data_rides.10k.csv nyc_data_rides.10k.csv
COPY ./tests tests

USER docker
CMD /usr/src/pgcopydb/copydb.sh
