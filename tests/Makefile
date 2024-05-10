# Copyright (c) 2021 The PostgreSQL Global Development Group.
# Licensed under the PostgreSQL License.

PGVERSION ?= 16
BUILD_ARGS = --build-arg PGVERSION=$(PGVERSION)

all: pagila pagila-multi-steps blobs unit filtering extensions \
	 cdc-wal2json cdc-test-decoding cdc-endpos-between-transaction cdc-low-level \
	 follow-wal2json follow-9.6 follow-data-only \
	 timescaledb timescaledb-test-decoding \
	 cdc-generated-column \
	 target-only-partition \
	 target-only-timescaledb-hypertable \
	 endpos-in-multi-wal-txn;

pagila: build
	$(MAKE) -C $@

pagila-multi-steps: build
	$(MAKE) -C $@

blobs: build
	$(MAKE) -C $@

unit: build
	$(MAKE) -C $@

filtering: build
	$(MAKE) -C $@

extensions: build
	$(MAKE) -C $@

cdc-wal2json: build
	$(MAKE) -C $@

cdc-test-decoding: build
	$(MAKE) -C $@

cdc-endpos-between-transaction: build
	$(MAKE) -C $@

cdc-low-level: build
	$(MAKE) -C $@

follow-wal2json: build
	$(MAKE) -C $@

follow-9.6: build
	$(MAKE) -C $@

follow-data-only: build
	$(MAKE) -C $@

endpos-in-multi-wal-txn: build
	$(MAKE) -C $@

timescaledb: build
	$(MAKE) -C $@

timescaledb-test-decoding: build
	$(MAKE) -C $@

cdc-generated-column: build
	$(MAKE) -C $@

target-only-partition: build
	$(MAKE) -C $@

target-only-timescaledb-hypertable: build
	$(MAKE) -C $@

build:
	docker build $(BUILD_ARGS) -t pagila -f Dockerfile.pagila .

.PHONY: all build
.PHONY: pagila pagila-multi-steps blobs unit filtering extensions
.PHONY: cdc-wal2json cdc-test-decoding cdc-low-level
.PHONY: follow-wal2json follow-9.6
.PHONY: endpos-in-multi-wal-txn
.PHONY: timescaledb timescaledb-test-decoding
.PHONY: cdc-generated-column
.PHONY: target-only-partition
.PHONY: target-only-timescaledb-hypertable
