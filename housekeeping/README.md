# Housekeeping

Housekeeping is a tool designed to remove unnecessary WAL files (.sql & .json) from the `$PGCOPYDB_DIR/cdc` directory.
These files have already been applied to the target and are no longer needed.

The tool operates by maintaining an ordered list of all files in the working directory, sorted by their hexadecimal names.
It then retrieves the current WAL file from the source DB and locates its index within the ordered list of files.
Once the index is identified, Housekeeping removes all preceding files, retaining a buffer of three files for safety.
This operation is performed every 5 minutes.

To function correctly, Housekeeping requires certain environment variables:

```shell
# Required.
export PGCOPYDB_SOURCE_PGURI=''
export PGCOPYDB_TARGET_PGURI=''
export PGCOPYDB_DIR=''

# Optional.
export HOUSEKEEPING_INTERVAL=300 # Seconds.
```

## Usage
```shell
# Use python >= 3.10
python -m venv venv

source venv/bin/activate

pip install -r requirements.txt

python housekeeping.py
```
