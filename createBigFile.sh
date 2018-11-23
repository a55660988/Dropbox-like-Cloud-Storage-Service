#! /bin/bash
FILE_DIR=big_files
FILENAME_PREFIX=big_files_
for n in {1..25}; do
    dd if=/dev/urandom of="$FILE_DIR"/"$FILENAME_PREFIX"$(printf %d "$n").bin bs=1000 count=60
done
