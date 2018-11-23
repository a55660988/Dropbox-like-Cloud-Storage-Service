#! /bin/bash
FILE_DIR=small_files
FILENAME_PREFIX=small_files_
for n in {1..25}; do
    dd if=/dev/urandom of="$FILE_DIR"/"$FILENAME_PREFIX"$(printf %d "$n").bin bs=1000 count=4
done
