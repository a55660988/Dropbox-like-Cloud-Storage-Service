for n in {1..25}; do
    exetime="$(/usr/bin/time -f %e python3 client.py config.txt upload big_files/big_files_${n}.bin 2>>output_b_upload.txt)"
    echo $exetime
done
