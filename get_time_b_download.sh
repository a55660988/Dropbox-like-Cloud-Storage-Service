for n in {1..25}; do
    exetime="$(/usr/bin/time -f %e python3 client.py config.txt download big_files_${n}.bin download/ 2>>output_b_download.txt)"
    echo $exetime
done
