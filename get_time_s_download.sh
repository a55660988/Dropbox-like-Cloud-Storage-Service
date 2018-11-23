for n in {1..25}; do
    exetime="$(/usr/bin/time -f %e python3 client.py config.txt download small_files_${n}.bin download/ 2>>output_s_download.txt)"
    echo $exetime
done
