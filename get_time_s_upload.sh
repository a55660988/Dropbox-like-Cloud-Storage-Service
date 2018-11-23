for n in {1..25}; do
    exetime="$(/usr/bin/time -f %e python3 client.py config.txt upload small_files/small_files_${n}.bin 2>>output_s_upload.txt)"
    echo $exetime
done
