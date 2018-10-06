#!/bin/bash
for i in $(seq 1 9); do
  # for j in $( ls ); do
    # echo copy $j to kfc $i
    peg scp to-rem kfc  $i prediction_producer.py /home/ubuntu/eye_of_sauron/
  # done
done
