#!/bin/bash

#SBATCH --time 00:02:00
#SBATCH --ntasks=4
#SBATCH --job-name assignment3BDC
#SBATCH --output assignment3.out

#
#for arg in "$@"
#do
#    parallel --pipe-part -a "$arg" --block -4 -j5 echo {} | python3 $STR
#done
