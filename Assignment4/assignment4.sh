#!/bin/bash
#SBATCH --job-name=assignment4
#SBATCH --nodes=5
#SBATCH --ntasks=5
#SBATCH --time=00:08:00
#SBATCH --cpus-per-task=1
#SBATCH --partition=assemblix
#SBATCH --nodelist=assemblix2019

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ASSIGNMENT_PATH=$SCRIPT_DIR"/assignment4.py"

for var in "$@"
do
  mpirun -n 4 python3 "${ASSIGNMENT_PATH}" "$var" -o ./output.csv
done
