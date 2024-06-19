#!/bin/bash
#SBATCH --job-name=assignment4
#SBATCH --nodes=5
#SBATCH --ntasks=3
#SBATCH --time=00:10:00
#SBATCH --cpus-per-task=4

srun python3 assignment4.py [ENTER FILE HERE] -o ./output.csv