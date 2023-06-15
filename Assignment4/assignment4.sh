#!/bin/bash

#SBATCH --time 00:30:00
#SBATCH --nodes=1
#SBATCH --tasks=8
# SBATCH --nodelist=bin301
# SBATCH --cpus-per-task=8
# SBATCH --mem=1024M
# SBATCH --hosts=assemblix*

source /commons/conda/conda_load.sh

export fastq_file=/commons/Themas/Thema12/HPC/rnaseq.fastq
# export fastq_file=/commons/Themas/Thema12/HPC/rnaseq_selection.fastq
# export chunk_size=4000
export n_process=7 # + 1 for the master

mpirun -np ${n_process} python assignment4.py ${fastq_file}
#cat ${fastq_file} | awk 'NR % 4 == 0' | parallel --pipe -N${chunk_size} -j${n_cores} "python3 assignment4.py --pre" | python3 assignment4.py --post
