#!/bin/bash

#SBATCH --time 02:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8

source /commons/conda/conda_load.sh

export fastq_file=/commons/Themas/Thema12/HPC/rnaseq.fastq
export chunk_size=4000
export n_cores=8

cat ${fastq_file} | awk 'NR % 4 == 0' | parallel --pipe -N${chunk_size} -j${n_cores} "python3 assignment3.py --pre" | python3 assignment3.py --post
