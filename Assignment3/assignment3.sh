#!/bin/bash

#SBATCH --time 0:10:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=size_in_MB=1024

source /commons/conda/conda_load.sh

export fastq_file=/commons/Themas/Thema12/HPC/rnaseq_selection.fastq
export fastq_files=fastq_files.txt
export n_cores=1

cat ${fastq_files} | parallel "python3 assignment3.py -n ${n_cores} -f {}" > out.csv
