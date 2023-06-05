#!/bin/bash

#SBATCH --time 0:10:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=size_in_MB=1024

source /commons/conda/conda_load.sh

export fastq_file=/commons/Themas/Thema12/HPC/part.fastq
export n_cores=4


