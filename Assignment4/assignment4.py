"""Assignment 4/5 of the course BDC"""


__author__ = "Ruben Hartsuiker"


#Imports
import sys
from mpi4py import MPI
import numpy as np


#Functions
def multiprocessing(fastq):
    comm = MPI.COMM_WORLD
    comm_size = comm.Get_size()
    my_rank = comm.Get_rank()

    if my_rank == 0:
        with open(fastq, 'r', encoding="UTF-8") as openfile:
            data = openfile.readlines()[3::4]
            chunks = [data[i*len(data) // comm_size: (i+1)*len(data) // comm_size] for i in range(comm_size)]
            comm.scatter(chunks)
            response_array = comm.gather(None)
            mean_phred_scores = np.mean(response_array[1:], axis=0)
            for i, val in enumerate(mean_phred_scores):
                print(f"{i}, {val}")
    else:
        chunk = comm.scatter(None)
        my_response = process_chunk(chunk)
        comm.gather(my_response)


def process_chunk(chunk):
    """Takes a fastq file quality lines and calculates the average quality of every base.
    input: fastq file quality lines
    output: stdout of 1D numpy array"""
    row_len = len(max(chunk, key=len))-1
    pscores = np.zeros((len(chunk),row_len))

    for i, line in enumerate(chunk):
        for j, char in enumerate(line.strip()):
            pscores[i][j] = 10 * np.log(ord(char)-33)

    return list(np.sum(pscores, axis=0) / np.sum(pscores != 0, axis=0))


#Main
def main(args):
    multiprocessing(args[1])


if __name__ == "__main__":
    sys.exit(main(sys.argv))
