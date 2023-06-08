"""Assignment 3/5 of the course BDC"""


__author__ = "Ruben Hartsuiker"


# Imports
import sys

import numpy as np


# Functions
def preprocess_data(data):
    """Takes a fastq file quality lines and calculates the average quality of every base.
    input: fastq file quality lines
    output: stdout of 1D numpy array"""
    row_len = len(max(data, key=len))-1
    pscores = np.zeros((len(data),row_len))

    for i, line in enumerate(data):
        for j, char in enumerate(line.strip()):
            pscores[i][j] = 10 * np.log(ord(char)-33)

    print(list(np.sum(pscores, axis=0) / np.sum(pscores != 0, axis=0)))


def postprocess_data(data):
    """Takes stdin from preprocess output and calculates the mean phred scores column wise
    input: commandline stdin
    output: stdout of enumerated score list"""
    mean_phred_scores = np.mean([[float(e.strip().strip("[")) for e in l.strip("[").split(",")] for l in data.split("]")[:-1]], axis=0)

    for i, val in enumerate(mean_phred_scores):
        print(f"{i}, {val}")


if __name__ == "__main__":
    if sys.argv[1] == "--pre":
        sys.exit(preprocess_data(sys.stdin.readlines()))
    elif sys.argv[1] == "--post":
        sys.exit(postprocess_data(sys.stdin.read()))
    else:
        sys.exit()
