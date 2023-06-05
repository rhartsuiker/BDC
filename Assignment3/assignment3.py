"""Assignment 3/5 of the course BDC"""


__author__ = "Ruben Hartsuiker"


# Imports
import os
import sys
import time
import queue
import csv

import argparse as ap
import numpy as np


# Functions
def get_quality(chunk):
    """Takes a fastq file and calculates the average quality of every base.
    input: file.fastq
    output: ndarray"""
    col_count = np.empty((len(chunk),1000))
    col_count.fill(np.nan)
    pscores = np.empty((len(chunk),1000))
    pscores.fill(np.nan)

    for i, line in enumerate(chunk):
        for j, char in enumerate(line.strip()):
            col_count[i][j] = 1
            pscores[i][j] = 10 * np.log(ord(char)-33)

    col_count = np.sum(col_count, axis=0)
    sum_of_pscores = np.sum(pscores, axis=0)

    return (sum_of_pscores, col_count)


# Main
def main(args):
    """main function is called when module is used independently"""
    process_data(args)


if __name__ == "__main__":
    argparser = ap.ArgumentParser(description="Script voor Opdracht 3 van Big Data Computing")
    argparser.add_argument("-n", action="store", dest="n", required=True, type=int,
                        help="Aantal cores om te gebruiken.")
    argparser.add_argument("-o", action="store", dest="csvfile",
                           type=ap.FileType('w', encoding='UTF-8'), required=False,
                           help="CSV file om de output in op te slaan. Default is output naar terminal STDOUT")
    argparser.add_argument("fastq_files", action="store", type=ap.FileType('r'), nargs='+',
                           help="Minstens 1 Illumina Fastq Format file om te verwerken")
    sys.exit(main(argparser.parse_args()))
