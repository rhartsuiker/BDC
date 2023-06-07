"""Assignment 3/5 of the course BDC"""


__author__ = "Ruben Hartsuiker"


# Imports
import os
import sys
import csv

import argparse as ap
import numpy as np


# Functions
def process_data(args):
    """Takes a fastq file and calculates the average quality of every base.
    input: file.fastq
    output: ndarray"""
    data = args.fastq.readlines()[3::4]

    pscores = np.empty((len(data),len(max(data, key=len))-1))
    pscores.fill(np.nan)

    for i, line in enumerate(data):
        for j, char in enumerate(line.strip()):
            pscores[i][j] = 10 * np.log(ord(char)-33)

    mean_pscores = np.nanmean(pscores, axis=0)
    # mean_pscores = pscores_w_nantail[~np.isnan(pscores_w_nantail)]

    # print(args.fastq.name)
    for i, val in enumerate(mean_pscores):
        print(f"{i}, {val}")



# Main
def main(args):
    """main function is called when module is used independently"""
    process_data(args)

    if args.csvfile is not None:
        args.csvfile.close()
    args.fastq.close()


if __name__ == "__main__":
    argparser = ap.ArgumentParser(description="Script voor Opdracht 3 van Big Data Computing")
    argparser.add_argument("-n", action="store", dest="n", required=True, type=int,
                        help="Aantal cores om te gebruiken.")
    argparser.add_argument("-o", action="store", dest="csvfile",
                           type=ap.FileType('w', encoding='UTF-8'), required=False,
                           help="CSV file om de output in op te slaan. Default is output naar terminal STDOUT")
    argparser.add_argument("-f", action="store", dest="fastq",
                           type=ap.FileType('r', encoding='UTF-8'), help="somehelp")
                        #    nargs='+', help="Minstens 1 Illumina Fastq Format file om te verwerken")
    sys.exit(main(argparser.parse_args()))
