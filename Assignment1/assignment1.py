"""Assignment 1/5 of the course BDC"""

__author__ = "Ruben Hartsuiker"

# Imports
import sys
import csv
import os
import argparse as ap
import multiprocessing as mp
import numpy as np


# Functions
def multiprocess_job(cores, csvfile, func, fastq_file, multiple_files):
    """Takes a function and work data and pools the data over a given number of cores
    Input: parsed ar"""
    # pool the qual lines and convert them to phred scores
    with mp.Pool(cores) as pool:
        phred_scores = pool.map(func, fastq_file.readlines()[3::4])

    # calc mean over the columns
    mean_phred_scores = np.mean(phred_scores, axis=0)

    # if outfile was given write to csv else write to commandline
    if multiple_files:
        if csvfile is not None:
            with open(f"{os.path.abspath(fastq_file.name)}.output.csv",
                      "w", encoding="UTF-8") as outfile:
                writer = csv.writer(outfile)
                for i, row in enumerate(mean_phred_scores):
                    writer.writerow([i] + list(row))
        else:
            print(fastq_file.name)
            for i, val in enumerate(mean_phred_scores):
                print(f"{i}, {val[0]}")
    else:
        if csvfile is not None:
            writer = csv.writer(csvfile)
            for i, row in enumerate(mean_phred_scores):
                writer.writerow([i] + list(row))
        else:
            for i, val in enumerate(mean_phred_scores):
                print(f"{i}, {val[0]}")

    fastq_file.close()


def convert_line_to_phred(qual_line):
    """Takes a quality line from a fastq file and converts it to phred scores.
    input: quality line from fastq file
    output: ndarray"""
    return [[10 * np.log(ord(c)) for c in p] for p in qual_line]


# Main
def main(args):
    """main function is called when module is used independently"""
    if len(args.fastq_files) > 1:
        for fastq_file in args.fastq_files:
            multiprocess_job(args.n, args.csvfile, convert_line_to_phred, fastq_file, True)
    else:
        multiprocess_job(args.n, args.csvfile, convert_line_to_phred, args.fastq_files[0], False)

    if args.csvfile is not None:
        args.csvfile.close()


if __name__ == "__main__":
    argparser = ap.ArgumentParser(description="Script voor Opdracht 1 van Big Data Computing")
    argparser.add_argument("-n", action="store", dest="n", required=True, type=int,
                        help="Aantal cores om te gebruiken.")
    argparser.add_argument("-o", action="store", dest="csvfile",
                           type=ap.FileType('w', encoding='UTF-8'), required=False,
                           help="CSV file om de output in op te slaan. Default is output naar terminal STDOUT")
    argparser.add_argument("fastq_files", action="store", type=ap.FileType('r'), nargs='+',
                           help="Minstens 1 Illumina Fastq Format file om te verwerken")
    sys.exit(main(argparser.parse_args()))
