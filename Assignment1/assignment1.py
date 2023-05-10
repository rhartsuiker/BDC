"""Assignment 1/5 of the course BDC"""

__author__ = "Ruben Hartsuiker"

# Imports
import sys
import csv
import argparse

import multiprocessing as mp
import numpy as np


# Functions
def get_quality(fastq_file):
    """Takes a fastq file and calculates the average quality of every base.
    input: file.fastq
    output: ndarray"""
    with open(fastq_file, 'r') as openfile:
        lines = openfile.readlines()
        sum_of_quality = [[10 * np.log(ord(c)) for c in p] for p in lines[3::4]]
    return np.mean(sum_of_quality, axis=0)


# Main
def main(args):
    """main function is called when module is used independently"""
    with mp.Pool(args.cores) as pool:
        results = pool.map(get_quality, args.fastq_files)

    if args.outfile is not None:
        with open(args.outfile, 'w', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow([""] + args.fastq_files)
            for i, row in enumerate(np.stack(results, axis=1)):
                writer.writerow([i] + list(row))
    else:
        print(results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', type=int, dest='cores', required=True)
    parser.add_argument('-o', type=str, dest='outfile')
    parser.add_argument('fastq_files', nargs=argparse.REMAINDER)
    sys.exit(main(parser.parse_args()))
