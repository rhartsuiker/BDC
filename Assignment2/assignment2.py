"""Assignment 2/5 of the course BDC"""

__author__ = "Ruben Hartsuiker"

# Imports
import os
import sys
import time
import queue
import csv

from multiprocessing.managers import BaseManager

import argparse as ap
import multiprocessing as mp
import numpy as np

# Constants
POISONPILL = "MEMENTOMORI"
ERROR = "DOH"
AUTHKEY = b'whathasitgotinitspocketsesss?'


# Functions
def make_server_manager(ip, port, authkey):
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = queue.Queue()
    result_q = queue.Queue()

    # This is based on the examples in the official docs of multiprocessing.
    # get_{job|result}_q return synchronized proxies for the actual Queue
    # objects.
    class QueueManager(BaseManager):
        """p"""

    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)

    manager = QueueManager(address=(ip, port), authkey=authkey)
    manager.start()
    return manager


def run_server(fn, args):
    """p"""
    # Start a shared manager server and access its queues
    manager = make_server_manager(args.host, args.port, AUTHKEY)
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()

    if not args.fastq_files:
        return

    for fastq in args.fastq_files:
        data = fastq.readlines()[3::4]
        data_chunks = [data[i*len(data) // args.chunks: (i+1)*len(data) // args.chunks] for i in range(args.chunks)]
        for chunk in data_chunks:
            shared_job_q.put({'fn': fn, 'arg': [chunk, len(max(data, key=len))-1]})

    time.sleep(2)

    results = []
    while True:
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            if len(results) == len(data_chunks) * len(args.fastq_files):
                break
        except queue.Empty:
            time.sleep(1)
            continue
    # Tell the client process no more data will be forthcoming
    shared_job_q.put(POISONPILL)
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(5)
    manager.shutdown()

    # concatenate all chunk results per file and calculate the mean pscore over the collums
    mean_pscores_nan = [np.nanmean(np.concatenate([rd["result"] for rd in results[i:i+args.chunks]]), axis=0)
                        for i in range(0,len(results),args.chunks)]
    mean_phred_scores = [pscores[~np.isnan(pscores)] for pscores in mean_pscores_nan]

    # if outfile was given write to csv else write to commandline
    if len(args.fastq_files) > 1:
        for fastq_file, scores in zip(args.fastq_files, mean_phred_scores):
            if args.csvfile is not None:
                with open(f"{os.path.abspath(fastq_file.name)}.output.csv",
                        "w", encoding="UTF-8") as outfile:
                    writer = csv.writer(outfile)
                    for i, val in enumerate(scores):
                        writer.writerow([i, val])
            else:
                print(fastq_file.name)
                for i, val in enumerate(scores):
                    print(f"{i}, {val}")
    else:
        if args.csvfile is not None:
            writer = csv.writer(args.csvfile)
            for i, val in enumerate(mean_phred_scores[0]):
                writer.writerow([i, val])
        else:
            for i, val in enumerate(mean_phred_scores[0]):
                print(f"{i}, {val}")

    # finalize by closing the files opened by argparser
    if args.csvfile is not None:
        args.csvfile.close()
    for openfile in args.fastq_files:
        openfile.close()


#################################################################################################################################

def make_client_manager(ip, port, authkey):
    """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
    """
    class ServerQueueManager(BaseManager):
        """p"""

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(ip, port), authkey=authkey)
    manager.connect()

    return manager


def run_client(cores, host, port):
    """p"""
    manager = make_client_manager(host, port, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, cores)


def run_workers(job_q, result_q, num_processes):
    """p"""
    processes = []
    for p in range(num_processes):
        temp = mp.Process(target=peon, args=(job_q, result_q))
        processes.append(temp)
        temp.start()
    for temp in processes:
        temp.join()


def peon(job_q, result_q):
    """p"""
    while True:
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                return
            try:
                result = job['fn'](job['arg'])
                result_q.put({'job': job, 'result': result})
            except NameError:
                result_q.put({'job': job, 'result': ERROR})

        except queue.Empty:
            time.sleep(1)


#################################################################################################################################

def get_quality(args):
    """Takes a fastq file and calculates the average quality of every base.
    input: file.fastq
    output: ndarray"""
    chunk = args[0]
    row_len = args[1]
    phred_scores = np.empty((len(chunk),row_len,))
    phred_scores.fill(np.nan)

    for i, line in enumerate(chunk):
        for j, char in enumerate(line.strip()):
            phred_scores[i][j] = 10 * np.log(ord(char)-33)
    return phred_scores


# Main
def main(args):
    """main function is called when module is used independently"""
    # starts the server side - processes data in a job queue for the client side
    if args.s:
        server = mp.Process(target=run_server, args=(get_quality, args))
        server.start()
        server.join()
    # starts the client side - gets jobs from the server side queue
    if args.c:
        if args.n is not None:
            client = mp.Process(target=run_client, args=(args.n, args.host, args.port))
        else:
            client = mp.Process(target=run_client, args=(mp.cpu_count(), args.host, args.port))
        client.start()
        client.join()


if __name__ == "__main__":
    argparser = ap.ArgumentParser(description="Script voor Opdracht 2 van Big Data Computing;  Calculate PHRED scores over the network.")

    mode = argparser.add_mutually_exclusive_group(required=True)
    mode.add_argument("-s", action="store_true", help="Run the program in Server mode; see extra options needed below")
    mode.add_argument("-c", action="store_true", help="Run the program in Client mode; see extra options needed below")

    server_args = argparser.add_argument_group(title="Arguments when run in server mode")
    server_args.add_argument("-o", action="store", dest="csvfile", type=ap.FileType('w', encoding='UTF-8'),
                   required=False, help="CSV file om de output in op te slaan. Default is output naar terminal STDOUT")
    server_args.add_argument("fastq_files", action="store", type=ap.FileType('r'), nargs='*',
                              help="Minstens 1 Illumina Fastq Format file om te verwerken")
    server_args.add_argument("--chunks", action="store", type=int, required=False)

    client_args = argparser.add_argument_group(title="Arguments when run in client mode")
    client_args.add_argument("-n", action="store", dest="n", required=False, type=int,
                            help="Aantal cores om te gebruiken per host.")
    client_args.add_argument("--host", action="store", type=str, help="The hostname where the Server is listening")
    client_args.add_argument("--port", action="store", type=int, help="The port on which the Server is listening")

    sys.exit(main(argparser.parse_args()))
