"""
A script containing all the necessary functions to process a fastq file through the
multiprocessing module with distributive computing. To do this you start a server and a client.

Both the server and client need the following arguments:
- fastq_file: the name of the fastq file(s)
- chunks: The amount of chunks the data will get cut up in
- host: the hostname of the server, so for the client you will fill in the same host name
- port: the port that gets used by the server, this needs to be the same for the client as well

The arguments that are passed to just the server are:
- o: the output file that is created ahead of time, this is optional

The arguments that are passed to just the client are:
- n: The amount of cores being used, the default is 1
"""
import argparse as ap
import csv
import multiprocessing as mp
import queue
import time
from itertools import zip_longest, islice
from multiprocessing.managers import BaseManager
import logging
import sys
POISONPILL = "MEMENTOMORI"
ERROR = "DOH"
IP = ''
PORTNUM = 5381
AUTHKEY = b'whathasitgotinitspocketsesss?'


def command_line_args():
    """
    Parse command line
    :return: args: the arguments that were given to the program
    """
    argparser = ap.ArgumentParser(
        description="Script assignment 2 of Big Data Computing;  "
                    "Calculate PHRED scores over the network.")
    mode = argparser.add_mutually_exclusive_group(required=True)
    mode.add_argument("-s", action="store_true",
                      help="Run the program in Server mode; see extra options needed below",
                      dest="server")
    mode.add_argument("-c", action="store_true",
                      help="Run the program in Client mode;see extra options needed below",
                      dest="client")
    server_args = argparser.add_argument_group(title="Arguments when run in server mode")
    server_args.add_argument("-o", action="store", dest="csvfile",
                             type=ap.FileType('w', encoding='UTF-8'),
                             required=False,
                             help="CSV file to save the output to. "
                                  "The default is output to the terminal with STDOUT")
    server_args.add_argument("fastq_files", action="store", type=ap.FileType('r'), nargs='*',
                             help="Minstens 1 Illumina Fastq Format file om te verwerken")
    server_args.add_argument("--chunks", action="store", type=int, required=False, dest="chunks")

    client_args = argparser.add_argument_group(title="Arguments when run in client mode")
    client_args.add_argument("-n", action="store",
                             dest="n", required=False, type=int,
                             help="Aantal cores om te gebruiken per host.", default=1)
    client_args.add_argument("--host", action="store", type=str,
                             help="The hostname where the Server is listening", dest="host")
    client_args.add_argument("--port", action="store", type=int,
                             help="The port on which the Server is listening", dest="port")
    return argparser.parse_args()


def fastq_reader(arguments):
    """
    Read each fastq file and return a list of lists containing the
    characters per column for each file
    :param arguments: the arguments that were given to the program from the command line
    :return: A list of lists containing the quality characters per column
    """
    col_lines = []
    logging.debug(f"Reading {len(arguments.fastq_files)} fastq files.")
    for fastqfile in arguments.fastq_files:
        with open(fastqfile.name, 'r') as fastq:
            col_lines.append(fastq_lines(fastq))
            logging.debug(f"Read {fastqfile.name}")
    logging.debug(f"Finished reading all fastq files: length {len(col_lines)}.")
    return col_lines


def fastq_lines(fastq):
    """
    Gather the quality lines from the fastq file and return a column read
    through from the fastq file
    :param fastq: an open fastq file
    :return: the columns from the fastq file
    """
    # Read through the fastq file by quality lines.
    logging.debug("Starting islice the fastq file.")
    rows = [line.strip() for line in islice(fastq, 3, None, 4)]
    logging.debug("Finished islice the fastq file.")
    # zip_longest to prevent loss of characters because fastq files can be differing lengths
    logging.debug("Starting to zip_longest the fastq file.")
    columns = [list(col) for col in zip_longest(*rows, fillvalue=None)]
    logging.debug("Finished zipping the fastq file.")
    return columns


def make_server_manager(host, port, authkey):
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = queue.Queue()
    result_q = queue.Queue()

    # This is based on the examples in the official docs of multiprocessing.
    # get_{job|result}_q return synchronized proxies for the actual Queue
    # objects.
    class QueueManager(BaseManager):
        """
        Class QueueManager to manage the queue's as based on the example in
        the multiprocessing docs
        """
        pass

    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)

    manager = QueueManager(address=(host, port), authkey=authkey)
    manager.start()
    print(f'Server started at port {port}')
    return manager


def runserver(fn, data):
    """
    Run the server given the function that processes the data and the data itself.
    :param fn: the function that does the bulk of the calculation
    :param data: the data to process
    """
    # Start a shared manager server and access its queues
    args = command_line_args()
    manager = make_server_manager(args.host, args.port, AUTHKEY)
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()
    collected_column_length = []
    if not data:
        print("Gimme something to do here!")
        return
    print("Sending data!")
    logging.debug(f"Sending data to the workers.")
    # Create chunks of the data
    for columns in data:
        collected_column_length.append(len(columns))
        for index in range(0, len(columns), args.chunks):
            current_index = index
            logging.debug(f"Sending data to the workers: {current_index}")
            shared_job_q.put({'fn': fn, 'arg': columns[current_index:current_index+args.chunks],
                              "pos": [i for i in range(current_index, current_index+args.chunks)]})

    logging.debug(f"Sent data to the workers: {current_index}")
    time.sleep(2)
    # Calculate what the length of the results should be
    length_results = round(sum(collected_column_length) / args.chunks) + 1
    logging.debug(f"Length of results: {length_results}")
    results = []
    timeout = 0
    while timeout < 3600:
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            if len(results) == length_results:
                print("Got all results!")
                break
        except queue.Empty:
            time.sleep(1)
            timeout += 1
            if timeout > 3600:
                logging.debug("Server Timeout waiting for results, exiting")
                # note; not properly shutting down the server here
                # problem with queue?
                sys.exit(1)
            else:
                continue
    # Tell the client process no more data will be forthcoming
    print("Time to kill some peons!")
    shared_job_q.put(POISONPILL)
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(5)
    print("Aaaaaand we're done for the server!")
    manager.shutdown()
    write_results(args.fastq_files, args.csvfile, results)


def write_results(fastqfiles, csvfile, results):
    """
    Write results to screen or, if a csv file is given, write to a csv file.
    :param fastqfiles: the fastq_files arguments
    :param csvfile: the csv_file argument
    :param results: the results of the distributive process
    """
    logging.debug("Writing results to screen or file.")
    counter = 0
    results = sorted(results, key=lambda dictionary: dictionary['pos'])
    if args.csvfile:
        if len(fastqfiles) > 1:
            for fastq_file in fastqfiles:
                filename = fastq_file.name.split("/")[-1]
                with open(f"output_{filename}.csv", "w") as csv_file:
                    writer = csv.writer(csv_file)
                    for result in results:
                        for count, i in enumerate(result["result"]):
                            writer.writerow([result["pos"][count], i])
        else:
            with csvfile as csv_file:
                writer = csv.writer(csv_file)
                for result in results:
                    for count, i in enumerate(result["result"]):
                        writer.writerow([result["pos"][count], i])
        print("Saved results to %s" % csvfile.name)
    else:
        for result in results:
            for count, i in enumerate(result["result"]):
                if result["pos"][count] == 1:
                    print(fastqfiles[counter].name)
                    counter += 1
                print(result["pos"][count], i)


def average_phred_score(quality_lines):
    """
    Calculate the average quality score of a list of phred score characters
    :param quality_lines: a list containing phred score character
    :return: the average score of a list of phred score characters
    """
    logging.debug("Calculating the average phred score.")
    average_scores = []
    for quality_line in quality_lines:
        phred_score_line = [ord(character) - 33 for character in
                            quality_line if character is not None]
        average_scores.append(sum(phred_score_line) / len(phred_score_line))
    return average_scores


def make_client_manager(ip_address, port, authkey):
    """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
    """
    logging.debug(f"Connecting client manager to server  {ip_address}:{port}")
    class ServerQueueManager(BaseManager):
        """
        Class ServerQueueManager to manage the queue's as based on the example in
        the multiprocessing docs
        """
        pass

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(ip_address, port), authkey=authkey)
    timeout = 0
    while True:
        try:
            time.sleep(2)
            manager.connect()
        except ConnectionRefusedError:
            logging.debug(f"Can't connect (yet) to {ip_address}:{port}")
            timeout += 1
            if timeout > 300: # 10 minutes
                logging.debug("Client manager connection Timeout reached, exiting.")
                sys.exit(1)
            else:
                continue
        else:
            logging.debug(f'Client connected to {ip_address}:{port}')
            return manager


def runclient(num_processes):
    """
    Run the client with the number of processors specified
    :param num_processes: Number of processes to use
    """
    logging.debug("Starting the client.")
    args = command_line_args()
    manager = make_client_manager(args.host, args.port, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, num_processes)


def run_workers(job_q, result_q, num_processes):
    """
    Run the workers that will use the job_q to determine what data needs processing
    and write the results into the result_q
    :param job_q: The queue that contains all the jobs to do
    :param result_q: The queue where the results are gathered in
    :param num_processes: Number of processors to use
    """
    logging.debug("Starting the workers.")
    processes = []
    for _ in range(num_processes):
        temp = mp.Process(target=peon, args=(job_q, result_q))
        processes.append(temp)
        temp.start()
    print(f"Started {len(processes)} workers!")
    for temp in processes:
        temp.join()


def peon(job_q, result_q):
    """
    The actual workers of the program.
    :param job_q: The queue that contains all the jobs to do
    :param result_q: The queue where the results are gathered in
    """
    my_name = mp.current_process().name
    logging.debug(f"Starting worker {my_name}")
    while True:
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                print("Aaaaaaargh", my_name)
                return
            else:
                logging.debug(f"Worker {my_name} is working on {job['pos']}")
                try:
                    result = job['fn'](job['arg'])
                    logging.debug(f"Worker {my_name} is done with {job['pos']}")
                    result_q.put({'result': result, "pos": job["pos"]})
                except NameError:
                    print("Can't find yer fun Bob!")
                    result_q.put({'job': job, 'result': ERROR})

        except queue.Empty:
            print("sleepytime for", my_name)
            time.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        stream=sys.stderr,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    args = command_line_args()
    if args.server:
        # Only read when server is called not when workers are called
        logging.info("Starting the program in server mode.")
        logging.debug("Reading the fastq files.")
        cols_per_file = fastq_reader(args)
        logging.debug(f"Starting the server with {len(cols_per_file)} columns.")
        server = mp.Process(target=runserver, args=(average_phred_score, cols_per_file))
        server.start()
        logging.debug("Waiting on Server.")
        server.join()
        logging.debug("Server has finished.")
    time.sleep(1)
    if args.client:
        logging.info("Starting the program in client mode.")
        client = mp.Process(target=runclient, args=(args.n,))
        client.start()
        logging.debug("Waiting on Client finished.")
        client.join()
        logging.debug("Client has finished.")
