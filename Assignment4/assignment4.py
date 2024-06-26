import csv
from itertools import islice, zip_longest, chain
import argparse as ap
from mpi4py import MPI


comm = MPI.COMM_WORLD
comm_size = comm.Get_size()
my_rank = comm.Get_rank()


def argument_parser():
    """
    Parse command line
    :return: args: the arguments that were given to the program
    """
    parser = ap.ArgumentParser(description="Script for assignment 1 of Big Data Computing")
    parser.add_argument("fastq_files", action="store", type=ap.FileType('r'), nargs='+',
                        help="The FASTQ files to process. Expected at least 1 Illumina fastq file")
    parser.add_argument("-o", action="store", dest="csvfile", type=ap.FileType('w', encoding='UTF-8')
                        , required=False,
                        help="CSV file to save the output to. The default is output to the terminal with STDOUT")
    return parser.parse_args()


def fastq_reader(arguments):
    """
    Read each fastq file and return a list of lists containing the
    characters per column for each file
    :param arguments: the arguments that were given to the program from the command line
    :return: A list of lists containing the quality characters per column
    """
    col_lines = []
    for fastqfile in arguments.fastq_files:
        with open(fastqfile.name, 'r') as fastq:
            col_lines.append(fastq_lines(fastq))
    return col_lines


def fastq_lines(fastq):
    """
    Gather the quality lines from the fastq file and return a column read
    through from the fastq file
    :param fastq: an open fastq file
    :return: the columns from the fastq file
    """
    # Read through the fastq file by quality lines.
    rows = [line.strip() for line in islice(fastq, 3, None, 4)]
    # zip_longest to prevent loss of characters because fastq files can be differing lengths
    columns = [list(col) for col in zip_longest(*rows, fillvalue=None)]
    return columns


def create_chunks(data, chunks=50):
    """
    Create chunks of data, so they can be easily processed as jobs later on.
    :param data: a list of data
    :param chunks: size of the chunks
    """
    # Create chunks of the data
    shared_job_q = []
    collected_column_length = []
    for columns in data:
        collected_column_length.append(len(columns))
        for index in range(0, len(columns), chunks):
            current_index = index
            shared_job_q.append({'arg': columns[current_index:current_index + chunks],
                                 "pos": [i for i in range(current_index, current_index + chunks)]})
    return shared_job_q


def average_phred_score(quality_lines):
    """
    Calculate the average quality score of a list of phred score characters
    :param quality_lines: a list containing phred score character
    :return: the average score of a list of phred score characters
    """
    average_scores = []
    for quality_line in quality_lines:
        phred_score_line = [ord(character) - 33 for character in
                            quality_line if character is not None]
        average_scores.append(sum(phred_score_line) / len(phred_score_line))
    return average_scores


def process_scattered_data(given_data):
    """
    This function processed the scattered data by getting the average phred score and the positions,
    after such they are put in a dictionary and this dictionary is appended to a list.
    :param given_data: the column of phred scores and positions in the line
    """
    result = []
    for data in given_data:
        result.append({'scores': average_phred_score(data['arg']), 'pos': data['pos']})
    return result


def write_results(fastqfiles, csvfile, results):
    """
    Write results to screen or, if a csv file is given, write to a csv file.
    :param fastqfiles: the fastq_files arguments
    :param csvfile: the csv_file argument
    :param results: the results of the distributive process
    """
    args = argument_parser()
    results = sorted(results, key=lambda dictionary: dictionary['pos'])
    if args.csvfile:
        if len(fastqfiles) > 1:
            for fastq_file in fastqfiles:
                filename = fastq_file.name.split("/")[-1]
                with open(f"output_{filename}.csv", "w") as csv_file:
                    writer = csv.writer(csv_file)
                    for result in results:
                        for count, i in enumerate(result["scores"]):
                            writer.writerow([result["pos"][count], i])
        else:
            with csvfile as csv_file:
                writer = csv.writer(csv_file)
                for result in results:
                    for count, i in enumerate(result["scores"]):
                        writer.writerow([result["pos"][count], i])
        print("Saved results to %s" % csvfile.name)
    else:
        for result in results:
            for count, i in enumerate(result["scores"]):
                print(result["pos"][count], i)


def main():
    """
    The main function of the assignment. This function sends and receives the data from all the workers.
    As the main function, it has no arguments.
    """
    args = argument_parser()

    # Define the jobs to do only if you are the manager process
    if comm.rank == 0:
        cols_per_file = fastq_reader(args)
        jobs = create_chunks(cols_per_file, 10)
        chunked_jobs = [jobs[i::comm_size] for i in range(comm_size)]
    else:
        chunked_jobs = None

    if comm.rank == 0:
        items_per_process = [[] for _ in range(comm_size)]
        
        # Package jobs
        for i, attr in enumerate(chunked_jobs):
            target_process = i % comm_size
            items_per_process[target_process].append(attr)
        # Send jobs out to different workers
        for i in range(1, comm_size):
            comm.send(items_per_process[i], dest=i)
        item_list = items_per_process[0]
    else:
        item_list = comm.recv(source=0)

    # Each process has a list of items that it will process
    processed_data = process_scattered_data(*item_list)

    # Send processed data back to the manager
    if my_rank != 0:
        comm.send(processed_data, dest=0)
    else:
        # Manager process collects processed data from all workers
        all_processed_data = [processed_data]
        for i in range(1, comm_size):
            data = comm.recv(source=i)
            all_processed_data.append(data)
        # Write the data out
        combined_list = list(chain(*all_processed_data))
        write_results(args.fastq_files, args.csvfile, combined_list)


if __name__ == "__main__":
    main()
