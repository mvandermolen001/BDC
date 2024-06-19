import csv
from itertools import islice, zip_longest, chain

from mpi4py import MPI
import argparse as ap

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


def test(given_data):
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
    counter = 0
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
                if result["pos"][count] == 1:
                    print(fastqfiles[counter].name)
                    counter += 1
                print(result["pos"][count], i)


def main():
    args = argument_parser()
    if my_rank == 0:  # Controller
        cols_per_file = fastq_reader(args)
        jobs = create_chunks(cols_per_file, 10)
        chunked_jobs = [jobs[i::4] for i in range(4)]
    else:  # Workers
        chunked_jobs = None
    data = comm.scatter(chunked_jobs, root=0)
    newData = comm.gather(test(data), root=0)
    if my_rank == 0:
        combined_list = list(chain(*newData))
        write_results(args.fastq_files, args.csvfile, combined_list)


if __name__ == "__main__":
    main()
