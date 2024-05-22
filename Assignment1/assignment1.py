import csv
import multiprocessing as mp
import argparse as ap
from itertools import islice, zip_longest


def argument_parser():
    """
    Parse command line
    :return: args: the arguments that were given to the program
    """
    parser = ap.ArgumentParser(description="Script for assignment 1 of Big Data Computing")
    parser.add_argument("-n", action="store",
                        dest="n", required=True, type=int,
                        help="number of cores that will be used")
    parser.add_argument("-o", action="store", dest="csvfile", type=ap.FileType('r', encoding='UTF-8'),
                        required=False,
                        help="CSV file to save the output to. The default is output to the terminal with STDOUT")
    parser.add_argument("fastq_files", action="store", type=ap.FileType('r'), nargs='+',
                        help="The FASTQ files to process. Expected at least 1 Illumina fastq file")
    args = parser.parse_args()
    return args


def fastq_reader(args):
    """
    Read each fastq file and return a list of lists containing the
    characters per column for each file
    :param args: the arguments that were given to the program from the command line
    :return: A list of lists containing the quality characters per column
    """
    col_lines = []
    for fastqfile in args.fastq_files:
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
    cols = [list(col) for col in zip_longest(*rows, fillvalue=None)]
    return cols


def column_reader(columns):
    """
    Get the average score of each column of a fastq file
    :param columns: A column of quality character of a fastq file
    :return: the average score of each column of a fastq file
    """
    average_scores = []
    for character_column in columns:
        average_scores.append(average_phred_score(character_column))
    return average_scores


def average_phred_score(quality_line):
    """
    Calculate the average quality score of a list of phred score characters
    :param quality_line: a list containing phred score character
    :return: the average score of a list of phred score characters
    """
    phred_score_line = [ord(character) - 33 for character in quality_line if character is not None]
    average_scores = sum(phred_score_line) / len(phred_score_line)
    return average_scores


def write_results(fastqfile, csvfile, results):
    """
    Write results to screen or, if a csv file is given, write to a csv file.
    :param fastqfile: the fastq_files arguments
    :param csvfile: the csv_file argument
    :param results: the average scores of each column in a fastq file
    """
    if args.csvfile:
        with open(csvfile.name, 'w') as csv_file:
            writer = csv.writer(csv_file)
            for count, average_scores in enumerate(results):
                writer.writerow([fastqfile[count].name, ""])
                for line_index, result in enumerate(average_scores):
                    writer.writerow([line_index+1, result])
    else:
        for count, average_scores in enumerate(results):
            print(fastqfile[count].name)
            for line_index, result in enumerate(average_scores):
                print(line_index+1, result)


if __name__ == '__main__':
    test = []
    args = argument_parser()
    with mp.Pool(processes=args.n) as pool:
        cols = fastq_reader(args)
        results = pool.map(column_reader, cols)
    write_results(args.fastq_files, args.csvfile, results)

