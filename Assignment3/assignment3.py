"""
A script containing the tools necessary to process a chunk of a fastq file.
From this chunk the average is calculated and along with the length of the sum
passed to the standard output.
"""

import sys
from itertools import islice, zip_longest


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


def gather_phred_scores(quality_lines):
    """
    Calculate the average quality score of a list of phred score characters
    :param quality_lines: a list containing phred score character
    :return: the average score of a list of phred score characters
    """
    average_scores = []
    for count, quality_line in enumerate(quality_lines):
        phred_line = [ord(character) - 33 for character in quality_line if character is not None]
        average_scores.append([sum(phred_line), len(phred_line)])
    return average_scores


if __name__ == "__main__":
    av_scores = gather_phred_scores(fastq_lines(sys.stdin))
    print(av_scores)
