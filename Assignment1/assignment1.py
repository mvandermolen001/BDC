import csv
import multiprocessing as mp
import argparse as ap
from itertools import islice


def main():
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


def fastq_parser(fastqfile):
    results = {}
    with open(fastqfile.name, 'r') as fastq:
        # With fastq as the iterator, walk through the lines starting at 3 and taking steps of 4
        for line in islice(fastq, 0, None, 4):
            line = line.strip()
            for index, character in enumerate(line):
                if index in results.keys():
                    results[index].append(ord(character) - 33)
                else:
                    results[index] = [ord(character) - 33]
        average_scores = {item_key: sum(item_value) / len(item_value) for (item_key, item_value) in results.items()}
    return average_scores



if __name__ == "__main__":
    args = main()
    with mp.Pool(processes=args.n) as p:
        pass
        # for fastqfile in args.fastq_files:
        #     average_scores = fastq_parser(fastqfile)
        #     if args.csvfile:
        #         with open(args.csvfile.name, 'w') as csv_file:
        #             writer = csv.writer(csv_file)
        #             writer.writerow([fastqfile.name, ""])
        #             for key, value in average_scores.items():
        #                 writer.writerow([key, value])
        #         args.csvfile.close()
        #     else:
        #         print(fastqfile.name + "\n" + average_scores + "\n")
