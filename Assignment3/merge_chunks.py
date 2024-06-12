"""
A script containing the tools necessary to process merge chunks of a fastq file.
The script merges the results into a dictionary with the key as the column position and
the value as a list containing the average and the length.

As output this script writes the dictionary to the standard output in the format:
key, average score
"""
import sys
import ast


def write_results(results):
    """
    Write results to the standard output using the merged results
    :param results: These are the merged results
    """
    for key, value in results.items():
        print(f"{key},{value[0] / value[1]}")


if __name__ == "__main__":
    merged_results = {}
    for data in sys.stdin:
        # ast.literal.eval to go from string to literal
        data = ast.literal_eval(data)
        for count, sublist in enumerate(data):
            if count not in merged_results:
                merged_results[count] = [sublist[0], sublist[1]]
            else:
                merged_results[count][0] += sublist[0]
                merged_results[count][1] += sublist[1]
    write_results(merged_results)
    
