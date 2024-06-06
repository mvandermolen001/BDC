import sys
import ast


def write_results(results):
    for key, value in results.items():
        print(f"{key},{sum(value) / len(value)}")


if __name__ == "__main__":
    # ast.literal.eval to go from string to literal
    merged_results = {}
    for i in sys.stdin:
        i = ast.literal_eval(i)
        for j in range(len(i)):
            if j not in merged_results.keys():
                merged_results[j] = i[j]
            else:
                for score in i[j]:
                    merged_results[j].append(score)
    write_results(merged_results)
