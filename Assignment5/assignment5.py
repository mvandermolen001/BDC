"""
A script containing the functions and code to create a dataframe from a genbank feature file.
It saves the coding features into a separate dataframe file and prints the answers to the
assignment's questions.
"""
import re

from pyspark.sql import SparkSession
from pyspark.sql import Row
from Bio import SeqIO


def extract_features(gbff_file):
    """
    Extract features from a genbank feature file using the Bio python library
    :param gbff_file: a genbank feature file
    :return: a list of rows to use in the creation of a spark dataframe
    """
    rows = []

    def get_length(length_part):
        """
        Get the length of a given feature
        :param length_part: the feature's location
        :return: the start, end, and length of the feature
        """
        length_part = re.sub('[^0-9:]', '', length_part)
        lower = int(length_part.split(":")[0])
        upper = int(length_part.split(":")[1])
        return lower, upper, upper - lower

    for gb_record in SeqIO.parse(open(gbff_file, "r"), "genbank"):
        for feature in gb_record.features:
            if feature.qualifiers.get("organism") is not None:
                organism = feature.qualifiers.get("organism")[0]
            if feature.type in ("CDS", "ncRNA", "rRNA", "gene"):
                lower, upper, length = get_length(str(feature.location))
                if feature.type == "gene":
                    rows.append(
                        Row(type=2, type_inf=feature.type,
                            start=lower, end=upper, length=length, organism_name=organism,
                            record_name=gb_record.name))
                elif feature.type == "CDS":
                    rows.append(
                        Row(type=1, type_inf=feature.type,
                            start=lower, end=upper, length=length, organism_name=organism,
                            record_name=gb_record.name))
                elif feature.type in ('ncRNA', 'rRNA'):
                    rows.append(
                        Row(type=0,  type_inf=feature.type,
                            start=lower, end=upper, length=length, organism_name=organism,
                            record_name=gb_record.name))
    return rows


def main():
    """
    Main function that creates the dataframe and gets the answers to the assignments questions
    """
    # Location of archea file
    archaea_file = "/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.3.genomic.gbff"
    test_file = "/homes/mvandermolen/thema_12/BDC/Assignment5/test_files/test_2.gbff"
    # Get the rows of each features
    archaea_rows = extract_features(test_file)
    spark = SparkSession.builder.getOrCreate()
    archaea_dataframe = spark.createDataFrame(archaea_rows)

    # TO DO: REMOVE GENES WHICH HAVE A CDS, KEEP GENES THAT DON'T AND ASSIGN NON-CODING

    # Average amount of features per archaea
    archaea_dataframe.groupby("organism_name", "record_name").count().groupby("organism_name").avg("count").show()
    # Minimum and maximum length of proteins per archaea
    archaea_dataframe.filter("type == 1").groupby("organism_name").agg({'length': 'min'}).show()
    archaea_dataframe.filter("type == 1").groupby("organism_name").agg({'length': 'max'}).show()
    # Average length of a feature
    archaea_dataframe.agg({'length': 'mean'}).show()
    # Ratio between coding and non coding features
    coding_count = archaea_dataframe.filter("type == 1").count()
    # Add one to prevent division by zero
    non_coding_count = archaea_dataframe.filter("type == 0").count() + 1
    print(f"This is the ratio of coding to non coding features in this file {coding_count / non_coding_count}")
    # Remove all non-coding features and save this into a seperate dataframe
    coding_df = archaea_dataframe.filter("type == 1")
    coding_df.write.save("coding_dataframe")


if __name__ == "__main__":
    main()
