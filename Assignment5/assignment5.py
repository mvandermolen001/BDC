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
        return lower, upper

    for gb_record in SeqIO.parse(open(gbff_file, "r"), "genbank"):
        for feature in gb_record.features:
            if feature.qualifiers.get("organism") is not None:
                organism = feature.qualifiers.get("organism")[0]
            if feature.type in ("CDS", "ncRNA", "rRNA", "gene") and "join" not in str(feature.location):
                lower, upper = get_length(str(feature.location))
                if feature.type == "gene":
                    rows.append(
                        Row(type=0, type_inf=feature.type,
                            start=lower, end=upper, organism_name=organism,
                            record_name=gb_record.name, locus_tag=feature.qualifiers.get("locus_tag")[0]))
                elif feature.type == "CDS":
                    rows.append(
                        Row(type=1, type_inf=feature.type,
                            start=lower, end=upper, organism_name=organism,
                            record_name=gb_record.name, locus_tag=feature.qualifiers.get("locus_tag")[0]))
                elif feature.type in ('ncRNA', 'rRNA'):
                    rows.append(
                        Row(type=0,  type_inf=feature.type,
                            start=lower, end=upper, organism_name=organism,
                            record_name=gb_record.name, locus_tag=feature.qualifiers.get("locus_tag")[0]))
    return rows


def main():
    """
    Main function that creates the dataframe and gets the answers to the assignments questions
    """
    # Location of archea file
    archaea_file = "/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.1.genomic.gbff"
    test_file = "/homes/mvandermolen/thema_12/BDC/Assignment5/test_files/test.gbff"
    # Get the rows of each features
    archaea_rows = extract_features(archaea_file)
    spark = SparkSession.builder.getOrCreate()
    archaea_dataframe = spark.createDataFrame(archaea_rows)

    cds = archaea_dataframe.filter("type_inf == 'CDS'")
    genes = archaea_dataframe.filter("type_inf == 'gene'")

    non_cryptic_genes = genes.join(cds, on=genes.locus_tag == cds.locus_tag, how="left_semi")
    filtered_dataframe = archaea_dataframe.join(non_cryptic_genes, on=["locus_tag", "type_inf"], how="left_anti")
    filtered_dataframe = filtered_dataframe.withColumn("length",
                                                       filtered_dataframe["end"] - filtered_dataframe["start"])
    filtered_dataframe.sort("length", ascending=False).show()
    # Average amount of features in a genome
    feature_amount = filtered_dataframe.count()
    genome_amount = filtered_dataframe.select("record_name").distinct().count()
    print(f"The average amount of features per genome {feature_amount / genome_amount}")

    # Minimum and maximum length of proteins of all organisms
    minimum = filtered_dataframe.where("type = 1").agg({'length': 'min'}).collect()
    maximum = filtered_dataframe.where("type = 1").agg({'length': 'max'}).collect()
    print(f"Minimum length of a coding sequence is {minimum[0][0]}")
    print(f"Maximum length of a coding sequence is {maximum[0][0]}")

    # Average length of a feature
    average_length = filtered_dataframe.agg({'length': 'mean'}).collect()
    print(f"The average length of a feature is {average_length[0][0]}")

    # Ratio between coding and non coding features
    coding_count = filtered_dataframe.filter("type == 1").count()
    # Add one to prevent division by zero
    non_coding_count = filtered_dataframe.filter("type == 0").count() + 1
    print(f"This is the ratio of coding to non coding features in this file {coding_count / non_coding_count}")

    # Remove all non-coding features and save this into a separate dataframe
    coding_df = filtered_dataframe.where("type = 1")
    coding_df.write.save("coding_dataframe")


if __name__ == "__main__":
    main()
