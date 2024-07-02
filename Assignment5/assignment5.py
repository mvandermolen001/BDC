import re
import glob

from pyspark.sql import SparkSession
from pyspark.sql import Row
from Bio import SeqIO


def extract_features(gbff_file):
    rows = []

    def get_length(length_part):
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
                        Row(type=1, start=lower, end=upper, length=length, organism_name=organism,
                            record_name=gb_record.name))
                elif feature.type == "CDS":
                    rows.append(
                        Row(type=1, start=lower, end=upper, length=length, organism_name=organism,
                            record_name=gb_record.name))
                elif feature.type == "ncRNA" or feature.type == "rRNA":
                    rows.append(
                        Row(type=0, start=lower, end=upper, length=length, organism_name=organism,
                            record_name=gb_record.name))
    return rows


def main():
    # Get a list of all .gbff files in a directory
    archea_file = "/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.3.genomic.gbff"
    archea_rows = extract_features(archae_file)
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(archea_rows)

    # Average amount of features per archae
    df.groupby("organism_name", "record_name").count().groupby("organism_name").avg("count").show()
    # Minimum and maximum length of proteins per archae
    df.filter("type == 1").groupby("organism_name").agg({'length': 'min'}).show()
    df.filter("type == 1").groupby("organism_name").agg({'length': 'max'}).show()
    # Average length of a feature
    df.agg({'length': 'mean'}).show()
    # Ratio between coding and non coding features
    coding_count = df.filter("type == 1").count()
    non_coding_count = df.filter("type == 0").count()
    print(f"This is the ratio of coding to non coding features in this file {coding_count / non_coding_count}")
    # Remove all non-coding features and save this into a seperate dataframe
    coding_df = df.filter("type == 1")
    coding_df.write.save("coding_dataframe")


if __name__ == "__main__":
    main()
