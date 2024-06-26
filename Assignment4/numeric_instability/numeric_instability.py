import pandas as pd
import glob

# Get a list of all CSV files in a directory
csv_files = glob.glob('worker_*/*.csv')

# Create an empty dataframe to store the combined data
combined_df = pd.DataFrame()

# Loop through each CSV file and append its contents to the combined dataframe
for csv_file in csv_files:
    df = pd.read_csv(csv_file, names=["LINE_NUMBER", csv_file.split("/")[-2]])
    combined_df = pd.concat([combined_df, df], axis=1)


worker_list = ["worker_1", "worker_2", "worker_3", "worker_4"]

for worker in worker_list:
    print(combined_df[worker][combined_df[worker].std(axis=1) != 0])
