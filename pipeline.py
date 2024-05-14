import pandas as pd
import glob
import os

folder_path = './dataset/'

# Use os.walk to recursively search for CSV files in all subdirectories
csv_files = []
for root, dirs, files in os.walk(folder_path):
    for file in files:
        if file.endswith('.csv'):
            csv_files.append(os.path.join(root, file))

# Create a dictionary to store the dataframes
dataframes = {}

# Iterate over each CSV file and read it into a dataframe
for file in csv_files:
    # Extract the filename without extension as the dataframe name
    df_name = os.path.splitext(os.path.basename(file))[0]
    df = pd.read_csv(file)
    dataframes[df_name] = df

# Now you have a dictionary of dataframes with their appropriate names
# You can access each dataframe using its name, for example: dataframes['filename']

# Concatenate all the dataframes into a single dataframe
oltp_data = pd.concat(dataframes.values())

print(oltp_data.head())
# Now you can use the 'oltp_data' dataframe for further processing
