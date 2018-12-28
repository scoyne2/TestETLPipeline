import pandas as pd

# Extract and Type the data to a dataframe
def extractData(csvFile, sep, skiprows=1):
    df = pd.read_csv(csvFile, sep=sep, skiprows=skiprows )
    return df