# Python Data Pipeline
import pandas as pd
import fastparquet
# https://pypi.org/project/schema/
import schema

# Create a standard schema
medicalInputSchema = Schema([{'first_name': And(str, len),
                         'last_name': And(str, len),
					     'age': And(Use(int), lambda n: 0 <= n <= 130),
					     'date_of_birth': And(str, len),
					     'incurred_date': And(str, len),
					     'allowed_amount': And(str, len),
					     'net_paid': And(str, len),
					     'gender': And(str, Use(str.upper), lambda s: s in ('M', 'F', 'U'))}])

medicalOutputSchema = Schema([{'full_name': And(str, len),
					           'age': And(Use(int), lambda n: 0 <= n <= 130),
					           'date_of_birth': And(date, len),
					           'incurred_date': And(date, len),
					           'allowed_amount': And(decimal(10,2), len),
					           'net_paid': And(decimal(10,2), len),
					           'gender': And(str, Use(str.upper), lambda s: s in ('M', 'F', 'U'))}])

# Extract and Type the data from 2 csv's
def extractData(csvFile, sep, skiprows=1):
    df = pd.read_csv(csvFile, sep=sep, skiprows=skiprows )
    return df

 # TODO split these out into a seperate class as functions
def sexTransformer(sex):
    switcher = {
        "Male": "M",
        "Female": "F",
        1: "M",
        2: "F"
    }
    return switcher.get(sex, "U")

# Transform the data to meet a standard schema
def transformMedicalData(df):
    #TODO, how can all of these happen in a single operation?
    df = df["gender"].apply(sexTransformer)
    df["full_name"] = df["first_name"].map(str).str.title() + df1["last_name"].str.title()
    df['date_of_birth'] = df['date_of_birth'].dt.strftime('%Y-%m-%d')
    df['incurred_date'] = df['incurred_date'].dt.strftime('%Y-%m-%d')
    df['allowed_amount'] = pd.to_numeric(df["allowed_amount"])
    df['net_paid'] = pd.to_numeric(df["net_paid"])
	return df

def mergeMetadata(df, metadata):
    # merge the two tables
	return mergedData

def saveOutputToParquet(df):
    dataSaved = False
    try:
    	# save output to parquet
    	df.to_parquet('df.parquet.gzip', compression='gzip')
    except:
    print "Unexpected error when saving to parquet:", sys.exc_info()[0]
        raise

def checkNull(df,columnName, acceptableAmount):
    acceptableQuality = False
    nullPercent = df[columnName].isnull().sum()/df[columnName].count()
    if nullPercent >= acceptableAmount:
    	acceptableQuality = True
	return acceptableQuality

def main():
    # Run ETL Process
    medialclaims = extractData('fakemedicalclaims.csv', '|')
    metadata = extractData('fakemmetadata.csv', '|', 0 )

    # Run QA check to confirm the data can be correctly coerced to the schema
    isMedicalSchemaValid = medialclaims.validate(medicalInputSchema)

    # Apply business logic to transform data and merge in metadata
    if isMedicalSchemaValid:
    	transformedData = transformMedicalData(medialclaims)
    	mergedData = mergeMetadata(medialclaims, metadata)
	else:
		raise ValueError('The schema for medialclaims does not meet the required format')

    # Run QA checks to confirm data population
    isNameValid = checkNull(mergedData,'full_name',.9)
    isAgeValid = checkNull(mergedData,'age',.9)
    isGenderValid = checkNull(mergedData,'gender',1)

    #Run QA check to confirm output schema is correct
    isMedicalOutputSchemaValid = mergedData.validate(medicalOutputSchema)

    #TODO(scoyne): have this error identify the field causing the failure
    if not isNameValid || not isAgeValid || not isGenderValid || not isMedicalOutputSchemaValid:
        raise ValueError('The data quality for one or more columns does not meet the required threshold')
    else:
    # Save the output to parquet
    saveOutputToParquet(mergedData)

if __name__ == "__main__":
	main()