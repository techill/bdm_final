import csv
import sys
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
## Import PySpark SQL Functions model for aggregation:
import pyspark.sql.functions as F


def main(sc):
    ## Copy and Paste the code from the notebook here

    ## Define the path of input datasets:
    inputFile_viol = "hdfs:///data/share/bdm/nyc_parking_violations/2017.csv"
    inputFile_cscl = "hdfs:///data/share/bdm/nyc_cscl.csv"

    ## Read CSV file into PySpark SQL DataFrame:
    violation_df = spark.read.csv(
        inputFile_viol,
        ## Set comma , (default) as the separator:
        sep=",",
        # Set "UTF-8" (default) as the encoding type:
        encoding="UTF-8",
        ## Set quotation mark " (default) as the character to escape the comma inside quotes:
        quote="\"",
        ## Set quotation mark" as the character to escape nested quotes:
        escape="\"",
        header=True)

    ## Select only the columns needed:
    violation_df = violation_df.select("Summons Number",
                                       "Issue Date",
                                       "Violation County",
                                       "House Number",
                                       "Street Name"
                                       )


    ## Read CSV file into PySpark SQL DataFrame:
    cscl_df = spark.read.csv(
        inputFile_cscl,
        ## Set comma , (default) as the separator:
        sep=",",
        # Set "UTF-8" (default) as the encoding type:
        encoding="UTF-8",
        ## Set quotation mark " (default) as the character to escape the comma inside quotes:
        quote="\"",
        ## Set quotation mark" as the character to escape nested quotes:
        escape="\"",
        header=True)

    ## Select only the columns needed:
    cscl_df = cscl_df.select("PHYSICALID",
                             "L_LOW_HN",
                             "L_HIGH_HN",
                             "R_LOW_HN",
                             "R_HIGH_HN",
                             "BOROCODE",
                             "FULL_STREE",
                             "ST_LABEL"
                             )


    ## Replace "Violation County" values to strings  of code (not int):
    def county_to_code(df, county_list, countyCode):
        for county in county_list:
            df = df.withColumn("Violation County",
                               F.when(df['Violation County'] == county, countyCode) \
                               .otherwise(df['Violation County']))
            return df

    ## Convert county names to a string of code for violations in Mahattan:
    county_mn = ["M", "MN", "NY", "R"]
    violation_df = county_to_code(violation_df, county_mn, "1")

    ## Convert county names to a string of code for violations in Bronx:
    county_bx = ["BX", "BRONX"]
    violation_df = county_to_code(violation_df, county_bx, "2")

    ## Convert county names to a string of code for violations in Queens:
    county_bk = ["K", "BK", "KINGS"]
    violation_df = county_to_code(violation_df, county_bk, "3")

    ## Convert county names to a string of code for violations in Queens:
    county_qn = ["Q", "QN", "QNS"]
    violation_df = county_to_code(violation_df, county_qn, "4")

    ## Convert county names to a string of code for violations in Staten Island:
    county_st = ["ST"]
    violation_df = county_to_code(violation_df, county_st, "5")


    violations = violation_df.groupBy('Violation County',
                                     F.year('Issue-Date').alias('Year'))

    output = violations.join(cscl_df,
                             [violations['Violation County'] == cscl_df['BOROCODE'],
                              violations['Street Name'] == cscl_df['ST_LABEL']
                              ],
                             how='inner')

    ## Write the final output to a CSV file (without header):
    output.write.csv(sys.argv[1])
    ## Write the output DataFrame to a CSV file as the third argument in the command line.
    ## The first argument is the script, input file is hard coded, so the second is the output.

    ...


## Add a “body” function, and create the SparkContext “sc” manually:
if __name__ == "__main__":
    sc = SparkContext()
    spark = SparkSession(sc)
    ## Execute the main fuction:
    main(sc)






