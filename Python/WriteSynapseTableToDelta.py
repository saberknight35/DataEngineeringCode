# Import libraries
import com.microsoft.spark.sqlanalytics
from com.microsoft.spark.sqlanalytics.Constants import Constants
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

# Set up Spark session
spark = SparkSession.builder.appName("ExportDimProductToDelta").getOrCreate()

# Read from existing internal table
dfToReadFromTable = (spark.read
                     .option(Constants.SERVER, "<synapse_server_name>.sql.azuresynapse.net")
                     .option(Constants.USER, "<synapse_user_name>")
                     .option(Constants.PASSWORD, "<synapse_user_pass>")
					 #data_source_name define with CREATE EXTERNAL DATA SOURCE ... as referenced in:  #https://learn.microsoft.com/en-us/sql/t-sql/statements/create-external-data-source-transact-sql?view=sql-server-ver15&tabs=dedicated#h-create-external-data-source-to-access-data-in-azure-storage-using-the-abfs-interface
                     .option(Constants.DATA_SOURCE, "<data_source_name>")
                     .synapsesql("<sql_poolname>.<schema_name>.DWH_DIM_SALESMAN_CONSOLE_KEYED")
                     ) 

#dfToReadFromTable.show(n=2)

# Define Delta Lake path in Azure Data Lake Storage (ADLS) Gen2
delta_table_path = "abfss://<container-name>@<storage_name>.dfs.core.windows.net/dwhlakehouse/DIM_SALESMAN_CONSOLE"


# Write to Delta format
dfToReadFromTable.write.format("delta").mode("overwrite").save(delta_table_path)

print("Dim_Salesman successfully exported to Delta format.")
