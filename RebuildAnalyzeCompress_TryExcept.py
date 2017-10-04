# Rebuild, Analyze, and Compress
# ------------------------------ #
# 8 August 2017 - Andrew Whitesides
# ------------------------------ #
# This code is designed to run the Rebuild Indexes and Analyze Datasets
# on both the COL and SA connections, before and after database
# compression.

# Set the necessary product code
#import arceditor
import arcpy
import os
import time

# Local variables for DB connections
# Update connection paths as needed

# Stormwater Billing 
COL_StormBill = "Database Connections\\COL@StormwaterBilling.sde"
SA_StormBill = "Database Connections\\SA@StormwaterBilling.sde"

# Stormwater 
COL_STWT = "Database Connections\\COL@Stormwater.sde"
SA_STWT = "Database Connections\\SA@Stormwater.sde"

# City GIS 
COL_CityGIS = "Database Connections\\COL@CityGIS.sde"
SA_CityGIS = "Database Connections\\SA@CityGIS.sde"

# Public Works 
COL_PubWorks = "Database Connections\\COL@PubWorks.sde"
SA_PubWorks = "Database Connections\\SA@PubWorks.sde"

# Streets 
COL_Streets = "Database Connections\\COL@Streets.sde"
SA_Streets = "Database Connections\\SA@Streets.sde"

# Parks
COL_Parks = "Database Connections\\COL@Parks.sde"
SA_Parks = "Database Connections\\SA@Parks.sde"

# LandRecords
COL_LandRecords = "Database Connections\\COL@LandRecords.sde"
SA_LandRecords = "Database Connections\\SA@LandRecords.sde"

# Parcels 
COL_Parcels = "Database Connections\\COL@Parcels.sde"
SA_Parcels = "Database Connections\\SA@Parcels.sde"

# GIS Prod 
COL_GISProd = "Database Connections\\COL@GISProd.sde"
SA_GISProd = "Database Connections\\SA@GISProd.sde"

# Other Agencies 
COL_OtherAgencies = "Database Connections\\COL@OtherAgencies.sde"
SA_OtherAgencies = "Database Connections\\SA@OtherAgencies.sde"

# LCAD 
COL_LCAD = "Database Connections\\COL@LCAD.sde"
SA_LCAD = "Database Connections\\SA@LCAD.sde"

# Contours 
COL_Contours = "Database Connections\\COL@Contours.sde"
SA_Contours = "Database Connections\\SA@Contours.sde"


# Build list of connections sorted by COL or SA
# One item list fromatting is [variable,(space)]
COL_ConnectionList = [COL_LandRecords, COL_StormBill, COL_STWT, COL_CityGIS, COL_PubWorks,
                      COL_Streets, COL_Parks, COL_Parcels, COL_GISProd,
                      COL_OtherAgencies, COL_LCAD, COL_Contours]
SA_ConnectionList = [SA_LandRecords, SA_StormBill, SA_STWT, SA_CityGIS, SA_PubWorks,
                     SA_Streets, SA_Parks, SA_Parcels, SA_GISProd,
                     SA_OtherAgencies, SA_LCAD, SA_Contours]


# Build list of COL database connections that will use "NO_SYSTEM" for
# the Rebuild/Analyze steps 
systemList = list(filter(lambda x: ((x != COL_StormBill) and\
                                    (x != COL_STWT)), COL_ConnectionList))

# Create text file to record databse compression attempt outcomes
# Folder to create text file
File_Location = "S:\gisds\Projects\Scripting\AndreW\Python\RebuildAnalyzeCompress"

# Calculate date, time, and file name for record compression attempt outcomes
date = time.strftime('%Y%m%d_%H%M%S')
File_Name = ("{}_CompressionReport.txt".format(date))


# Function for rebuild indexes and analyze datasets
def RebuildAnalyze(connection):

    print ("\nStarting {} RebuildAnalyze function".format(connection))
    # COL side rebuild
    # Set workspace (COL connection path)
    arcpy.env.workspace = connection

    # Get list of all datasets COL connection has access to
    dataList = arcpy.ListTables() + arcpy.ListFeatureClasses() + arcpy.ListRasters()

    # Now, add all datasets and featureclasses to the master list
    for dataset in arcpy.ListDatasets("", "Feature"):
        arcpy.env.workspace = os.path.join(connection, dataset)
        dataList += arcpy.ListFeatureClasses() + arcpy.ListDatasets()

    # Removal of stormwater geometric network from GISProd and Stormwater:
    # swGeometricNetwork was throwing an undefined error previously so
    # this particular item is removed (Thanks Jack Wilson for this!).
    # Future Geometric Networks will need to be added here through
    # "and (x != 'new geometric network')"
    dataList = list(filter(lambda x: ((x != 'StormWater.COL.swGeometricNetwork')\
                                  and\
                                  (x != 'GISProd.COL.swGeometricNetwork')),
                       dataList))
    print "Made it past dataList filter"
    
    # reset the workspace and retreive user name
    arcpy.env.workspace = connection
    userName = arcpy.Describe(connection).connectionProperties.user.lower()

    # remove any datasets that are not owned by COL
    userDataList = [ds for ds in dataList if ds.lower().find(".%s." % userName) > -1]

    # "SYSTEM" option is default except for COL connections in systemList
    # systemList connections will use "NO_SYSTEM"
    system = "SYSTEM"
    if connection in systemList:
        system = "NO_SYSTEM"
               
    # Execute rebuild indexes
    arcpy.RebuildIndexes_management(connection, system, userDataList, "ALL")
    print("{} Rebuild Complete".format(connection))

    # Analyze COL Datasets
    # Execute analyze datasets
    # Note: to use the "SYSTEM" option the workspace user must be an
    # administrator.
    arcpy.AnalyzeDatasets_management(connection, system, dataList,\
                                     "ANALYZE_BASE",\
                                     "ANALYZE_DELTA",\
                                     "ANALYZE_ARCHIVE")
    print ("{} Analyze Complete".format(connection))


# Loop for rebuild, analyze, compress, rebuild, and analyze for each DB
# connection(COL and SA)
if len(COL_ConnectionList) == len(SA_ConnectionList):
    for COL, SA in zip(COL_ConnectionList, SA_ConnectionList):
        # Initial rebuild and analyze
        try:
            print ("==================New Database Connection==================")
            RebuildAnalyze(COL)
            print ("{} pre-compression Rebuild Indexes and Analyze Datasets complete"\
                   .format(COL))
            RebuildAnalyze(SA)
            print ("{} pre-compression Rebuild Indexes and Analyze Datasets complete"\
                  .format(SA))

            # Database compression
            arcpy.Compress_management(SA)
            print ("\n{} compression complete".format(SA))

            # Post compression rebuild and analyze
            RebuildAnalyze(COL)
            print ("{} post compression Rebuild Indexes and Analyze Datasets complete"\
                   .format(COL))
            RebuildAnalyze(SA)
            print ("{} post compression Rebuild Indexes and Analyze Datasets complete"\
                   .format(SA))

            # Open file create previously and append compression attempt results
            f = open(File_Name, "a")
            pre = ("Pre compression Rebuild/Analyze was successful for {} and {}"\
                .format(COL, SA))
            comp = ("Compression was successful for {}".format(SA))
            post = ("Post compression Rebuild/Analyze was successful for {} and {}"\
                   .format(COL, SA))
            f.write("{0}\n{1}\n{2}\n \n".format(pre, comp, post))
            f.close()
        except Exception as e:
            f = open(File_Name, "a")
            f.write("Error processing {} and {}:\n >>>{}<<< \n".format(COL, SA, e))
            f.close()
            continue

      ## Test comment for new branch
