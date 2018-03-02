#-------------------------------------------------------------------------------
# Purpose:
"""
SHORT VERSION:
To Process the recently downloaded Fire Damage Assessment Data.
From accessing the raw downloaded data from another script,
through putting the final data into a production Feature Class


LONG VERSION:
Set up variables
1. Set some variables directly in the script
2. Set some variables from a config file

Start Calling Functions
1. Turn all 'print' statements into a log-writing object for logging purposes.

2. Get the path to the most recently downloaded data.
     This is the path of the data that was most recently downloaded from AGOL
     with the 'DA_Download_Fire_Data.py' script.

3. Set the date that the data was most recently downloaded.
     The date is set into a Feature Class (FC) that has an attribute
     'AGOL_Data_Last_Downloaded'.
     This is used to report in the Dashboard when the data was downloaded.

4. Get an extract of all parcels that intersect with the DA Reports.
     This selects all the parcels that intersect with the DA Reports and exports
     them to their own FC 'Parcel_All_Int_DA_Reports'.
     We do this so that the Parcel and DA Report Join that happens below is MUCH faster.

5. Spatially Join the DA Reports with the extracted Parcels from above to get the working data FC.
     This creates a point FC that has all the data from the DA Reports AND the extracted Parcels.

6. Handle data on a stacked parcel.
     Stacked parcels are multiple APN's on one parcel footprint.
     If a DA Report is on a stacked parcel we either keep the first point and
     nullify all the other points that were created during the spatial join from above,
     OR we keep only the point with the correct APN if that Report Number and Parcel APN were 'linked' in a Control CSV.
     This is a complicated function, please see documentation in the function 'Handle_Stacked_Parcels' below.

7. Add fields to the working data.
     This function uses a Control CSV to programatically add the same fields every script execution.
     Please see which fields are added by viewing the 'FieldsToAdd.csv' file.

8. Calculate fields in the working data.
     This functions uses a Control CSV to programatically calc the same fields every script execution.
     Please see which fields are calced by viewing the 'FieldsToCalculate.csv' file.

9. QA/QC the working data.
     This function writes to a separate log file that only contains the below QA/QC checks.
     This separate QA/QC log file can be placed in a folder that can be viewable
     by a layperson in order to tell them how they should edit the data themselves to produce good data.
     This is a complicated function, please see documentation in the function 'QA_QC_Data' below.

10. Backup the production features before attempting to edit them.
      This function will only delete the existing features in the FC, it will not delete the FC itself.
      This is so we do not need to have an admin connection to the SDE, and we
      don't have to worry about schema locks.
      NOTE: If the schema of the prod FC changes, this _BAK FC will need to be manually changed.

11. Delete the features in the prod FC
      This function will only delete the existing features in the FC, it will not delete the FC itself.
      This is so any settings (fields, domains) that are a part of the FC will remain intact.

12. Append the features from the working FC to the prod Fc
      This function will only append the features in the working FC to the prod FC, it will not recreate the prod FC itself.
      This is so any settings (fields, domains) that are a part of the prod FC will remain intact.

13. Get a token from AGOL so we have permission to access the AGOL database with the original DA Reports.

14. Update the AGOL fields.
      This function updates the AGOL DA Reports
      a. Update any features where the '[Quantity] IS NULL' to have a Quantity = 1.
           We do this because it is possible for a user to have sent a survey
           with a quantity of NULL by removing the default accidentally when
           they filled out the survey.  So if [Quantity] IS NULL, we should just
           assume they meant that it is '1'.
      b. Update ALL features in AGOL that have an [EstimatedReplacementCost] value in the working FC.
           We do this because we have already calculated the values of
           [EstimatedReplacementCost] (in the 'Fields_Calculate_Fields' function above),
           and we want to push any calculations we made on the working FC to the AGOL data.

End of script reporting
1. Print out a footer and if the script was successful or not.
2. Send an email with the results of the script.
"""
#
# Author:      mgrue
#
# Created:     10/11/2017
# Copyright:   (c) mgrue 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------

#TODO: test this script on the County network

import arcpy, sys, datetime, os, ConfigParser
arcpy.env.overwriteOutput = True

def main():

    #---------------------------------------------------------------------------
    #                     Set Variables that will change

    # Set the path prefix depending on if this script is called manually by a
    #  user, or called by a scheduled task on ATLANTIC server.
    called_by = arcpy.GetParameterAsText(0)

    if called_by == 'MANUAL':
        path_prefix = 'P:'  # i.e. 'P:' or 'U:'

    elif called_by == 'SCHEDULED':
        path_prefix = 'D:\projects'  # i.e. 'D:\projects' or 'D:\users'

    else:  # If script run directly and no called_by parameter specified
        path_prefix = 'P:'  # i.e. 'P:' or 'U:'

    # Name of this script
    name_of_script = 'DA_Process_Fire_Data.py'

    # Set the Email variables
    ##email_admin_ls = ['michael.grue@sdcounty.ca.gov', 'randy.yakos@sdcounty.ca.gov', 'gary.ross@sdcounty.ca.gov']
    email_admin_ls = ['michael.grue@sdcounty.ca.gov']

    # Flag to control if there is an error
    success = True
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    #                   Use cfgFile to set the below variables

    # Full path to a text file that has the username and password of an account
    #  that has access to at least VIEW the FS in AGOL, as well as an email
    #  account that has access to send emails.
    cfgFile     = r"{}\Damage_Assessment_GIS\Fire_Damage_Assessment\DEV\Scripts\Config_Files\DA_Download_and_Process.ini".format(path_prefix)
    if os.path.isfile(cfgFile):
        config = ConfigParser.ConfigParser()
        config.read(cfgFile)
    else:
        print("*** ERROR! cannot find valid INI file ***\nMake sure a valid INI file exists at:\n\n{}\n".format(cfgFile))
        print 'You may have to change the name/location of the INI file,\nOR change the variable in the script.'
        raw_input('\nPress ENTER to end script...')
        sys.exit()

    # Set the working folder, FGDBs, FCs, and Tables
    wkg_folder            = config.get('Download_Info', 'wkg_folder')

    raw_agol_FGDB_name    = config.get('Download_Info', 'FGDB_name')
    raw_agol_FGDB_path    = '{}\{}'.format(wkg_folder, raw_agol_FGDB_name)

    processing_FGDB_name  = 'DA_Fire_Processing.gdb'
    processing_FGDB_path  = '{}\{}'.format(wkg_folder, processing_FGDB_name)

    AGOL_Data_DL_name = 'AGOL_Data_Last_Downloaded'
    AGOL_Data_DL_path = '{}\{}'.format(processing_FGDB_path, AGOL_Data_DL_name)

    parcels_extract_name  = config.get('Process_Info', 'Parcels_Extract')
    parcels_extract_path  = '{}\{}'.format(processing_FGDB_path, parcels_extract_name)

    prod_FC_path          = config.get('Process_Info', 'Prod_FC_path')


    # Set CSV that looks for Report Number / APN pairs (for stacked parcels)
    match_Report_to_APN_csv  = config.get('Process_Info', 'Report_to_APN_csv')


    # Set the log file paths
    log_file_folder = config.get('Download_Info', 'Log_File_Folder')
    log_file = r'{}\{}'.format(log_file_folder, name_of_script.split('.')[0])

    QA_QC_log_folder = config.get('Process_Info', 'QA_QC_Log_Folder')

    # Set the path to the success/fail files
    success_error_folder = config.get('Download_Info', 'Success_Error_Folder')
    download_success_file = 'SUCCESS_running_DA_Download_Fire_Data.txt'  # Hard Coded into variable here
    process_success_file  = 'SUCCESS_running_{}.txt'.format(name_of_script.split('.')[0])

    # Set the Control_Files path
    control_file_folder = config.get('Process_Info', 'Control_Files')
    add_fields_csv      = '{}\FieldsToAdd.csv'.format(control_file_folder)
    calc_fields_csv     = '{}\FieldsToCalculate.csv'.format(control_file_folder)


    # Set the PARCELS_ALL Feature Class path
    parcels_all = config.get('Process_Info', 'Parcels_All')


    # Set the Survey123 Feature Service variables
    name_of_FS           = config.get('Download_Info', 'FS_name')
    index_of_layer_in_FS = config.get('Download_Info', 'FS_index')

    #---------------------------------------------------------------------------
    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    #---------------------------------------------------------------------------
    #                          Start Calling Functions

    # Turn all 'print' statements into a log-writing object
    if success == True:
        try:
            orig_stdout, log_file_date, dt_to_append = Write_Print_To_Log(log_file, name_of_script)
        except Exception as e:
            success = False
            print '\n*** ERROR with Write_Print_To_Log() ***'
            print str(e)

    # If this script was called with a batch file, make sure that the data
    # was downloaded successfully before trying to process it.
    if called_by != '':
        print 'Checking to see if the AGOL data was downloaded successfully'

        if os.path.exists('{}\{}'.format(success_error_folder, download_success_file)):
            print '\nDA_Download_Fire_Data.py was run successfully, processing the data now'
            sys.stdout.flush()
        else:
            success = False
            print '\n*** ERROR! ***'
            print '  This script is designed to process data that was downloaded by a previously run script: "DA_Download_Fire_Data.py"'
            print '  If it was completed successfully, The "DA_Download_Fire_Data.py" script should have written a file named:\n    {}'.format(download_success_file)
            print '  At:\n    {}'.format(success_error_folder)
            print '\n  It appears that the above file does not exist, meaning that the Download script had an error.'
            print '  This script will not run if there was an error in DA_Download_Fire_Data.py'
            print '  Please first fix any problems with that script first.'
            print '  You can find the log files at:\n    {}'.format(log_file_folder)

    # Get the path to the most recently downloaded data
    if success == True:
        try:
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            orig_DA_reports_fc = Get_Newest_Data(raw_agol_FGDB_path)

            # Add Attribute Index
            try:
                arcpy.AddIndex_management(orig_DA_reports_fc, ['ReportNumber'], 'orig_DA_index')
                print 'Added index to: {}\n'.format(orig_DA_reports_fc)
            except:
                print 'Index not added.  It probably already exists\n'

        except Exception as e:
            success = False
            print '\n*** ERROR with Get_Newest_Data() ***'
            print str(e)

    #---------------------------------------------------------------------------
    # Set the date that the data was most recently downloaded
    if success == True:
        try:
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            Set_Date_Data_DL(orig_DA_reports_fc, AGOL_Data_DL_path)
        except Exception as e:
            success = False
            print '\n*** ERROR with Set_Date_Data_DL() ***'
            print str(e)

    #---------------------------------------------------------------------------
    # Get an extract of all parcels that intersect with the DA Reports
    if success == True:
        try:
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            Extract_Parcels(parcels_all, orig_DA_reports_fc, parcels_extract_path)

            # Add Attribute Index
            try:
                arcpy.AddIndex_management(parcels_extract_path, ['APN', 'APN_8'], 'parcels_extract_index')
                print 'Added index to: {}\n'.format(parcels_extract_path)
            except:
                print 'Index not added.  It probably already exists\n'

        except Exception as e:
            success = False
            print '\n*** ERROR with Extract_Parcels() ***'
            print str(e)

    #---------------------------------------------------------------------------
    # Spatially Join the DA Reports with the parcels_extract_path
    if success == True:
        try:
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            working_fc = Join_2_FC_By_Spatial_Join(orig_DA_reports_fc, parcels_extract_path, processing_FGDB_path)

            # Add Attribute Index
            try:
                arcpy.AddIndex_management(working_fc, ['ReportNumber', 'APN', 'APN_8'], 'working_fc_index')
                print 'Added index to: {}\n'.format(working_fc)
            except:
                print 'Index not added.  It probably already exists\n'

        except Exception as e:
            success = False
            print '\n*** ERROR with Join_2_FC_By_Spatial_Join() ***'
            print str(e)

    #---------------------------------------------------------------------------
    # Handle data on a stacked parcel.
    # Stacked parcels are multiple APN's on one parcel footprint
    if success == True:
        try:
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            Handle_Stacked_Parcels(orig_DA_reports_fc, working_fc, parcels_extract_path, match_Report_to_APN_csv)
        except Exception as e:
            success = False
            print '\n*** ERROR with Handle_Stacked_Parcels() ***'
            print str(e)

    #---------------------------------------------------------------------------
    # Add Fields to downloaded DA Fire Data
    if success == True:
        try:
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            Fields_Add_Fields(working_fc, add_fields_csv)
        except Exception as e:
            success = False
            print '\n*** ERROR with Fields_Add_Fields() ***'
            print str(e)

    #---------------------------------------------------------------------------
    # Calculate Fields
    if success == True:
        try:
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            Fields_Calculate_Fields(working_fc, calc_fields_csv)
        except Exception as e:
            success = False
            print '\n*** ERROR with Fields_Calculate_Fields() ***'
            print str(e)

    #---------------------------------------------------------------------------
    # QA/QC the data
    if success == True:
        try:
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            success = QA_QC_Data(orig_DA_reports_fc, working_fc, QA_QC_log_folder, dt_to_append, parcels_extract_path, match_Report_to_APN_csv)
        except Exception as e:
            success = False
            print '\n*** ERROR with QA_QC_Data() ***'
            print str(e)

    #---------------------------------------------------------------------------
    #                     Backup the production features
    #                     before attempting to change it
    # Delete the features in the backup database
    if success == True:
        try:
            backup_fc = '{}_BAK'.format(prod_FC_path)
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print 'Backup the production features\n'
            Delete_Features(backup_fc)
        except Exception as e:
            success = False
            print '\n*** ERROR with Delete_Features() ***'
            print str(e)

    # Append the features from the production database to the backup database
    if success == True:
        try:
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            Append_Data(prod_FC_path, backup_fc)
        except Exception as e:
            success = False
            print '\n*** ERROR with Append_Data() ***'
            print str(e)

    #---------------------------------------------------------------------------
    #                       Append newly processed data
    #                      into the production database
    # Delete the features in the prod database
    if success == True:
        try:
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print 'Append newly processed data to the production database\n'
            Delete_Features(prod_FC_path)
        except Exception as e:
            success = False
            print '\n*** ERROR with Delete_Features() ***'
            print str(e)

    # Append the features from the working database to the prod database
    if success == True:
        try:
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            Append_Data(working_fc, prod_FC_path)
        except Exception as e:
            success = False
            print '\n*** ERROR with Append_Data() ***'
            print str(e)

    #---------------------------------------------------------------------------
    #                           Update AGOL fields
    #---------------------------------------------------------------------------
    # Get a token with permissions to view the AGOL data
    if success == True:
        try:
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            token = Get_Token(cfgFile)
        except Exception as e:
            success = False
            print '\n*** ERROR with Get_Token() ***'
            print str(e)

    # Update AGOL fields
    if success == True:
        try:
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            Update_AGOL_Fields(name_of_FS, index_of_layer_in_FS, token, working_fc)
        except Exception as e:
            success = False
            print '\n*** ERROR with Update_AGOL_Fields() ***'
            print str(e)

    #---------------------------------------------------------------------------
    # Write a file to disk to let other scripts know if this script ran
    # successfully or not
    try:
        # Set a file_name depending on the 'success' variable.
        if success == True:
            file_name = 'SUCCESS_running_{}.txt'.format(name_of_script.split('.')[0])

        else:
            file_name = 'ERROR_running_{}.txt'.format(name_of_script.split('.')[0])

        # Write the file
        file_path = '{}\{}'.format(success_error_folder, file_name)
        print '\nCreating file:\n  {}'.format(file_path)
        open(file_path, 'w')

    except Exception as e:
        success = False
        print '*** ERROR with Writing a Success or Fail file() ***'
        print str(e)

    #---------------------------------------------------------------------------
    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    #---------------------------------------------------------------------------
    # Footer for log file
    finish_time_str = [datetime.datetime.now().strftime('%m/%d/%Y  %I:%M:%S %p')][0]
    print '\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
    print '                    {}'.format(finish_time_str)
    print '              Finished {}'.format(name_of_script)
    print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'

    # End of script reporting
    print 'Success = {}'.format(success)
    sys.stdout = orig_stdout

    # Email recipients
    if success == True:
        subj = 'SUCCESS running {}'.format(name_of_script)
        body = """Success<br>
        The Log is found at: {}""".format(log_file_date)

    else:
        subj = 'ERROR running {}'.format(name_of_script)
        body = """There was an error with this script.<br>
        Please see the log file for more info.<br>
        The Log file is found at: {}""".format(log_file_date)

    Email_W_Body(subj, body, email_admin_ls, cfgFile)

    if success == True:
        print '\nSUCCESSFULLY ran {}'.format(name_of_script)
        print 'Please find log file at:\n  {}\n'.format(log_file_date)
    else:
        print '\n*** ERROR with {} ***'.format(name_of_script)
        print 'Please find log file at:\n  {}\n'.format(log_file_date)

    if called_by == 'MANUAL':
        ##raw_input('Press ENTER to continue')
        pass

#-------------------------------------------------------------------------------
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#-------------------------------------------------------------------------------
#                              Define Functions
#-------------------------------------------------------------------------------
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#-------------------------------------------------------------------------------

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          FUNCTION Write_Print_To_Log()
def Write_Print_To_Log(log_file, name_of_script):
    """
    PARAMETERS:
      log_file (str): Path to log file.  The part after the last "\" will be the
        name of the .log file after the date, time, and ".log" is appended to it.

    RETURNS:
      orig_stdout (os object): The original stdout is saved in this variable so
        that the script can access it and return stdout back to its orig settings.
      log_file_date (str): Full path to the log file with the date appended to it.
      dt_to_append (str): Date and time in string format 'YYYY_MM_DD__HH_MM_SS'

    FUNCTION:
      To turn all the 'print' statements into a log-writing object.  A new log
        file will be created based on log_file with the date, time, ".log"
        appended to it.  And any print statements after the command
        "sys.stdout = write_to_log" will be written to this log.
      It is a good idea to use the returned orig_stdout variable to return sys.stdout
        back to its original setting.
      NOTE: This function needs the function Get_DT_To_Append() to run

    """
    ##print 'Starting Write_Print_To_Log()...'

    # Get the original sys.stdout so it can be returned to normal at the
    #    end of the script.
    orig_stdout = sys.stdout

    # Get DateTime to append
    dt_to_append = Get_DT_To_Append()

    # Create the log file with the datetime appended to the file name
    log_file_date = '{}_{}.log'.format(log_file,dt_to_append)
    write_to_log = open(log_file_date, 'w')

    # Make the 'print' statement write to the log file
    print 'Find log file found at:\n  {}'.format(log_file_date)
    print '\nProcessing...\n'
    sys.stdout = write_to_log

    # Header for log file
    start_time = datetime.datetime.now()
    start_time_str = [start_time.strftime('%m/%d/%Y  %I:%M:%S %p')][0]
    print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
    print '                  {}'.format(start_time_str)
    print '             START {}'.format(name_of_script)
    print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n'

    return orig_stdout, log_file_date, dt_to_append

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          FUNCTION Get_dt_to_append
def Get_DT_To_Append():
    """
    PARAMETERS:
      none

    RETURNS:
      dt_to_append (str): Which is in the format 'YYYY_MM_DD__HH_MM_SS'

    FUNCTION:
      To get a formatted datetime string that can be used to append to files
      to keep them unique.
    """
    ##print 'Starting Get_DT_To_Append()...'

    start_time = datetime.datetime.now()

    date = start_time.strftime('%Y_%m_%d')
    time = start_time.strftime('%H_%M_%S')

    dt_to_append = '%s__%s' % (date, time)

    ##print '  DateTime to append: {}'.format(dt_to_append)

    ##print 'Finished Get_DT_To_Append()\n'
    return dt_to_append

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                         FUNCTION: Get Newest Data
def Get_Newest_Data(FGDB_path):
    """
    PARAMETERS:
      FGDB_path (str): Full path to a FGDB that contains the data to be searched.
        Data in this FGDB must contain FC's that are all named the same with the
        only difference between the FC's is that their time stamp is different.
        See below for info about the timestamp naming convention.

    RETURNS:
      newest_download_path(str): Full path to a FC that contains the newest
        downloaded data.

    FUNCTION:
      To return the full path of the FC containing the newest data.
      This works if the FGDB being searched only contains one FC basename and
      that FC is time stamped as 'YYYY_MM_DD__HH_MM_SS'.
      This ensures that the newest data will be the first FC in the list
      (because we do a reverse sort based on the name of the FC).

      For example:
        Data_From_AGOL_2018_01_01__10_00_00
        Data_From_AGOL_2018_01_02__10_00_00
        Data_From_AGOL_2018_01_02__10_00_01
    """

    print '--------------------------------------------------------------------'
    print 'Starting Get_Newest_Data()'

    arcpy.env.workspace = FGDB_path
    print 'Finding the newest data in: {}'.format(FGDB_path)

    # List all FC's in the FGDB
    AGOL_downloads = arcpy.ListFeatureClasses()

    # Sort the FC's alphabetically in reverse (this ensures the most recent date is first)
    AGOL_downloads.sort(reverse=True)
    newest_download = AGOL_downloads[0]

    # Set the path of the newest data
    newest_download_path = '{}\{}'.format(FGDB_path, newest_download)
    print 'The newest download is at:\n  {}'.format(newest_download_path)

    print 'Finished Get_Newest_Data()\n'

    return newest_download_path

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#            FUNCTION: Set Date the AGOL data was Downloaded
def Set_Date_Data_DL(fc_w_timestamp, fc_to_update):
    """
    PARAMETERS:
      fc_w_timestamp (str): Full path to a FC that contains a time stamp
        of when the data was downloaded (or was 'current').  The time stamp
        should be in the format 'YYYY_MM_DD__HH_MM_SS'.
        i.e. 'Data_From_AGOL_2018_01_01__10_00_00'

      fc_to_update (str): Full path to a FC or Table that contains a field named
        'AGOL_Data_Last_Downloaded'.  This is the field that we will write the
        read-friendly formatted time stamp.  If the field name is different,
        that name will have to be specified below in the function itself.

        i.e. a time stamp of:
          2018_01_01__14_00_00
        will become:
          01 Jan, 2018 - 02:00:00 PM

    RETURNS:
      None

    FUNCTION:
      To turn a FC or Table time stamp from 'YYYY_MM_DD__HH_MM_SS' to
      'DD Month, YYYY - HH:MM:SS AM/PM' and set that read-friendly string into
      another FC or Table.
      Specifically we are setting the date and time the data
      was downloaded from AGOL into a FC that will be used to report when the
      data was downloaded.
    """
    print '--------------------------------------------------------------------'
    print 'Starting Set_Date_Data_DL()'

    import time

    # Get the last 20 characters from the FC name (i.e. "2018_02_02__11_11_33")
    dt_stripped = fc_w_timestamp[-20:]

    # Parse the string to time and format the time
    t = time.strptime(dt_stripped, '%Y_%m_%d__%H_%M_%S')
    t_formatted = time.mktime(t)

    # Format time back into a string (i.e. "02 Feb, 2018 - 11:11:33 AM"
    AGOL_data_downloaded = time.strftime("%d %b, %Y - %I:%M:%S %p", time.localtime(t_formatted))

    # Field Calculate the string into the fc_to_update FC
    fc = fc_to_update
    field = 'AGOL_Data_Last_Downloaded'
    expression = '"{}"'.format(AGOL_data_downloaded)

    print '  Calculating field:\n    {}\n  In fc:\n    {} '.format(field, fc)
    print '  To equal:\n    {}'.format(expression)

    arcpy.CalculateField_management(fc, field, expression)

    print 'Finished Set_Date_Data_DL()\n'

    return

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                         FUNCTION: Extract Parcels
def Extract_Parcels(parcels_all, related_fc, parcels_int_related_fc):
    """
    PARAMETERS:
      parcels_all (str): Full path to the PARCELS_ALL FC.  This should be an SDE
        FC.

      related_fc (str):  Full path to a FC that will be used to select parcels
        that intersect features in this FC.

      parcels_int_related_fc (str):  Full path to an EXISTING FC that will contain
        the selected parcels.

    RETURNS:
      None

    FUNCTION:
      To select the parcels that intersect the 'related_fc' features and then
      appending the selected parcels into the the 'parcels_int_related_fc'.

      This function is usually used to speed up other geoprocessing tasks that
      may be performed on a parcels database.

      NOTE: This function deletes the existing features in the
        'parcels_int_related_fc' before appending the selected parcels so we get
        a 'fresh' FC each run of the script AND there is no schema lock to worry
        about.
    """

    print '--------------------------------------------------------------------'
    print 'Starting Extract_Parcels()'

    print '  PARCELS_ALL FC path:\n    {}'.format(parcels_all)
    print '  Related FC used to select parcels:\n    {}'.format(related_fc)

    # Delete the existing features
    print '  Deleting the old existing parcels at:\n    {}'.format(parcels_int_related_fc)
    arcpy.DeleteFeatures_management(parcels_int_related_fc)

    # Make a feature layer out of the PARCELS_ALL FC
    arcpy.MakeFeatureLayer_management(parcels_all, 'par_all_lyr')

    # Select Parcels that intersect with the DA Reports
    print '\n  Selecting parcels that intersect with the DA Reports'
    arcpy.SelectLayerByLocation_management('par_all_lyr', 'INTERSECT', related_fc)

    # Get count of selected parcels
    count = Get_Count_Selected('par_all_lyr')
    print '  There are: "{}" selected parcels\n'.format(count)

    # Export selected parcels
    if (count != 0):

        # Append the newly selected features
        print '  Appending the selected parcels to:\n    {}'.format(parcels_int_related_fc)
        arcpy.Append_management('par_all_lyr', parcels_int_related_fc, 'NO_TEST')

    else:
        print '*** WARNING! There were no selected parcels. ***'
        print '  Please find out why there were no selected parcels.'
        print '  Script still allowed to run w/o an error flag.'

    print 'Finished Extract_Parcels()\n'

    return

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#               FUNCTION: Join DA Reports with Parcels Extract
def Join_2_FC_By_Spatial_Join(target_fc, join_fc, output_FGDB):
    """
    PARAMETERS:
      target_fc (str): Full path to the FC you want to be joined
        to the join_fc.  These are the features you want to add data TO.

      join_fc (str): Full path to the FC that holds the data
        you want to get information FROM.

      output_FGDB (str): Path to the FGDB that you want to hold the newly created joined FC.
        The full path to the FC in this FGDB will be calculated to be the path
        of this FGDB + the name of the target_fc + '_joined'

    RETURNS:
      output_fc (str): Full path to the FC that resulted from the spatial join

    FUNCTION:
      To spatially join the tabular information from the join_fc to the target_fc.
    """

    print '--------------------------------------------------------------------'
    print 'Starting Join_2_FC_By_Spatial_Join()'

    output_fc       = '{}\{}_joined'.format(output_FGDB, os.path.basename(target_fc))
    join_operation   = 'JOIN_ONE_TO_MANY'

    print '  Spatially Joining:\n    {}\n  With:\n    {}\n  Joined FC at:\n    {}'.format(target_fc, join_fc, output_fc)
    arcpy.SpatialJoin_analysis(target_fc, join_fc, output_fc, join_operation)

    print 'Finished Join_2_FC_By_Spatial_Join()\n'

    return output_fc

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          Function Handle Stacked Parcels
def Handle_Stacked_Parcels(orig_fc, working_fc, parcels_fc, match_report_to_APN_csv):
    """
    PARAMETERS:
      orig_fc (str):  Full path to the originally downloaded AGOL FC

      working_fc (str):  Full path to the working FC that has the duplicate
        generated reports on the stacked parcels.  This is the FC that we will
        keep, nullify, or delete the duplicate reports.

      parcels_fc (str):  Full path to the parcels FC that we will use to determine
        if a report is on a stacked parcel.

      match_report_to_APN_csv (str):  Full path to the CSV file that contains
        Report Numbers and APNs that should be associated with each other.

    RETURNS:
      None

    FUNCTION:
      To programatically decide if a report that is on a stacked parcel should:
        1) To be kept as is.
        2) To have its APN information nullified.
        3) To be deleted.
      Then carrying out the nullification and deleting of the features.

      Stacked parcels are when there are multiple parcels on one parcel footprint.
      i.e. Condos are separate APN's but they are on one parcel footprint.
      If a report is placed on a stacked parcel, the join operation performed
      above will create a duplicate report for each APN on that stacked parcel.
      This is usually incorrect.  Usually, one report should be associated with one
      APN.  This function goes through the CSV at 'match_report_to_APN_csv' and creates
      pairs of Report Number and the APN that the report should be associated with.

      The script will then keep the feature in 'working_fc' that has the correct
      Report Number and APN combo.  It will then set all the other features
      created by the spatial join to 'Delete' in field [APN_8].

      If the script encounters a report on a stacked parcel that does not have
      a record in the 'match_report_to_APN_csv', then this script will set 'Nullify'
      in field [APN_8] for the first feature with that Record Number.
      For all subsequent features, this function will set their attribute to
      'Delete' in [APN_8].
      This will ensure that even if a report does not have any information as to
      which APN the report is associated with, we will not lose that report
      in the production database.  But we do not want to have any PARCELS_ALL
      attribute information for that record either since we do not know which
      parcel it should be associated with.

      Once the script has either left a feature alone (if it was in the CSV file
      and had a correct Report Number and APN combo), or it marked the feature
      as 'Nullify' or 'Delete', we will now either nullify all the tabular info
      obtained from PARCELS_ALL from the spatial join, or we will delete that
      feature.
    """

    print '--------------------------------------------------------------------'
    print 'Starting Handle_Stacked_Parcels()'
    print '  Original FC at:\n    {}'.format(orig_fc)
    print '  Working FC at:\n    {}'.format(working_fc)
    print '  Parcel FC at:\n    {}'.format(parcels_fc)
    print '  CSV used to match Report Numbers and APNs at:\n    {}\n'.format(match_report_to_APN_csv)

    import csv

    arcpy.MakeFeatureLayer_management(orig_fc,    'orig_fc_lyr')
    arcpy.MakeFeatureLayer_management(working_fc, 'working_fc_lyr')
    arcpy.MakeFeatureLayer_management(parcels_fc, 'par_lyr')



    #---------------------------------------------------------------------------
    # List of fields that should not be attempted to be nullified
    ignore_fields = ['OBJECTID', 'Shape', 'Shape.area', 'Shape.len',
                     'Shape_Area', 'Shape_Length']

    #---------------------------------------------------------------------------
    # Get list of Report Numbers / APN pairs from the CSV file
    ##print '\n  Creating Report Number and APN lists from CSV at:\n    {}'.format(match_report_to_APN_csv)
    with open (match_report_to_APN_csv) as csv_file:
        readCSV = csv.reader(csv_file, delimiter = ',')

        # Create blank lists
        r_numbers_csv = []
        apns_csv      = []

        # Populate lists
        row_num = 0
        for row in readCSV:
            if row_num > 7:
                r_number_csv = row[0]
                apn_csv      = row[1]

                r_numbers_csv.append(r_number_csv)
                apns_csv.append(apn_csv)
            row_num += 1

    num_reports_to_match = len(r_numbers_csv)
    print '  There are: "{}" Report / APN pairs from the CSV\n'.format(num_reports_to_match)

    #---------------------------------------------------------------------------
    print '  Starting to search each Report for stacked parcels:\n'
    print '    ---------------------------------------------------'

    # Create cursor to loop through each DA Report in the orig_fc
    with arcpy.da.SearchCursor(orig_fc, ['ReportNumber']) as orig_cursor:
        for orig_row in orig_cursor:
            report_number = orig_row[0]

            # Select by attribute the feature(s) in the working_fc that has the
            # same Report Number as the orig_fc.  If there are more than 1
            # record selected, then that report is on a stacked parcel
            where_clause = "ReportNumber = '{}'".format(report_number)
            print '  Searching where: {}'.format(where_clause)
            arcpy.SelectLayerByAttribute_management('working_fc_lyr', 'NEW_SELECTION', where_clause)

            # Get count of selected parcels
            count_selected_features = Get_Count_Selected('working_fc_lyr')

            if count_selected_features > 1:  # Then the report is on a stacked parcel
                print '    Report Number: "{}" is on a stacked parcel (With "{}" APNs)'.format(report_number, count_selected_features)

                # Select by attribute the feature in orig_fc
                where_clause = "ReportNumber = '{}'".format(report_number)
                print '  Searching where: {}'.format(where_clause)
                arcpy.SelectLayerByAttribute_management('orig_fc_lyr', 'NEW_SELECTION', where_clause)

                # Select by location the parcels that intersect with the orig_fc point
                print '  Selecting Parcels that intersect that report'
                arcpy.SelectLayerByLocation_management('par_lyr', 'INTERSECT', 'orig_fc_lyr')
                #---------------------------------------------------------------
                # Test to see if the report_number is in the CSV file that
                # specifies which APN the point on a stacked parcel should be
                # associated with
                if report_number in r_numbers_csv:

                    # Get the first index of the Report Number in the CSV
                    try:
                        csv_index = r_numbers_csv.index(report_number)
                        ##print '  Index in CSV where that report / APN reside = {}'.format(csv_index)
                    except ValueError:
                        print '*** Warning No index returned ***'

                    # Test to make sure that the Report Number / APN pair in the CSV
                    # exist in the working_fc
                    where_clause = "ReportNumber = '{}' and APN = '{}'".format(r_numbers_csv[csv_index], apns_csv[csv_index])
                    arcpy.SelectLayerByAttribute_management('working_fc_lyr', 'NEW_SELECTION', where_clause)
                    count = Get_Count_Selected('working_fc_lyr')
                    if count > 0:
                        report_in_csv = True
                        print '    Report / APN pair in CSV is found in working_fc'
                    else:
                        # If the first report number / APN pair in the CSV does not match with
                        # any of the report number / APN's in working_fc,
                        # we don't want to
                        # delete all of the features from that report.  We will
                        # instead nullify the first feature and delete the
                        # subsequent features as if the report wasn't in the CSV
                        # at all.
                        print '\n*** WARNING This report was in the CSV, but the CSVs APN ({}) is not consistent with its location ***'.format(apns_csv[csv_index])
                        print '*** Please double check the APN in the CSV that it exists and that the location of the report overlaps that APN ***\n'
                        report_in_csv = False

                else:
                    report_in_csv = False

                #---------------------------------------------------------------
                # The below code will keep the reports with the APNs that are
                # specified in the CSV, and will input 'Delete' into the field
                # [APN_8] for all records not in the CSV file
                if report_in_csv == True:
                    print '    Keeping the feature(s) with the APN(s) in the CSV, and deleting the rest:'

                    # Loop through the features in the working_fc and keep the
                    # feature or features specified in the CSV, and mark the
                    # others as 'Delete' in field [APN_8].
                    where_clause ="ReportNumber = '{}'".format(report_number)
                    with arcpy.da.UpdateCursor(working_fc, ['APN', 'APN_8', 'ReportNumber'], where_clause) as working_cursor:
                        for working_row in working_cursor:
                            apn_in_working_fc = working_row[0]
                            keep_feature = False  # Changed to 'True' if we find a Report Number / APN pair in the CSV

                            # Get a list of all the indexes in the CSV that have this report_number
                            csv_report_index = [i for i, value in enumerate(r_numbers_csv) if value == report_number]
                            ##print csv_report_index
                            for index in csv_report_index:

                                # For each time the Report Number is found in the CSV get the APN in the CSV and compare to the APN in the working_fc
                                # If there is a match, change keep_feature to 'True' and break out of the above 'for' loop
                                if apn_in_working_fc == apns_csv[index]:
                                    print '      Report: {}, with APN: {}, will be kept with all APN info.'.format(report_number, apn_in_working_fc)
                                    keep_feature = True
                                    break

                            # Will not keep this report if the APN from this report has no Report Number / APN pair from the CSV
                            if keep_feature == False:
                                print '      Report: {}, with APN: {}, will be deleted.'.format(report_number, apn_in_working_fc)
                                working_row[1] = 'Delete'
                                working_cursor.updateRow(working_row)

                    del working_cursor, working_row
                #---------------------------------------------------------------
                # The below code will input 'Nullify' into the field [APN_8] for
                # the first feature for each record on a stacked parcel
                # It will input 'Delete' into the field [APN_8] for all subsequent
                # features created by the spatial join with a stacked parcel
                if report_in_csv == False:
                    print '    Keeping the first feature, nullifying its APN info, then deleting all subsequent features:'

                    first_parcel = True  # Changed to 'False' after first report in parcel_cursor is processed

                    # Get APN's of selected parcels from the parcels_fc's layer (par_lyr)
                    with arcpy.da.SearchCursor('par_lyr', ['APN', 'APN_8']) as parcel_cursor:

                        for parcel_row in parcel_cursor:
                            apn = parcel_row[0]
                            where_clause = "ReportNumber = '{}' AND APN = '{}'".format(report_number, apn)

                            # Make a cursor to loop through all the Features in the working_fc
                            # and either mark them 'Nullify' or 'Delete'
                            with arcpy.da.UpdateCursor(working_fc, ['APN', 'APN_8', 'ReportNumber'], where_clause) as working_cursor:
                                for working_row in working_cursor:

                                    # Keep the first feature, but input 'Nullify' into [APN_8]
                                    # (Since we don't know which APN info is correct)
                                    if first_parcel == True:
                                        print '      Report: {}, with APN: {}, will be kept.  But all APN info will be nullified.'.format(report_number, apn)
                                        working_row[1] = 'Nullify'
                                        working_cursor.updateRow(working_row)

                                        # Change the flag to false so that we delete the
                                        # subsequent stacked parcel reports
                                        first_parcel = False

                                    # Input 'Delete' into [APN_8]
                                    elif first_parcel == False:
                                        print '      Report: {}, with APN: {}, will be deleted.'.format(report_number, apn)
                                        working_row[1] = 'Delete'
                                        working_cursor.updateRow(working_row)

                            del working_cursor, working_row
                    del parcel_cursor, parcel_row
                print '\n    --------------------------------------------------'

    del orig_cursor, orig_row
    print '  ----------------------------------------------------'

    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------

    #   Handle the features that were marked for 'Nullify', or 'Delete' above

    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    #          Nullify the APN fields on the features that were kept
    #         but we don't know which stacked APN the report belongs to

    print '  Nullifying All APN info for features on a stacked parcel, but not in the CSV\n'

    # Select the features that should have their APN info nullified
    where_clause = "APN_8 = 'Nullify'"
    arcpy.SelectLayerByAttribute_management('working_fc_lyr', 'NEW_SELECTION', where_clause)

    count = Get_Count_Selected('working_fc_lyr')

    if count != 0:
        # Get list of field names in the parcels_fc
        parcel_field_names = [f.name for f in arcpy.ListFields(parcels_fc)]

        # Nullify each field that came from the parcel_fc
        for f_name in parcel_field_names:
            if f_name not in ignore_fields:
                expression = "None"
                ##print '    Nullifying Field: {}'.format(f_name)
                arcpy.CalculateField_management('working_fc_lyr', f_name, expression, 'PYTHON_9.3')

    else:
        print '  INFO: There were no selected features with the above where clause'
        print '  Nothing to be nullified\n'

    #---------------------------------------------------------------------------
    #          Delete the extra features that were created by the join with the
    #          stacked parcels

    print '  Deleting extra features created by the join with stacked parcels\n'

    # Select the features that should be deleted
    where_clause = "APN_8 = 'Delete'"
    f_to_delete_lyr  = Select_By_Attribute(working_fc, 'NEW_SELECTION', where_clause)

    count = Get_Count_Selected(f_to_delete_lyr)

    if count != 0:
        ##rint '  Deleting selected features'
        arcpy.DeleteFeatures_management(f_to_delete_lyr)

    else:
        print '  INFO: There were no selected features with the above where clause'
        print '  Nothing to be deleted\n'

    print 'Finished Handle_Stacked_Parcels()\n'

    return

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                        FUNCTION: ADD FIELDS

def Fields_Add_Fields(wkg_data, add_fields_csv):
    """
    PARAMETERS:
      wkg_data (str) = Name of the working FC in the wkgGDB. This is the FC
        that is processed.
      add_fields_csv (str) = Full path to the CSV file that lists which fields
        should be created.

    RETURNS:
      None

    FUNCTION:
      To add fields to the wkg_data using a CSV file located at add_fields_csv.
    """
    import csv

    print '--------------------------------------------------------------------'
    print 'Starting Fields_Add_Fields()'
    print '  Adding fields to:\n    %s' % wkg_data
    print '  Using Control CSV at:\n    {}\n'.format(add_fields_csv)
    with open (add_fields_csv) as csv_file:
        readCSV = csv.reader(csv_file, delimiter = ',')

        # Create blank lists
        f_names = []
        f_types = []
        f_lengths = []

        row_num = 0
        for row in readCSV:
            if row_num > 1:
                f_name   = row[0]
                f_type   = row[1]
                f_length = row[2]

                f_names.append(f_name)
                f_types.append(f_type)
                f_lengths.append(f_length)
            row_num += 1

    num_new_fs = len(f_names)
    print '  There are %s new fields to add:' % str(num_new_fs)

    f_counter = 0
    while f_counter < num_new_fs:
        print ('    Creating field: %s, with a type of: %s, and a length of: %s'
        % (f_names[f_counter], f_types[f_counter], f_lengths[f_counter]))

        in_table          = wkg_data
        field_name        = f_names[f_counter]
        field_type        = f_types[f_counter]
        field_precision   = '#'
        field_scale       = '#'
        field_length      = f_lengths[f_counter]
        field_alias       = '#'
        field_is_nullable = '#'
        field_is_required = '#'
        field_domain      = '#'


        try:
            # Process
            arcpy.AddField_management(in_table, field_name, field_type,
                        field_precision, field_scale, field_length, field_alias,
                        field_is_nullable, field_is_required, field_domain)
        except Exception as e:
            print '*** WARNING! Field: %s was not able to be added.***' % field_name
            print str(e)
        f_counter += 1

    print '\nFinished Fields_Add_Fields().\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                     FUNCTION: CALCULATE FIELDS

def Fields_Calculate_Fields(wkg_data, calc_fields_csv):
    """
    PARAMETERS:
      wkg_data (str) = Name of the working FC in the wkgGDB. This is the FC
        that is processed.
      calc_fields_csv (str) = Full path to the CSV file that lists which fields
        should be calculated, and how they should be calculated.

    RETURNS:
      None

    FUNCTION:
      To calculate fields in the wkg_data using a CSV file located at
      calc_fields_csv.
    """

    import csv

    print '--------------------------------------------------------------------'
    print 'Starting Fields_Calculate_Fields()'
    print '  Calculating fields in:\n    %s' % wkg_data
    print '  Using Control CSV at:\n    {}\n'.format(calc_fields_csv)

    # Make a table view so we can perform selections
    arcpy.MakeTableView_management(wkg_data, 'wkg_data_view')

    #---------------------------------------------------------------------------
    #                   Get values from the CSV file:
    #                FieldsToCalculate.csv (calc_fields_csv)
    with open (calc_fields_csv) as csv_file:
        readCSV = csv.reader(csv_file, delimiter = ',')

        where_clauses = []
        calc_fields = []
        calcs = []

        row_num = 0
        for row in readCSV:
            if row_num > 1:
                where_clause = row[0]
                calc_field   = row[2]
                calc         = row[4]

                where_clauses.append(where_clause)
                calc_fields.append(calc_field)
                calcs.append(calc)
            row_num += 1

    num_calcs = len(where_clauses)
    print '  There are %s calculations to perform:\n' % str(num_calcs)

    #---------------------------------------------------------------------------
    #                    Select features and calculate them
    f_counter = 0
    while f_counter < num_calcs:
        #-----------------------------------------------------------------------
        #               Select features using the where clause

        in_layer_or_view = 'wkg_data_view'
        selection_type   = 'NEW_SELECTION'
        my_where_clause  = where_clauses[f_counter]

        print '    Selecting features where: "%s"' % my_where_clause

        # Process
        arcpy.SelectLayerByAttribute_management(in_layer_or_view, selection_type, my_where_clause)

        #-----------------------------------------------------------------------
        #     If features selected, perform one of the following calculations
        # The calculation that needs to be performed depends on the field or the calc
        #    See the options below:

        countOfSelected = arcpy.GetCount_management(in_layer_or_view)
        count = int(countOfSelected.getOutput(0))
        print '      There was/were %s feature(s) selected.' % str(count)

        if count != 0:
            in_table   = in_layer_or_view
            field      = calc_fields[f_counter]
            calc       = calcs[f_counter]

            #-------------------------------------------------------------------
            # Perform special calculation for SiteFullAddress
            if (field == 'SiteFullAddress'):

                try:
                    fields = ['SITUS_ADDRESS', 'SITUS_PRE_DIR', 'SITUS_STREET',
                              'SITUS_SUFFIX', 'SITUS_POST_DIR', 'SITUS_SUITE',
                              'SiteFullAddress']

                    with arcpy.da.UpdateCursor(in_table, fields) as cursor:
                        for row in cursor:
                            SITUS_ADDRESS  = row[0]
                            SITUS_PRE_DIR  = row[1]
                            SITUS_STREET   = row[2]
                            SITUS_SUFFIX   = row[3]
                            SITUS_POST_DIR = row[4]
                            SITUS_SUITE    = row[5]

                            if SITUS_ADDRESS == None:
                                SITUS_ADDRESS = 0
                            if SITUS_PRE_DIR == None:
                                SITUS_PRE_DIR = ''
                            if SITUS_STREET == None:
                                SITUS_STREET = ''
                            if SITUS_SUFFIX == None:
                                SITUS_SUFFIX = ''
                            if SITUS_POST_DIR == None:
                                SITUS_POST_DIR = ''
                            if SITUS_SUITE == None:
                                SITUS_SUITE = ''

                            site_full_address = '{} {} {} {} {} {}'.format(SITUS_ADDRESS, SITUS_PRE_DIR, SITUS_STREET, SITUS_SUFFIX, SITUS_POST_DIR, SITUS_SUITE)
                            row[6] = site_full_address
                            cursor.updateRow(row)
                    del cursor

                    print ('      From the selected features, special calculated field: {}, so that it equals a concatenation of Situs Address Fields\n'.format(field))

                except Exception as e:
                    print '*** WARNING! Field: %s was not able to be calculated.***' % field
                    print str(e)
                    print '\n'

            #-------------------------------------------------------------------
            # Perform special calculation for OwnerName
            elif (field == 'OwnerName'):

                try:
                    fields = ['OWN_NAME1', 'OWN_NAME2', 'OWN_NAME3', 'OwnerName']
                    with arcpy.da.UpdateCursor(in_table, fields) as cursor:
                        for row in cursor:

                            OWN_NAME1 = row[0]
                            OWN_NAME2 = row[1]
                            OWN_NAME3 = row[2]

                            if OWN_NAME1 == None:
                                OWN_NAME1 = ''
                            if OWN_NAME2 == None:
                                OWN_NAME2 = ''
                            if OWN_NAME3 == None:
                                OWN_NAME3 = ''

                            owner_name = '{}    {}    {}'.format(OWN_NAME1, OWN_NAME2, OWN_NAME3)
                            row[3] = owner_name
                            cursor.updateRow(row)
                    del cursor

                    print ('        From the selected features, special calculated field: {}, so that it equals a concatenation of Owner Name Fields\n'.format(field))

                except Exception as e:
                    print '*** WARNING! Field: %s was not able to be calculated.***' % field
                    print str(e)
                    print '\n'

            #-------------------------------------------------------------------
            # Perform special calculation for OwnerFullAddress
            elif (field == 'OwnerFullAddress'):

                try:
                    fields = ['OWN_ADDR1', 'OWN_ADDR2', 'OWN_ADDR3',
                              'OWN_ADDR4', 'OwnerFullAddress']
                    with arcpy.da.UpdateCursor(in_table, fields) as cursor:
                        for row in cursor:

                            OWN_ADDR1 = row[0]
                            OWN_ADDR2 = row[1]
                            OWN_ADDR3 = row[2]
                            OWN_ADDR4 = row[3]

                            if OWN_ADDR1 == None:
                                OWN_ADDR1 = ''
                            if OWN_ADDR2 == None:
                                OWN_ADDR2 = ''
                            if OWN_ADDR3 == None:
                                OWN_ADDR3 = ''
                            if OWN_ADDR4 == None:
                                OWN_ADDR4 = ''

                            owner_address = '{}   {}   {}   {}'.format(OWN_ADDR1, OWN_ADDR2, OWN_ADDR3, OWN_ADDR4)
                            row[4] = owner_address
                            cursor.updateRow(row)
                    del cursor

                    print ('        From the selected features, special calculated field: {}, so that it equals a concatenation of Owner Address Fields\n'.format(field))

                except Exception as e:
                    print '*** WARNING! Field: %s was not able to be calculated.***' % field
                    print str(e)
                    print '\n'

            #-------------------------------------------------------------------
            # Test if the user wants to calculate the field being equal to
            # ANOTHER FIELD by seeing if the calculation starts or ends with an '!'
            elif (calc.startswith('!') or calc.endswith('!')):
                f_expression = calc

                try:
                    # Process
                    arcpy.CalculateField_management(in_table, field, f_expression, expression_type="PYTHON_9.3")

                    print ('        From selected features, calculated field: %s, so that it equals FIELD: %s\n'
                            % (field, f_expression))

                except Exception as e:
                    print '*** WARNING! Field: %s was not able to be calculated.***\n' % field
                    print str(e)

            #-------------------------------------------------------------------
            # If calc does not start or end with a '!', it is probably because the
            # user wanted to calculate the field being equal to a STRING
            else:
                s_expression = "'%s'" % calc

                try:
                    # Process
                    arcpy.CalculateField_management(in_table, field, s_expression, expression_type="PYTHON_9.3")

                    print ('        From selected features, calculated field: %s, so that it equals STRING: %s\n'
                            % (field, s_expression))

                except Exception as e:
                    print '*** WARNING! Field: %s was not able to be calculated.***\n' % field
                    print str(e)

        else:
            print ('        WARNING.  No records were selected.  Did not perform calculation.\n')

        #-----------------------------------------------------------------------

        # Clear selection before looping back through to the next selection
        arcpy.SelectLayerByAttribute_management(in_layer_or_view, 'CLEAR_SELECTION')

        f_counter += 1

    print 'Finished Fields_Calculate_Fields().\n'
    return

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          Function QA/QC Data
def QA_QC_Data(orig_fc, working_fc, QA_QC_log_folder, dt_to_append, parcels_extract, match_Report_to_APN_csv):
    """
    PARAMETERS:
      orig_fc (str): Full path to the originally downloaded AGOL data.

      working_fc (str): Full path to the working data that is being processed.

      QA_QC_log_folder (str): Full path to the folder that should contain the
        QA/QC log file.

      dt_to_append (str): Date and time in string format 'YYYY_MM_DD__HH_MM_SS'

      parcels_extract (str): Full path to the Parcel FC that represents parcels
        that intersect one or more Damage Assessment reports.  This FC was
        created by the Extract_Parcels() Function above.

      match_report_to_APN_csv (str):  Full path to the CSV file that contains
        Report Numbers and APNs that should be associated with each other.


    RETURNS:
      success (bool):
        True if the function completed successfully,
        False if the function had an error.
        Most of this function is in a 'try/except'.  This means that if there
        was an error in the 'try', the main() function wouldn't know that there
        was ever an error.

        This value is passed back to the main() function so that we can still
        write the error to the main log file.


    FUNCTION:
      To write logging information to a specifically formatted QA/QC log file
      that is separate from the general log file.

      This function creates and writes to a QA/QC log file in the 'QA_QC_log_folder'
      After setting up a header for the QA/QC log file, this script:
        1) Check to see if any features are not on a parcel
        2) Check to see if any features are on a parcel but have no APN info
        3) Check to see if there are any duplicates in [ReportNumber]
        4) Check to see if any features have NULL in the field [ReportNumber]
        5) Check to see if any features have NULL in [IncidentName]

      Each check has a mini report of the results of the check, which features
      failed the check (if any), who is responsible for editing the data, and
      how they can edit the data to pass the QA/QC checks.

      The final steps are to turn the print statement back into the general
      log writing object.

      If there is an error in this function, the error message will first be
      written to the QA/QC log file, and then to the general log file.

      The variable 'success' will be returned to the main() function so that if
      there is an error, the script can handle the error.
    """

    print '--------------------------------------------------------------------'
    print 'Starting QA_QC_Data()'
    print '  QA/QC Data at: {}'.format(working_fc)

    #---------------------------------------------------------------------------
    #                 Set up a new QA/QC log file to write to
    # Get the original logfile so it can be returned to normal at the
    # end of the function.
    orig_logfile = sys.stdout

    # Create the log file with the datetime appended to the file name
    log_file_date = '{}\DA_Fire_QA_QC_{}.log'.format(QA_QC_log_folder,dt_to_append)
    write_to_log = open(log_file_date, 'w')

    # Make the 'print' statement write to the QA/QC log file
    print '\n  Find log file found at:\n    {}'.format(log_file_date)
    sys.stdout = write_to_log

    try:
        #===========================================================================
        # Every print statement between the equal (=) symbols will print to a QA/Q log file.

        # Make a header for the new QA/QC log file
        print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
        print '                   Start DA Fire QA/QC Log'
        print 'This is the log file for any QA/QC checks that were performed by the'
        print 'Process_DA_Fire_Data.py script on the downloaded AGOL'
        print 'Fire Damage Assessment data'
        print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'

        #---------------------------------------------------------------------------
        #      1) Check to see if any features are not on a parcel
        print '\n------------------------------------------------------------------'
        print '1) Checking for features that are not a parcel...'

        # Select features that do not intersect the parcel_extract
        arcpy.MakeFeatureLayer_management(orig_fc, 'orig_fc_lyr')
        arcpy.SelectLayerByLocation_management('orig_fc_lyr', 'INTERSECT', parcels_extract, '', 'NEW_SELECTION', 'INVERT')

        not_on_parcel_ls = []
        with arcpy.da.SearchCursor('orig_fc_lyr', ['ReportNumber']) as cursor:
            for row in cursor:
                not_on_parcel_ls.append(row[0])
            del cursor

        # Report findings
        if (len(not_on_parcel_ls) > 0):
            del row  # if len() > 0 then there was a 'row' variable created.  Delete it.
            print '  WARNING! There were "{}" features not on a parcel'.format(len(not_on_parcel_ls))
            print '  Report Number:'
            for report_number in not_on_parcel_ls:
                print '    {}'.format(report_number)
            print '\n  ANY DAMAGE ASSESSMENT STAFF, to fix please log onto AGOL Web Map'
            print '  and move the features with the above Report Numbers to the correct parcel.'
        else:
            print '  OK! There were no features not on a parcel.'

        arcpy.Delete_management('orig_fc_lyr')

        #---------------------------------------------------------------------------
        #      2) Check to see if any features are on a parcel but have no APN info
        print '\n------------------------------------------------------------------'
        print '2) Checking for features that are on a parcel but have no APN info...'

        # Select features that have are NULL for [APN] and intersect parcel_extract
        where_clause = "APN IS NULL"
        arcpy.MakeFeatureLayer_management(working_fc, 'working_fc_lyr', where_clause)
        arcpy.SelectLayerByLocation_management('working_fc_lyr', 'INTERSECT', parcels_extract)

        no_apn_info_ls = []

        with arcpy.da.SearchCursor('working_fc_lyr', ['ReportNumber']) as cursor:
            for row in cursor:
                no_apn_info_ls.append(row[0])
            del cursor

        # Report findings
        if (len(no_apn_info_ls) > 0):
            del row  # if len() > 0 then there was a 'row' variable created.  Delete it.
            print '  WARNING! There were "{}" Reports on a parcel but have no APN info'.format(len(no_apn_info_ls))
            print '  Report Number:'
            for report_number in no_apn_info_ls:
                print '    {}'.format(report_number)
            print '\n  This usually happens if the report is on a stacked parcel and'
            print '  there is no info as to which APN the report should be associated with.'
            print '\n  ANY DAMAGE ASSESSMENT STAFF, to fix please use CSV located at:\n    {}'.format(match_Report_to_APN_csv)
            print '  To add the above reports and APNs you wish associated to each other.'
        else:
            print '  OK! There were no features on a parcel w/o APN info.'

        arcpy.Delete_management('working_fc_lyr')

        #---------------------------------------------------------------------------
        #      3) Check to see if there are any duplicates in [ReportNumber]
        print '\n------------------------------------------------------------------'
        print '3) Checking for features with duplicate Report Numbers...'
        print '  At: {}\n'.format(orig_fc)
        orig_list = []
        dup_list  = []

        # Use data from AGOL (orig_fc) before it is potentially split into
        # multiple records by the Spatial Join proecss, which would result in false
        # positive results when searching for duplicate Report Numbers
        with arcpy.da.SearchCursor(orig_fc, ['ReportNumber']) as cursor:
            for row in cursor:
                report_number = row[0]

                # Sort each report number into one of two lists
                if report_number in orig_list:
                    dup_list.append(report_number)
                else:
                    orig_list.append(report_number)

            del cursor

        if (len(dup_list) > 0):
            del row  # if len() > 0 then there was a 'row' variable created.  Delete it.
            print '  WARNING! There were duplicate Report Numbers:'
            for dup in dup_list:
                print '    {}'.format(dup)
            print '\n  LUEG-GIS, please log onto the AGOL database and find out why there are'
            print '  duplicate Report Numbers.  Survey123 should create a unique'
            print '  Report Number as long as staff don\'t start their surveys at'
            print '  the same 1/100th of a second.'

        else:
            print '  OK! There were no duplicate Report Numbers.'

        #---------------------------------------------------------------------------
        #      4) Check to see if any features have NULL in the field [ReportNumber]
        print '\n------------------------------------------------------------------'
        print '4) Checking for features with no Report Number...'

        # Select features that do not have a Report Number
        where_clause = "ReportNumber IS NULL or ReportNumber = '' "
        print '  At: {}\n  Where: {}\n'.format(working_fc, where_clause)
        lyr = Select_By_Attribute(working_fc, 'NEW_SELECTION', where_clause)

        # Get count of the number of features selected
        num_selected = Get_Count_Selected(lyr)

        # Report findings
        if num_selected > 0:
            print '  WARNING! There were {} features without a Report Number at the time the data was downloaded.'.format(num_selected)
            print '\n  LUEG-GIS, please log onto AGOL and find out why there is no Report Number for these features.'
            print '  Survey123 should auto generate report numbers, so if there is a record w/o a Report Number'
            print '  it is possible that the record was created by the AGOL Web Map.  If so, this is against normal workflow'
            print '  and must be investigated.'
        else:
            print '  OK! There were no features without a Report Number.'

        #---------------------------------------------------------------------------
        #      5) Check to see if any features have NULL in [IncidentName]
        print '\n------------------------------------------------------------------'
        print '5) Checking for features with no Incident Name...'

        # Select features that do not have an Incident Name
        where_clause = "IncidentName IS NULL or IncidentName = '' "
        print '  At: {}\n  Where: {}\n'.format(working_fc, where_clause)
        lyr = Select_By_Attribute(working_fc, 'NEW_SELECTION', where_clause)

        # Get count of the number of features selected
        num_selected = Get_Count_Selected(lyr)

        # Report findings
        if num_selected > 0:
            print '  INFO: There were "{}" features without an Incident Name at the time the data was downloaded.'.format(num_selected)
            print '  It is expected that LUEG-GIS staff will fill out the Incident Name as needed.'
            print '\n  LUEG-GIS, please log onto AGOL and fill out an Incident Name for these features.'
            print '  If there are more than one current incident, this should be done now.'
            print '  If there is only one current incident, this can be done when convenient.'

        else:
            print '  OK! There were no features without an Incident Name.'

        #---------------------------------------------------------------------------
        # Make a footer for the new QA/QC log file
        print ''
        print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
        print '                      End DA Fire QA/QC'
        print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'

        #-----------------------------------------------------------------------
        # Return the print statement to write to our general log file
        sys.stdout = orig_logfile
        success = True  # This function completed successfully

    except Exception as e:
        success = False

        # If there is an error, print error in the QA/QC Log file
        print '\n*** ERROR with QA_QC_Data() ***'
        print str(e)

        # Return the print statement to write to our general log file and print the same error
        sys.stdout = orig_logfile
        print '\n*** ERROR with QA_QC_Data() ***'
        print str(e)


    #===========================================================================
    print '\nFinished QA_QC_Data()\n'

    return success

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                       FUNCTION Select_By_Attribute()
def Select_By_Attribute(path_to_obj, selection_type, where_clause=None):
    """
    PARAMETERS:
      path_to_obj (str): Full path to the object (Feature Class or Table) that
        is to be selected.

      selection_type (str): Selection type.  Valid values are:
        NEW_SELECTION
        ADD_TO_SELECTION
        REMOVE_FROM_SELECTION
        SUBSET_SELECTION
        SWITCH_SELECTION
        CLEAR_SELECTION

      where_clause (str): The SQL where clause.

    RETURNS:
      'lyr' (lyr): The layer/view with the selection on it.

    FUNCTION:
      To perform a selection on the object.
    """

    ##print 'Starting Select_By_Attribute()...'

    # Use try/except to handle either object type (Feature Layer / Table)
    try:
        arcpy.MakeFeatureLayer_management(path_to_obj, 'lyr')
    except:
        arcpy.MakeTableView_management(path_to_obj, 'lyr')

    ##print '  Selecting "lyr" with a selection type: {}, where: "{}"'.format(selection_type, where_clause)
    arcpy.SelectLayerByAttribute_management('lyr', selection_type, where_clause)

    ##print 'Finished Select_By_Attribute()\n'
    return 'lyr'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                        FUNCTION Get_Count_Selected()
def Get_Count_Selected(lyr):
    """
    PARAMETERS:
      lyr (lyr): The layer that should have a selection on it that we want to test.

    RETURNS:
      count_selected (int): The number of selected records in the lyr

    FUNCTION:
      To get the count of the number of selected records in the lyr.
    """

    ##print 'Starting Get_Count()...'

    # See if there are any selected records
    desc = arcpy.Describe(lyr)

    if desc.fidSet: # True if there are selected records
        result = arcpy.GetCount_management(lyr)
        count_selected = int(result.getOutput(0))

    # If there weren't any selected records
    else:
        count_selected = 0

    ##print '  Count of Selected: {}'.format(str(count_selected))

    ##print 'Finished Get_Count()\n'

    return count_selected

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          FUNCTION Delete_Features()
def Delete_Features(in_fc):
    """
    PARAMETERS:
      in_fc (str): Full path to a Feature Class.

    RETURNS:
      None

    FUNCTION:
      To delete the features from one FC.
    """

    print '--------------------------------------------------------------------'
    print 'Starting Delete_Features()...'

    print '  From: {}'.format(in_fc)

    arcpy.DeleteFeatures_management(in_fc)

    print 'Finished Delete_Features()\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                        FUNCTION:  APPEND DATA

def Append_Data(input_item, target, schema_type='NO_TEST', field_mapping=None):
    """
    PARAMETERS:
      input_item (str) = Full path to the item to append.
      target (str) = Full path to the item that will be updated.
      schema_type (str) = Controls if a schema test will take place.
        TEST - Schemas between two items must match.
        NO_TEST - Schemas don't have to match.
                  This is the default for this function.
      field_mapping {arcpy.FieldMappings obj} = Arcpy Field Mapping object.
        Optional.

    RETURNS:
      None

    FUNCTION:
      To append the data from the input_item to the target using an
      optional arcpy field_mapping object to override the default field mapping.
    """

    print '--------------------------------------------------------------------'
    print 'Starting Append_Data()...'
    print '  From: {}'.format(input_item)
    print '  To:   {}'.format(target)

    # If there is a field mapping object, make sure there is no schema test
    if field_mapping != None:
        schema_type = 'NO_TEST'

    # Process
    arcpy.Append_management(input_item, target, schema_type, field_mapping)

    print 'Finished Append_Data()\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                       FUNCTION:    Get AGOL token
def Get_Token(cfgFile, gtURL="https://www.arcgis.com/sharing/rest/generateToken"):
    """
    PARAMETERS:
      cfgFile (str):
        Path to the .txt file that holds the user name and password of the
        account used to access the data.  This account must be in a group
        that has access to the online database.
        The format of the config file should be as below with
        <username> and <password> completed:

          [AGOL]
          usr: <username>
          pwd: <password>

      gtURL {str}: URL where ArcGIS generates tokens. OPTIONAL.

    VARS:
      token (str):
        a string 'password' from ArcGIS that will allow us to to access the
        online database.

    RETURNS:
      token (str): A long string that acts as an access code to AGOL servers.
        Used in later functions to gain access to our data.

    FUNCTION: Gets a token from AGOL that allows access to the AGOL data.
    """

    print '--------------------------------------------------------------------'
    print "Getting Token..."

    import ConfigParser, urllib, urllib2, json

    # Get the user name and password from the cfgFile
    configRMA = ConfigParser.ConfigParser()
    configRMA.read(cfgFile)
    usr = configRMA.get("AGOL","usr")
    pwd = configRMA.get("AGOL","pwd")

    # Create a dictionary of the user name, password, and 2 other keys
    gtValues = {'username' : usr, 'password' : pwd, 'referer' : 'http://www.arcgis.com', 'f' : 'json' }

    # Encode the dictionary so they are in URL format
    gtData = urllib.urlencode(gtValues)

    # Create a request object with the URL adn the URL formatted dictionary
    gtRequest = urllib2.Request(gtURL,gtData)

    # Store the response to the request
    gtResponse = urllib2.urlopen(gtRequest)

    # Store the response as a json object
    gtJson = json.load(gtResponse)

    # Store the token from the json object
    token = gtJson['token']
    ##print token  # For testing purposes

    print "Successfully retrieved token.\n"

    return token

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                FUNCTION:
def Update_AGOL_Fields(name_of_FS, index_of_layer_in_FS, token, working_fc):
    """
    PARAMETERS:
      name_of_FS (str): The name of the Feature Service (do not include things
        like "services1.arcgis.com/1vIhDJwtG5eNmiqX/ArcGIS/rest/services", just
        the name is needed.  i.e. "DPW_WP_SITES_DEV_VIEW".

      index_of_layer_in_FS (str or int): The index of the layer in the Feature Service.
        This will frequently be 0, but it could be a higer number if the FS has
        multiple layers in it.

      token (str): A string 'password' from ArcGIS that will allow us to to access the
        online database.  Obtained from the Get_Token() Function.

      working_fc (str): Full path to the FC that contains the features you
        want to use to get values from.

    RETURNS:
      None

    FUNCTION:
      To update a Feature Service in AGOL (name_of_FS) at index (index_of_layer_in_FS)
      based off of either fixed calculations (like for the Quantity field) or
      based off of the values in the working_fc

      This function updates that [Quantity] field to equal 1 for any features
      in AGOL where that field equals NULL.

      It then updates the [EstimatedReplacementCost] field in AGOL based off
      of the value in that field in the working_fc.  We already calculated
      the Estimated Replacement Cost in the Fields_Calculate_Fields() function
      above, and we can now update AGOL features to match those calculations.

      If there is a value in [EstimatedReplacementCost] in AGOL, we will not
      perform a calculation for that feature.
    """

    print '--------------------------------------------------------------------'
    print 'Starting Update_AGOL_Fields()...'

    #---------------------------------------------------------------------------
    #               Update (in AGOL) NULL [Quantity] to equal 1
    print '\n  1) Updating (in AGOL) any records with a NULL [Quantity] to equal 1'

    # Get list of Object IDs
    where_clause = "Quantity IS NULL"
    obj_ids = AGOL_Get_Object_Ids_Where(name_of_FS, index_of_layer_in_FS, where_clause, token)

    # Update those Object IDs to have 1 in their [Quantity] field
    field_to_update = 'Quantity'
    new_value       = 1
    for object_id in obj_ids:

        AGOL_Update_Features(name_of_FS, index_of_layer_in_FS, object_id, field_to_update, new_value, token)

    #---------------------------------------------------------------------------
    #              Update (in AGOL) features with NULL
    #              in field [EstimatedReplacementCost]
    #         to equal the [EstimatedReplacementCost] value
    #             for that feature in the working_fc

    # Make a cursor that only looks at reports with an Estimated Replacement Cost
    print '\n  2) Updating (in AGOL) all records with the working_fc EstimatedReplacementCost value'
    print '  working_fc:\n    {}'.format(working_fc)
    fields = ['EstimatedReplacementCost', 'ReportNumber']
    cur_where_clause = "EstimatedReplacementCost IS NOT NULL"
    print '  Cursor Where Clause: "{}"'.format(cur_where_clause)
    with arcpy.da.SearchCursor(working_fc, fields, cur_where_clause) as cursor:
        for row in cursor:
            est_replcmt_cost = row[0]
            report_number    = row[1]

            # Get the object id of the AGOL feature with that report number
            where_clause = "ReportNumber = {}".format(report_number)
            obj_ids = AGOL_Get_Object_Ids_Where(name_of_FS, index_of_layer_in_FS, where_clause, token)

            # Update the AGOL feature with that report number with the Estimated Replacement Cost
            if (len(obj_ids) == 1):  # There should only be one object id with that report number
                field_to_update = 'EstimatedReplacementCost'
                new_value = est_replcmt_cost

                for object_id in obj_ids:
                    AGOL_Update_Features(name_of_FS, index_of_layer_in_FS, object_id, field_to_update, new_value, token)

            else:
                print '  WARNING! There was more than 1 feature that satisfied the where clause: {}'.format(where_clause)
                print '  The features on AGOL were not updated.'

    print 'Finished Update_AGOL_Fields()\n'
    return

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                FUNCTION:    Get AGOL Object IDs Where

def AGOL_Get_Object_Ids_Where(name_of_FS, index_of_layer_in_FS, where_clause, token):
    """
    PARAMETERS:
      name_of_FS (str): The name of the Feature Service (do not include things
        like "services1.arcgis.com/1vIhDJwtG5eNmiqX/ArcGIS/rest/services", just
        the name is needed.  i.e. "DPW_WP_SITES_DEV_VIEW".
      index_of_layer_in_FS (int): The index of the layer in the Feature Service.
        This will frequently be 0, but it could be a higer number if the FS has
        multiple layers in it.
      where_clause (str): Where clause. i.e.:
        where_clause = "FIELD_NAME = 'Value in field'"
      token (str): Obtained from the Get_Token()

    RETURNS:
      object_ids (list of str): List of OBJECTID's that satisfied the
      where_clause.

    FUNCTION:
      To get a list of the OBJECTID's of the features that satisfied the
      where clause.  This list will be the full list of all the records in the
      FS regardless of the number of the returned OBJECTID's or the max record
      count for the FS.

    NOTE: This function assumes that you have already gotten a token from the
    Get_Token() and are passing it to this function via the 'token' variable.
    """

    print '  ------------------------------------------------------------------'
    print "  Starting AGOL_Get_Object_Ids_Where()"
    import urllib2, urllib, json

    #TODO: have a success variable to return

    # Create empty list to hold the OBJECTID's that satisfy the where clause
    object_ids = []

    # Encode the where_clause so it is readable by URL protocol (ie %27 = ' in URL).
    # visit http://meyerweb.com/eric/tools/dencoder to test URL encoding.
    where_encoded = urllib.quote(where_clause)

    # Set URLs
    query_url = r'https://services1.arcgis.com/1vIhDJwtG5eNmiqX/ArcGIS/rest/services/{}/FeatureServer/{}/query'.format(name_of_FS, index_of_layer_in_FS)
    query = '?where={}&returnIdsOnly=true&f=json&token={}'.format(where_encoded, token)
    get_object_id_url = query_url + query

    # Get the list of OBJECTID's that satisfied the where_clause

    print '    Getting list of OBJECTID\'s that satisfied the where clause for layer:\n      {}'.format(query_url)
    print '    Where clause: "{}"'.format(where_clause)
    response = urllib2.urlopen(get_object_id_url)
    response_json_obj = json.load(response)
    try:
        object_ids = response_json_obj['objectIds']
    except KeyError:
        print '***ERROR!  KeyError! ***'
        print '  {}\n'.format(response_json_obj['error']['message'])
        print '  If you receive "Invalid URL", are you sure you have the correct FS name?'

    if len(object_ids) > 0:
        print '    There are "{}" features that satisfied the query.'.format(len(object_ids))
        print '    OBJECTID\'s of those features:'
        for obj in object_ids:
            print '      {}'.format(obj)

    else:
        print '    No features satisfied the query.'

    print "  Finished AGOL_Get_Object_Ids_Where()\n"

    return object_ids

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                FUNCTION:    Update AGOL Features

def AGOL_Update_Features(name_of_FS, index_of_layer_in_FS, object_id, field_to_update, new_value, token):
    """
    PARAMETERS:
      name_of_FS (str): The name of the Feature Service (do not include things
        like "services1.arcgis.com/1vIhDJwtG5eNmiqX/ArcGIS/rest/services", just
        the name is needed.  i.e. "DPW_WP_SITES_DEV_VIEW".
      index_of_layer_in_FS (str or int): The index of the layer in the Feature Service.
        This will frequently be 0, but it could be a higer number if the FS has
        multiple layers in it.
      object_id (str or int): OBJECTID that should be updated.
      field_to_update (str): Field in the FS that should be updated.
      new_value (str or int): New value that should go into the field.  Data
        type depends on the data type of the field.
      token (str): Token from AGOL that gives permission to interact with
        data stored on AGOL servers.  Obtained from the Get_Token().

    RETURNS:
      success (boolean): 'True' if there were no errors.  'False' if there were.

    FUNCTION:
      To Update features on an AGOL Feature Service.
    """

    print '  ------------------------------------------------------------------'
    print "  Starting AGOL_Update_Features()"
    import urllib2, urllib, json

    success = True

    # Set the json upate
    features_json = {"attributes" : {"objectid" : object_id, "{}".format(field_to_update) : "{}".format(new_value)}}
    ##print 'features_json:  {}'.format(features_json)

    # Set URLs
    update_url       = r'https://services1.arcgis.com/1vIhDJwtG5eNmiqX/ArcGIS/rest/services/{}/FeatureServer/{}/updateFeatures?token={}'.format(name_of_FS, index_of_layer_in_FS, token)
    update_params    = urllib.urlencode({'Features': features_json, 'f':'json'})


    # Update the features
    print '    Updating Features in FS: {}'.format(name_of_FS)
    print '                   At index: {}'.format(index_of_layer_in_FS)
    print '     OBJECTID to be updated: {}'.format(object_id)
    print '        Field to be updated: {}'.format(field_to_update)
    print '     New value for updt fld: {}'.format(new_value)

    ##print update_url + update_params
    response  = urllib2.urlopen(update_url, update_params)
    response_json_obj = json.load(response)
    ##print response_json_obj

    for result in response_json_obj['updateResults']:
        ##print result
        print '     OBJECTID: {}'.format(result['objectId'])
        print '       Updated? {}'.format(result['success'])
        if result['success'] != True:
            success = False

    print '\n  Finished AGOL_Update_Features()\n'
    return success

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                               Function Email_W_Body()
def Email_W_Body(subj, body, email_list, cfgFile=
    r"P:\DPW_ScienceAndMonitoring\Scripts\DEV\DEV_branch\Control_Files\accounts.txt"):

    """
    PARAMETERS:
      subj (str): Subject of the email
      body (str): Body of the email in HTML.  Can be a simple string, but you
        can use HTML markup like <b>bold</b>, <i>italic</i>, <br>carriage return
        <h1>Header 1</h1>, etc.
      email_list (str): List of strings that contains the email addresses to
        send the email to.
      cfgFile {str}: Path to a config file with username and password.
        The format of the config file should be as below with
        <username> and <password> completed:

          [email]
          usr: <username>
          pwd: <password>

        OPTIONAL. A default will be used if one isn't given.

    RETURNS:
      None

    FUNCTION: To send an email to the listed recipients.
      If you want to provide a log file to include in the body of the email,
      please use function Email_w_LogFile()
    """
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    import ConfigParser, smtplib

    print '  Starting Email_W_Body()'
    print '    With Subject: {}'.format(subj)

    # Set the subj, From, To, and body
    msg = MIMEMultipart()
    msg['Subject']   = subj
    msg['From']      = "Python Script"
    msg['To']        = ', '.join(email_list)  # Join each item in list with a ', '
    msg.attach(MIMEText(body, 'html'))

    # Get username and password from cfgFile
    config = ConfigParser.ConfigParser()
    config.read(cfgFile)
    email_usr = config.get('email', 'usr')
    email_pwd = config.get('email', 'pwd')

    # Send the email
    ##print '  Sending the email to:  {}'.format(', '.join(email_list))
    SMTP_obj = smtplib.SMTP('smtp.gmail.com',587)
    SMTP_obj.starttls()
    SMTP_obj.login(email_usr, email_pwd)
    SMTP_obj.sendmail(email_usr, email_list, msg.as_string())
    SMTP_obj.quit()
    time.sleep(2)

    print '  Successfully emailed results.'

#-------------------------------------------------------------------------------
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#-------------------------------------------------------------------------------
if __name__ == '__main__':
    main()
