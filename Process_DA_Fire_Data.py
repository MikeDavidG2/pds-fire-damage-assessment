#-------------------------------------------------------------------------------
# Purpose:
"""
To Process the recently downloaded Fire Damage Assessment Data.
"""
#
# Author:      mgrue
#
# Created:     10/11/2017
# Copyright:   (c) mgrue 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------

#TODO: comment Purpose above
#TODO: add try/except and error handling to this script
#TODO: test this script on the County network
#TODO: test this script on appending to SDE FC instead of a FGDB

import arcpy, sys, datetime, os, ConfigParser
arcpy.env.overwriteOutput = True

def main():

    #---------------------------------------------------------------------------
    #                     Set Variables that will change

    # The below variables are used if running the script
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
    name_of_script = 'Process_DA_Fire_Data.py'

    # Full path to a text file that has the username and password of an account
    #  that has access to at least VIEW the FS in AGOL, as well as an email
    #  account that has access to send emails.
    cfgFile     = r"{}\Damage_Assessment_GIS\Fire_Damage_Assessment\DEV\Scripts\Config_Files\config_file.ini".format(path_prefix)
    if os.path.isfile(cfgFile):
        config = ConfigParser.ConfigParser()
        config.read(cfgFile)
    else:
        print("INI file not found. \nMake sure a valid '.ini' file exists at {}.".format(cfgFile))
        sys.exit()

    # Set the working folder and FGDBs
    # TODO: some of these 'Process_Info' can be changed to 'Download Info'
    wkg_folder           = config.get('Process_Info', 'wkg_folder')
    raw_agol_FGDB        = config.get('Process_Info', 'raw_agol_FGDB')
    raw_agol_FGDB_path   = '{}\{}'.format(wkg_folder, raw_agol_FGDB)
    processing_FGDB      = config.get('Process_Info', 'Processing_FGDB')
    processing_FGDB_path = '{}\{}'.format(wkg_folder, processing_FGDB)
    prod_FC_path   = config.get('Process_Info', 'Prod_FC')

    # Set the log file folder path
    log_file_folder = config.get('Process_Info', 'Log_File_Folder')
    log_file = r'{}\{}'.format(log_file_folder, name_of_script.split('.')[0])

    # Set the Control_Files path
    control_file_folder = config.get('Process_Info', 'Control_Files')
    add_fields_csv      = '{}\FieldsToAdd.csv'.format(control_file_folder)
    calc_fields_csv     = '{}\FieldsToCalculate.csv'.format(control_file_folder)

    # Set the PARCELS_ALL Feature Class path
    parcels_all = config.get('Process_Info', 'Parcels_All')

    # Set the Survey123 Feature Service variables
    name_of_FS = config.get('Download_Info', 'FS_names')
    index_of_layer_in_FS = config.get('Download_Info', 'FS_indexes')

    # Set the Email variables
    ##email_admin_ls = ['michael.grue@sdcounty.ca.gov', 'randy.yakos@sdcounty.ca.gov', 'gary.ross@sdcounty.ca.gov']
    email_admin_ls = ['michael.grue@sdcounty.ca.gov']

    #---------------------------------------------------------------------------
    #                Set Variables that will probably not change

    # Flag to control if there is an error
    success = True

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
            print '*** ERROR with Write_Print_To_Log() ***'
            print str(e)

    # Get a token with permissions to view the AGOL data
    if success == True:
        try:
            token = Get_Token(cfgFile)
        except Exception as e:
            success = False
            print '*** ERROR with Get_Token() ***'
            print str(e)


    #---------------------------------------------------------------------------
    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    #---------------------------------------------------------------------------
    #                 Non-standard Template Functions go HERE

    # QA/QC AGOL data; edit / update the raw data if needed here
    # TODO: add a function here to:
    #   Log warning if NULL [IncidentName],
    #   Log if duplicate [ReportNumber],
    #   Log if NULL [ReportNumber]

    # Get the most recently downloaded data
    print '\n--------------------------------------------------------------------'
    arcpy.env.workspace = raw_agol_FGDB_path
    print 'Listing FCs in: {}'.format(raw_agol_FGDB_path)
    AGOL_downloads = arcpy.ListFeatureClasses()  # List all FC's in the FGDB
    for download in AGOL_downloads:
        newest_download = download  # Only the last FC in the list is kept after the loop

    newest_download_path = '{}\{}'.format(raw_agol_FGDB_path, newest_download)
    print 'The newest download is at: {}\n'.format(newest_download_path)

    #---------------------------------------------------------------------------
    # Spatially Join the downloaded data with the PARCELS_ALL
    target_features   = newest_download_path
    join_features     = parcels_all
    working_fc = '{}\{}_joined'.format(processing_FGDB_path, newest_download)
    join_operation =  'JOIN_ONE_TO_MANY'

    print 'Spatially Joining:\n  {}\nWith:\n  {}\nNew FC at:\n  {}\n'.format(target_features, parcels_all, working_fc)
##    arcpy.SpatialJoin_analysis(target_features, join_features, working_fc, join_operation)

    #---------------------------------------------------------------------------
    # Add Fields to downloaded DA Fire Data
    ##working_fc = r'P:\Damage_Assessment_GIS\Fire_Damage_Assessment\DEV\Data\DA_Fire_Processing.gdb\DA_Fire_from_AGOL_2018_01_26__14_28_48'
##    Fields_Add_Fields(working_fc, add_fields_csv)

    #---------------------------------------------------------------------------
    # Calculate Fields
##    Fields_Calculate_Fields(working_fc, calc_fields_csv)

    #---------------------------------------------------------------------------
    # Handle data on a stacked parcel
    # TODO: write function here

    #---------------------------------------------------------------------------
    # QA/QC the data
    working_fc = r'P:\Damage_Assessment_GIS\Fire_Damage_Assessment\DEV\Data\DA_Fire_Processing.gdb\DA_Fire_from_AGOL_2018_01_31__08_47_28_joined'
    QA_QC_log_file   = r'P:\Damage_Assessment_GIS\Fire_Damage_Assessment\DEV\Scripts\Logs\QA_QC_Logs\DA_Fire_QA_QC'
    QA_QC_Data(newest_download_path, working_fc, QA_QC_log_file, dt_to_append)

    #---------------------------------------------------------------------------
    # Backup the production database before attempting to edit it
##    Backup_FC(prod_FC_path)

    #---------------------------------------------------------------------------
    #         Append newly processed data into the production database
    # Delete the features in the prod database
##    Delete_Features(prod_FC_path)

    # Append the features from the working database to the prod database
##    Append_Data(working_fc, prod_FC_path)

    #---------------------------------------------------------------------------
    #                           Update AGOL fields
    #---------------------------------------------------------------------------
    #
    #               Update (in AGOL) NULL [Quantity] to equal 1
    print '\nUpdating (in AGOL) any records with a NULL [Quantity] to equal 1'

    # Get list of Object IDs
    where_clause = "Quantity IS NULL"
    obj_ids = AGOL_Get_Object_Ids_Where(name_of_FS, index_of_layer_in_FS, where_clause, token)

    # Update those Object IDs to have 1 in their [Quantity] field
    field_to_update = 'Quantity'
    new_value       = 1
    for object_id in obj_ids:

        AGOL_Update_Features(name_of_FS, index_of_layer_in_FS, object_id, field_to_update, new_value, token)


    #           Update (in AGOL) NULL [EstimatedReplacementCost]
    #                to equal (in working database) [EstimatedReplacementCost]
    # Make a cursor that only looks at reports with an Estimated Replacement Cost
    print '\nUpdating (in AGOL) all records with the working_fc EstimatedReplacementCost value'
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

##    Email_W_Body(subj, body, email_admin_ls, cfgFile)

    if success == True:
        print '\nSUCCESSFULLY ran {}'.format(name_of_script)
        print 'Please find log file at:\n  {}\n'.format(log_file_date)
    else:
        print '\n*** ERROR with {} ***'.format(name_of_script)
        print 'Please find log file at:\n  {}\n'.format(log_file_date)

    if called_by == 'MANUAL':
        raw_input('Press ENTER to continue')

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
    print ' Adding fields to:\n  %s' % wkg_data
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
    print '    There are %s new fields to add:' % str(num_new_fs)

    f_counter = 0
    while f_counter < num_new_fs:
        print ('      Creating field: %s, with a type of: %s, and a length of: %s'
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

    print 'Finished Fields_Add_Fields().\n'

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
    print ' Calculating fields in:\n  %s' % wkg_data
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
    print '    There are %s calculations to perform:\n' % str(num_calcs)

    #---------------------------------------------------------------------------
    #                    Select features and calculate them
    f_counter = 0
    while f_counter < num_calcs:
        #-----------------------------------------------------------------------
        #               Select features using the where clause

        in_layer_or_view = 'wkg_data_view'
        selection_type   = 'NEW_SELECTION'
        my_where_clause  = where_clauses[f_counter]

        print '      Selecting features where: "%s"' % my_where_clause

        # Process
        arcpy.SelectLayerByAttribute_management(in_layer_or_view, selection_type, my_where_clause)

        #-----------------------------------------------------------------------
        #     If features selected, perform one of the following calculations
        # The calculation that needs to be performed depends on the field or the calc
        #    See the options below:

        countOfSelected = arcpy.GetCount_management(in_layer_or_view)
        count = int(countOfSelected.getOutput(0))
        print '        There was/were %s feature(s) selected.' % str(count)

        if count != 0:
            in_table   = in_layer_or_view
            field      = calc_fields[f_counter]
            calc       = calcs[f_counter]

            #-------------------------------------------------------------------
            # Perform special calculation for SiteFullAddress
            if (field == 'SiteFullAddress'):

                try:
                    fields = ['SITUS_ADDRESS', 'SITUS_PRE_DIR', 'SITUS_STREET', 'SITUS_SUFFIX', 'SITUS_POST_DIR', 'SITUS_SUITE', 'SiteFullAddress']
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

                    print ('        From the selected features, special calculated field: {}, so that it equals a concatenation of Situs Address Fields\n'.format(field))

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
                    fields = ['OWN_ADDR1', 'OWN_ADDR2', 'OWN_ADDR3', 'OWN_ADDR4', 'OwnerFullAddress']
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

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          Function QA/QC Data
def QA_QC_Data(orig_fc, working_fc, QA_QC_log_file, dt_to_append):
    """
    PARAMETERS:

    RETURNS:

    FUNCTION:
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
    log_file_date = '{}_{}.log'.format(QA_QC_log_file,dt_to_append)
    write_to_log = open(log_file_date, 'w')

    # Make the 'print' statement write to the QA/QC log file
    print 'Find log file found at:\n  {}'.format(log_file_date)
    sys.stdout = write_to_log

    #===========================================================================
    # Every print statement between the equal (=) symbols will print to a QA/Q log file.

    # Make a header for the new QA/QC log file
    print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
    print 'This is the log file for any QA/QC checks that were performed by the'
    print 'Process_DA_Fire_Data.py script on the downloaded AGOL'
    print 'Fire Damage Assessment data'
    print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'

    #---------------------------------------------------------------------------
    #      1) Check to see if there are any duplicates in [ReportNumber]
    print '\n------------------------------------------------------------------'
    print '1) Checking for features with duplicate Report Numbers...'
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
        print '  WARNING! There were duplicate Report Numbers:'
        for dup in dup_list:
            print '    {}'.format(dup)
        print '\n  LUEG-GIS, please log onto the AGOL database and find out why there are'
        print '  duplicate Report Numbers.  Survey123 should create a unique'
        print '  Report Number as long as staff don\'t start their surveys at'
        print '  the same 1/100th of a second.'

    else:
        print '  OK! There were no duplicate Report Numbers'

    #---------------------------------------------------------------------------
    #      2) Check to see if any features have NULL in the field [ReportNumber]
    print '\n------------------------------------------------------------------'
    print '2) Checking for features with no Report Number...'

    # Select features that do not have a Report Number
    where_clause = "ReportNumber IS NULL or ReportNumber = '' "
    print '  At: {}\n  Where: {}\n'.format(working_fc, where_clause)
    lyr = Select_Object(working_fc, 'NEW_SELECTION', where_clause)

    # Get count of the number of features selected
    num_selected = Get_Count_Selected(lyr)

    # Report findings
    if num_selected > 0:
        print '  WARNING! There were {} features without a Report Number at the time the data was downloaded'.format(num_selected)
        print '\n  LUEG-GIS, please log onto AGOL and find out why there is no Report Number for these features'
        print '  Survey123 should auto generate report numbers, so if there is a record w/o a Report Number'
        print '  it is possible that the record was created by the AGOL Web Map.  If so, this is against normal workflow'
        print '  and must be investigated'
    else:
        print '  OK! There were no features without a Report Number'

    #---------------------------------------------------------------------------
    #      3) Check to see if any features have NULL in [IncidentName]
    print '\n------------------------------------------------------------------'
    print '3) Checking for features with no Incident Name...'

    # Select features that do not have an Incident Name
    where_clause = "IncidentName IS NULL or IncidentName = '' "
    print '  At: {}\n  Where: {}\n'.format(working_fc, where_clause)
    lyr = Select_Object(working_fc, 'NEW_SELECTION', where_clause)

    # Get count of the number of features selected
    num_selected = Get_Count_Selected(lyr)

    # Report findings
    if num_selected > 0:
        print '  WARNING! There were "{}" features without an Incident Name at the time the data was downloaded.'.format(num_selected)
        print '  It is expected that LUEG-GIS staff will fill out the Incident Name as needed'
        print '\n  LUEG-GIS, please log onto AGOL and fill out an Incident Name for these features'
    else:
        print '  OK! There were no features without an Incident Name'

    #---------------------------------------------------------------------------
    #      4) Check to see if any features have NULL in [APN]
    print '\n------------------------------------------------------------------'
    print '4) Checking for features with no APN...'

    # Select features that do not have a Report Number
    where_clause = "APN IS NULL or APN = '' "
    print '  At: {}\n  Where: {}\n'.format(working_fc, where_clause)
    no_apns = []
    with arcpy.da.SearchCursor(working_fc, ['ReportNumber', 'APN'], where_clause) as cursor:
        for row in cursor:
            no_apns.append(row[0])

    # Report findings
    if (len(no_apns) > 0):
        print '  WARNING! There were {} features without an APN at the time the data was downloaded'.format(len(no_apns))
        print '  Report Number:'
        for no_apn in no_apns:
            print '    {}'.format(no_apn)
        print '\n  This usually happens if the feature\'s location is not on a parcel.'
        print '  Any Damage Assessment Staff, please log onto AGOL Web Map'
        print '  and move the features with the above Report Numbers to the correct parcel.'
    else:
        print '  OK! There were no features without a Report Number'

    #---------------------------------------------------------------------------
    #     5) Check to see if [SITUS_ADDRESS] is in [StreetNumber_Usr]
    # TODO: See if this makes sense to write up...


    #---------------------------------------------------------------------------
    # Return the print statement to write to our general log file
    sys.stdout = orig_logfile
    #===========================================================================
    print 'Finished QA_QC_Data()\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                       FUNCTION Select_Object()
def Select_Object(path_to_obj, selection_type, where_clause):
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

    ##print 'Starting Select_Object()...'

    # Use try/except to handle either object type (Feature Layer / Table)
    try:
        arcpy.MakeFeatureLayer_management(path_to_obj, 'lyr')
    except:
        arcpy.MakeTableView_management(path_to_obj, 'lyr')

    ##print '  Selecting "lyr" with a selection type: {}, where: "{}"'.format(selection_type, where_clause)
    arcpy.SelectLayerByAttribute_management('lyr', selection_type, where_clause)

    ##print 'Finished Select_Object()\n'
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
#                         Function Backup Feature Class
def Backup_FC(full_path_to_fc):
    """
    PARAMETERS:
      full_path_to_fc (str): Full path to the FC you want to create a copy of.

    RETURNS:
      None

    FUNCTION:
      To create a copy of a FC.  Primarily as a Backup.  Appends '_BAK' to the
      end of the copied FC.  The FC is copied to the same workspace as the
      original FC.  The original FC remains unchanged.
    """

    arcpy.env.overwriteOutput = True
    import os

    print '--------------------------------------------------------------------'
    print 'Starting Backup_FC()'


    in_features = full_path_to_fc
    out_path    = os.path.dirname(full_path_to_fc)
    out_name    = '{}_BAK'.format(os.path.basename(full_path_to_fc))

    print '  Backing up FC: {}'.format(in_features)
    print '             To: {}'.format(out_path)
    print '             As: {}'.format(out_name)

    arcpy.FeatureClassToFeatureClass_conversion (in_features, out_path, out_name)

    print 'Finished Backup_FC()\n'

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

    print '  Deleting Features from: "{}"'.format(in_fc)

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
    if field_mapping <> None:
        schema_type = 'NO_TEST'

    # Process
    arcpy.Append_management(input_item, target, schema_type, field_mapping)

    print 'Finished Append_Data()\n'

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

    print '--------------------------------------------------------------------'
    print "Starting Get_AGOL_Object_Ids_Where()"
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

    print '  Getting list of OBJECTID\'s that satisfied the where clause for layer:\n    {}'.format(query_url)
    print '  Where clause: "{}"'.format(where_clause)
    response = urllib2.urlopen(get_object_id_url)
    response_json_obj = json.load(response)
    try:
        object_ids = response_json_obj['objectIds']
    except KeyError:
        print '***ERROR!  KeyError! ***'
        print '  {}\n'.format(response_json_obj['error']['message'])
        print '  If you receive "Invalid URL", are you sure you have the correct FS name?'

    if len(object_ids) > 0:
        print '  There are "{}" features that satisfied the query.'.format(len(object_ids))
        print '  OBJECTID\'s of those features:'
        for obj in object_ids:
            print '    {}'.format(obj)

    else:
        print '  No features satisfied the query.'

    print "Finished Get_AGOL_Object_Ids_Where()\n"

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
      index_of_layer_in_FS (int): The index of the layer in the Feature Service.
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

    print '--------------------------------------------------------------------'
    print "Starting AGOL_Update_Features()"
    import urllib2, urllib, json

    success = True

    # Set the json upate
    features_json = {"attributes" : {"objectid" : object_id, "{}".format(field_to_update) : "{}".format(new_value)}}
    ##print 'features_json:  {}'.format(features_json)

    # Set URLs
    update_url       = r'https://services1.arcgis.com/1vIhDJwtG5eNmiqX/ArcGIS/rest/services/{}/FeatureServer/{}/updateFeatures?token={}'.format(name_of_FS, index_of_layer_in_FS, token)
    update_params    = urllib.urlencode({'Features': features_json, 'f':'json'})


    # Update the features
    print '  Updating Features in FS: {}'.format(name_of_FS)
    print '                 At index: {}'.format(index_of_layer_in_FS)
    print '   OBJECTID to be updated: {}'.format(object_id)
    print '      Field to be updated: {}'.format(field_to_update)
    print '   New value for updt fld: {}'.format(new_value)

    ##print update_url + update_params
    response  = urllib2.urlopen(update_url, update_params)
    response_json_obj = json.load(response)
    ##print response_json_obj

    for result in response_json_obj['updateResults']:
        ##print result
        print '    OBJECTID: {}'.format(result['objectId'])
        print '      Updated? {}'.format(result['success'])
        if result['success'] != True:
            success = False

    print 'Finished AGOL_Update_Features()\n'
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
