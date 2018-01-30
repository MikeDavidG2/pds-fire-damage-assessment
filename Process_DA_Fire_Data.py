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
    wkg_folder           = config.get('Process_Info', 'wkg_folder')
    raw_agol_FGDB        = config.get('Process_Info', 'raw_agol_FGDB')
    raw_agol_FGDB_path   = '{}\{}'.format(wkg_folder, raw_agol_FGDB)
    processing_FGDB      = config.get('Process_Info', 'Processing_FGDB')
    processing_FGDB_path = '{}\{}'.format(wkg_folder, processing_FGDB)

    # Set the log file folder path
    log_file_folder = config.get('Process_Info', 'Log_File_Folder')
    log_file = r'{}\{}'.format(log_file_folder, name_of_script.split('.')[0])

    # Set the Control_Files path
    control_file_folder = config.get('Process_Info', 'Control_Files')
    add_fields_csv      = '{}\FieldsToAdd.csv'.format(control_file_folder)
    calc_fields_csv     = '{}\FieldsToCalculate.csv'.format(control_file_folder)

    # Set the PARCELS_ALL Feature Class path
    parcels_all = config.get('Process_Info', 'Parcels_All')

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
            orig_stdout, log_file_date = Write_Print_To_Log(log_file, name_of_script)
        except Exception as e:
            success = False
            print '*** ERROR with Write_Print_To_Log() ***'
            print str(e)

    # Get a token with permissions to view the data
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
    AGOL_downloads = arcpy.ListFeatureClasses()  # List all FC's in the FGDB
    for download in AGOL_downloads:
        newest_download = download  # Only the last FC in the list is kept after the loop

    newest_download_path = '{}\{}'.format(raw_agol_FGDB_path, newest_download)
    print 'The newest download is at: {}\n'.format(newest_download_path)

    # Spatially Join the downloaded data with the PARCELS_ALL
    target_features   = newest_download_path
    join_features     = parcels_all
    working_fc = '{}\{}_joined'.format(processing_FGDB_path, newest_download)

    print 'Spatially Joining:\n  {}\nWith:\n  {}\nNew FC at:\n  {}\n'.format(target_features, parcels_all, working_fc)
    arcpy.SpatialJoin_analysis(target_features, join_features, working_fc)

    # Add Fields to downloaded DA Fire Data
##    working_fc = r'P:\Damage_Assessment_GIS\Fire_Damage_Assessment\DEV\Data\DA_Fire_Processing.gdb\DA_Fire_from_AGOL_2018_01_26__14_28_48'
    Fields_Add_Fields(working_fc, add_fields_csv)

    # Calculate Fields
    Fields_Calculate_Fields(working_fc, calc_fields_csv)

    # Backup the production database before attempting to edit it

    # Append newly processed data into the production database

    # Overwrite the FS that the Dashboard is pointing to

    # Update AGOL fields
    # TODO: add function here to:
    #   Update (in AGOL) NULL [Quantity] to equal 1,
    #   Update (in AGOL) NULL [EstimatedReplacementCost] to equal SquareFootageDamaged * x,


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

    return orig_stdout, log_file_date

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
    print 'Adding fields to:\n  %s' % wkg_data
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

    print 'Successfully added fields.\n'

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
    print 'Calculating fields in:\n  %s' % wkg_data
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
                    print '*** WARNING! Field: %s was not able to be calculated.***\n' % field
                    print str(e)

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
                    print '*** WARNING! Field: %s was not able to be calculated.***\n' % field
                    print str(e)

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
                    print '*** WARNING! Field: %s was not able to be calculated.***\n' % field
                    print str(e)

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

    print 'Successfully calculated fields.\n'

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
