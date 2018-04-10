#-------------------------------------------------------------------------------
# Name:        Download_DA_Fire_Data.py
# Purpose:
"""
To download data from an AGOL Feature Service.  This script will download ALL
of the data in the FS regardless of the size of the data or the number of
features returned by the server.

NOTE: This script has has had a section added to it that writes a success or
error file to a specific folder (success_error_folder).  This section is
at the end of the main() (before the footer for the log file is written).
It is intended to communicate to other scripts if this script had a successful
run or not.  The success_error_folder is first deleted to remove any files
from previous runs of the script, then a success or error file is written
to disk.

The users set many of the variables in a config file:
    1) Username and Password of an AGOL account that has permission to download
       the data (used to get the token).
    2) Username and Password of a google account that can be used to send an
       email.
    3) Path the the log file folder
    4) Name of the Feature Service to download
    5) Feature Service index
    6) FGDB name
    7) FC name

    Format for config file:

        [AGOL]
        usr: lueggis
        pwd: xxxxx

        [email]
        usr: dplugis@gmail.com
        pwd: xxxxx

        [Download_Info]
        # Feature Service Name
        FS_name   =

        # Index of the layer in the Feature Service on AGOL that you want to download
        FS_index =

        [Paths]
        # Root folder for project
        Root_Folder =

Users set some variables in this script:
  Name of this script
  Location of the config file
  Email addresses to get a notification of success or failure.
"""
#
# Author:      mgrue
#
# Created:     10/11/2017
# Copyright:   (c) mgrue 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import arcpy, sys, datetime, os, ConfigParser, time, shutil
arcpy.env.overwriteOutput = True

def main():

    #---------------------------------------------------------------------------
    #                     Set Variables that will change

    # Name of this script
    name_of_script = 'DA_Download_Fire_Data.py'

    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    #                   Use cfgFile to set the below variables
    try:
        # Full path to a text file that has the username and password of an account
        #  that has access to at least VIEW the FS in AGOL, as well as an email
        #  account that has access to send emails.
        cfgFile     = r"P:\Damage_Assessment_GIS\Fire_Damage_Assessment\DEV\Scripts\Config_Files\DA_Main_Config_File.ini"
        if not os.path.exists(cfgFile):  # Try another path for the ini file
            cfgFile = r"C:\Users\mgrue\Desktop\DA_Main_Config_File.ini"

        if os.path.isfile(cfgFile):
            print 'Using INI file found at: {}'.format(cfgFile)
            config = ConfigParser.ConfigParser()
            config.read(cfgFile)
        else:
            print("*** ERROR! cannot find valid INI file ***\nMake sure a valid INI file exists at:\n\n{}\n".format(cfgFile))
            print 'You may have to change the name/location of the INI file,\nOR change the variable in the script.'
            raw_input('\nPress ENTER to end script...')
            sys.exit()

        # FS_name is the name of the Feature Service (FS) with the layer you want
        #  to download (d/l).  For example: "Homeless_Activity_Sites"
        FS_name        = config.get('Download_Info', 'FS_name')

        # Index of the layer in the FS you want to d/l.  Frequently 0.
        index_of_layer = config.get('Download_Info', 'FS_index')

        # Set root folder
        root_folder    = config.get('Paths', 'Root_Folder')

        # Set the FGDB name and FC name to hold the new AGOL data (timestamp added in script)
        data_folder = '{}\Data'.format(root_folder)
        FGDB_name   = 'DA_Fire_From_AGOL.gdb'
        FC_name     = 'DA_Fire_from_AGOL'

        # Set the log file path
        log_file_folder = '{}\Scripts\Logs'.format(root_folder)
        log_file = r'{}\{}'.format(log_file_folder, name_of_script.split('.')[0])

        # Set the path to the success/fail files
        success_error_folder = '{}\Scripts\Source_Code\Control_Files\Success_Error'.format(root_folder)

    except Exception as e:
        print '*** ERROR! There was a problem setting variables from the config file'
        print str(e)
        time.sleep(5)
        sys.exit()

    #---------------------------------------------------------------------------
    # Set the Email variables
    ##email_admin_ls = ['michael.grue@sdcounty.ca.gov', 'randy.yakos@sdcounty.ca.gov', 'gary.ross@sdcounty.ca.gov']
    email_admin_ls = ['michael.grue@sdcounty.ca.gov']

    #---------------------------------------------------------------------------
    #                Set Variables that will probably not change

    # We will get all the fields
    AGOL_fields = '*'

    # Flag to control if there is an error
    success = True

    #---------------------------------------------------------------------------
    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    #---------------------------------------------------------------------------
    #                          Start Calling Functions

    # Make sure the log file folder exists, create it if it does not
    if not os.path.exists(log_file_folder):
        print 'NOTICE, log file folder does not exist, creating it now\n'
        os.mkdir(log_file_folder)

    # Turn all 'print' statements into a log-writing object
    if success == True:
        try:
            orig_stdout, log_file_date = Write_Print_To_Log(log_file, name_of_script)
        except Exception as e:
            success = False
            print '*** ERROR with Write_Print_To_Log() ***'
            print str(e)

    #---------------------------------------------------------------------------
    #                         Check Folder Schema.
    #          Confirm all folders/files needed in this script exist
    # Make sure data_folder exists, create it if it does not
    if not os.path.exists(data_folder):
        print 'NOTICE, Working Folder does not exist, creating it now\n'
        os.mkdir(data_folder)

    # Make sure FGDB_name exists in the data_folder, create it if it does not
    if not os.path.exists(data_folder + '\\' + FGDB_name):
        print 'NOTICE, FGDB does not exist, creating it now\n'
        arcpy.CreateFileGDB_management(data_folder, FGDB_name, 'CURRENT')

    #---------------------------------------------------------------------------
    # Get a token with permissions to view the data
    if success == True:
        try:
            token = Get_Token(cfgFile)
        except Exception as e:
            success = False
            print '*** ERROR with Get_Token() ***'
            print str(e)

    #---------------------------------------------------------------------------
    # Download the data
    if success == True:

        # Set the full FS URL. "1vIhDJwtG5eNmiqX" is the CoSD portal server so it shouldn't change much.
        FS_url  = r'https://services1.arcgis.com/1vIhDJwtG5eNmiqX/arcgis/rest/services/{}/FeatureServer'.format(FS_name)

        # Set the name of the FC we want to create in our FGDB w/ Date and Time
        try:
            dt_to_append = Get_DT_To_Append()
            FC_name_date = FC_name + '_' + dt_to_append
        except Exception as e:
            print '*** ERROR with Get_DT_To_Append() ***'
            print str(e)

        # Download the data
        try:
            Get_AGOL_Data_All(AGOL_fields, token, FS_url, index_of_layer, data_folder, FGDB_name, FC_name_date)
        except Exception as e:
            success = False
            print '*** ERROR with Get_AGOL_Data_All() ***'
            print str(e)

    #---------------------------------------------------------------------------
    # Write a file to disk to let other scripts know if this script ran
    # successfully or not
    try:
        # Delete the success_error_folder to remove any previously written files
        if os.path.exists(success_error_folder):
            print '\nDeleting the folder at:\n  {}'.format(success_error_folder)
            shutil.rmtree(success_error_folder)
            time.sleep(3)

        # Create the empty success_error_folder
        print '\nMaking a folder at:\n  {}'.format(success_error_folder)
        os.mkdir(success_error_folder)

        # Set a file_name depending on the 'success' variable.
        if success == True:
            file_name = 'SUCCESS_running_{}.txt'.format(name_of_script.split('.')[0])

        else:
            file_name = 'ERROR_running_{}.txt'.format(name_of_script.split('.')[0])

        # Write the file
        file_path = '{}\{}'.format(success_error_folder, file_name)
        print '\nCreating file:\n  {}\n'.format(file_path)
        open(file_path, 'w')

    except Exception as e:
        success = False
        print '*** ERROR with Writing a Success or Fail file() ***'
        print str(e)

    #---------------------------------------------------------------------------
    # Email recipients
    if success == True:
        subj = 'SUCCESS running {}'.format(name_of_script)
        body = """Success<br>
        The Log file is at: {}""".format(log_file_date)

    else:
        subj = 'ERROR running {}'.format(name_of_script)
        body = """There was an error with this script.<br>
        Please see the log file for more info.<br>
        The Log file is at: {}""".format(log_file_date)
    try:
        Email_W_Body(subj, body, email_admin_ls, cfgFile)
    except Exception as e:
        print 'WARNING! Email not sent.  This is to be expected if the script'
        print 'is running on a server w/o email capabilities.  Error msg:\n  {}'.format(str(e))

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

    if success == True:
        print '\nSUCCESSFULLY ran {}'.format(name_of_script)
        print 'Please find downloaded data at:\n  {}\n'.format(data_folder)
    else:
        print '\n*** ERROR with {} ***'.format(name_of_script)
        print 'Please see log file (noted above) for troubleshooting\n'

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
#                             FUNCTION Get_AGOL_Data_All()
def Get_AGOL_Data_All(AGOL_fields, token, FS_url, index_of_layer, data_folder, wkg_FGDB, FC_name):
    """
    PARAMETERS:
      AGOL_fields (str) = The fields we want to have the server return from our query.
        use the string ('*') to return all fields.
      token (str) = The token obtained by the Get_Token() which gives access to
        AGOL databases that we have permission to access.
      FS_url (str) = The URL address for the feature service.
        Should be the service URL on AGOL (up to the '/FeatureServer' part).
      index_of_layer (int)= The index of the specific layer in the FS to download.
        i.e. 0 if it is the first layer in the FS, 1 if it is the second layer, etc.
      data_folder (str) = Full path to the folder that contains the FGDB that you
        want to download the data into.  FGDB must already exist.
      wkg_FGDB (str) = Name of the working FGDB in the data_folder.
      FC_name (str) = The name of the FC that will be created to hold the data
        downloaded by this function.  This FC gets overwritten every time the
        script is run.

    RETURNS:
      None

    FUNCTION:
      To download ALL data from a layer in a FS on AGOL, using OBJECTIDs.
      This function, establishs a connection to the
      data, finds out the number of features, gets the highest and lowest OBJECTIDs,
      and the maxRecordCount returned by the server, and then loops through the
      AGOL data and downloads it to the FGDB.  The first time the data is d/l by
      the script it will create a FC.  Any subsequent loops will download the
      next set of data and then append the data to the first FC.  This looping
      will happen until all the data has been downloaded and appended to the one
      FC created in the first loop.

    NOTE:
      Need to have obtained a token from the Get_Token() function.
      Need to have an existing FGDB to download data into.
    """
    print '--------------------------------------------------------------------'
    print 'Starting Get_AGOL_Data_All()'

    import urllib2, json, urllib

    # Set URLs
    query_url = FS_url + '/{}/query'.format(index_of_layer)
    print '  Downloading all data found at: {}/{}\n'.format(FS_url, index_of_layer)

    #---------------------------------------------------------------------------
    #        Get the number of records are in the Feature Service layer

    # This query returns ALL the OBJECTIDs that are in a FS regardless of the
    #   'max records returned' setting
    query = "?where=1=1&returnIdsOnly=true&f=json&token={}".format(token)
    obj_count_URL = query_url + query
    ##print obj_count_URL  # For testing purposes
    response = urllib2.urlopen(obj_count_URL)  # Send the query to the web
    obj_count_json = json.load(response)  # Store the response as a json object
    try:
        object_ids = obj_count_json['objectIds']
    except:
        print '*** ERROR! ***'
        print '  {}'.format(obj_count_json['error']['message'])
        print '  Is the Feature Service Name correct?'
        print '  URL: {}'.format(obj_count_URL)

    num_object_ids = len(object_ids)
    print '  Number of records in FS layer: {}'.format(num_object_ids)

    #---------------------------------------------------------------------------
    #                  Get the lowest and highest OBJECTID
    object_ids.sort()
    lowest_obj_id = object_ids[0]
    highest_obj_id = object_ids[num_object_ids-1]
    print '  The lowest OBJECTID is: {}\n  The highest OBJECTID is: {}'.format(\
                                                  lowest_obj_id, highest_obj_id)

    #---------------------------------------------------------------------------
    #               Get the 'maxRecordCount' of the Feature Service
    # 'maxRecordCount' is the number of records the server will return
    # when we make a query on the data.
    query = '?f=json&token={}'.format(token)
    max_count_url = FS_url + query
    ##print max_count_url  # For testing purposes
    response = urllib2.urlopen(max_count_url)
    max_record_count_json = json.load(response)
    max_record_count = max_record_count_json['maxRecordCount']
    print '  The max record count is: {}\n'.format(str(max_record_count))


    #---------------------------------------------------------------------------

    # Set the variables needed in the loop below
    start_OBJECTID = lowest_obj_id  # i.e. 1
    end_OBJECTID   = lowest_obj_id + max_record_count - 1  # i.e. 1000
    last_dl_OBJECTID = 0  # The last downloaded OBJECTID
    first_iteration = True  # Changes to False at the end of the first loop

    while last_dl_OBJECTID <= highest_obj_id:
        where_clause = 'OBJECTID >= {} AND OBJECTID <= {}'.format(start_OBJECTID, end_OBJECTID)

        # Encode the where_clause so it is readable by URL protocol (ie %27 = ' in URL).
        # visit http://meyerweb.com/eric/tools/dencoder to test URL encoding.
        # If you suspect the where clause is causing the problems, uncomment the
        #   below 'where = "1=1"' clause.
        ##where_clause = "1=1"  # For testing purposes
        print '  Getting data where: {}'.format(where_clause)
        where_encoded = urllib.quote(where_clause)
        query = "?where={}&outFields={}&returnGeometry=true&f=json&token={}".format(where_encoded, AGOL_fields, token)
        fsURL = query_url + query

        # Create empty Feature Set object
        fs = arcpy.FeatureSet()

        #---------------------------------------------------------------------------
        #                 Try to load data into Feature Set object
        # This try/except is because the fs.load(fsURL) will fail whenever no data
        # is returned by the query.
        try:
            ##print 'fsURL %s' % fsURL  # For testing purposes
            fs.load(fsURL)
        except:
            print '*** ERROR, data not downloaded ***'

        #-----------------------------------------------------------------------
        # Process d/l data

        if first_iteration == True:  # Then this is the first run and d/l data to the FC_name
            path = data_folder + "\\" + wkg_FGDB + '\\' + FC_name
        else:
            path = data_folder + "\\" + wkg_FGDB + '\\temp_to_append'

        #Copy the features to the FGDB.
        print '    Copying AGOL database features to: %s' % path
        arcpy.CopyFeatures_management(fs,path)

        # If this is a subsequent run then append the newly d/l data to the FC_name
        if first_iteration == False:
            orig_path = data_folder + "\\" + wkg_FGDB + '\\' + FC_name
            print '    Appending:\n      {}\n      To:\n      {}'.format(path, orig_path)
            arcpy.Append_management(path, orig_path, 'NO_TEST')

            print '    Deleting temp_to_append'
            arcpy.Delete_management(path)

        # Set the last downloaded OBJECTID
        last_dl_OBJECTID = end_OBJECTID

        # Set the starting and ending OBJECTID for the next iteration
        start_OBJECTID = end_OBJECTID + 1
        end_OBJECTID   = start_OBJECTID + max_record_count - 1

        # If we reached this point we have gone through one full iteration
        first_iteration = False
        print ''

    if first_iteration == False:
        print "  Successfully retrieved data.\n"
    else:
        print '  * WARNING, no data was downloaded. *'

    print 'Finished Get_AGOL_Data_All()'

    return

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

    print 'Starting Email_W_Body()'
    print '  With Subject: {}'.format(subj)

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

    print 'Successfully emailed results.'

#-------------------------------------------------------------------------------
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#-------------------------------------------------------------------------------
if __name__ == '__main__':
    main()
