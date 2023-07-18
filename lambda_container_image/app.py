from dynamoDB_handler import DynamoDB_handler
import boto3
from boto3 import Session
import os
import json
from datetime import datetime
from io import StringIO, BytesIO
import pandas as pd
import numpy as np

from google.auth import aws
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

list_of_dep = ['CO', 'DBT', 'DCMS', 'DLUHC', 'DVLA', 'DVSA', 'DWP', 'DEFRA', 'DfT', 'FCDO', 'HMCTS', 'HMRC', 'HMT',
                'HO', 'MoD', 'MoJ', 'SLC', 'GDS', 'Homes England', 'DESNZ', 'DFE']


def google_api_connection(credential_json):

    '''
        Creates a connection between AWS and the specified google service account using the specified
        credentials json file. The access is limited to google drive space, google drive files and spreadsheets.

        :param credential_json: json credentials for service account using Workload Identity Pools
    '''

    # Configure AWS credentials for google auth
    session = Session()
    credentials = session.get_credentials()

    # Credentials are refreshable, so accessing your access key / secret key
    # separately can lead to a race condition. Use this to get an actual matched
    # set.
    current_credentials = credentials.get_frozen_credentials()

    os.environ["AWS_ACCESS_KEY_ID"] = current_credentials.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = current_credentials.secret_key
    if current_credentials.token is not None:
        os.environ["AWS_SESSION_TOKEN"] = current_credentials.token
    os.environ["AWS_REGION"] = "eu-west-2"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

    # Load google credentials configuration file
    json_file_buffer = open(os.path.join(os.path.dirname(__file__), credential_json))
    json_config_info = json.load(json_file_buffer)
    credentials = aws.Credentials.from_info(json_config_info)
    scope =['https://www.googleapis.com/auth/spreadsheets',
            "https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    scoped_credentials = credentials.with_scopes(scope)

    # Create the google api connection
    service = build('drive', 'v3', credentials=scoped_credentials)

    return(service)

def wfc_folder_valid_files(service):

    '''
        Extracts the list of files in the WFC google drive folder and returns dataframe with columns 
        ['name', 'id', 'createdTime', 'quarter']

        :param service: Google api build connection
    '''

    list_of_files = service.files().list(\
        corpora='drive',\
            q="'1_RPJ74ceKlxU3JnGw681FLchz8mBtyA5' in parents",\
                driveId='0AJrAiNj_H4RzUk9PVA',\
                    includeItemsFromAllDrives=True,\
                        supportsAllDrives=True,\
                            spaces='drive',\
                                fields='nextPageToken, ''files(id, name, createdTime, trashed)',\
                                    orderBy='createdTime desc'\
                                        ).execute()

    # Exclude files that are copies of other files and are trashed
    files_names_ids = [ (d['name'], d['id'], d['createdTime']) for d in list_of_files['files'] \
        if (d['trashed']==False) and ('Copy of' not in d['name'])]

    # Extract quarter name from file name
    files_df = pd.DataFrame(files_names_ids, columns = ['name', 'id', 'createdTime'])
    files_df['quarter'] = files_df['name'].map(lambda file_name: quarter_calculater(file_name))

    # Include only the oldest file for given quarter
    valid_files = files_df.groupby('quarter').apply(lambda df: df.sort_values('createdTime').iloc[0]).reset_index(drop=True)

    return valid_files

def api_data_extraction(service):

    '''
        Extracts the list of files in the WFC google drive folder and extracts the file name and file id
        for the latest created file

        :param service: Google api build connection
    '''

    list_of_files = service.files().list(corpora='drive',q="'1_RPJ74ceKlxU3JnGw681FLchz8mBtyA5' in parents"
    ,driveId='0AJrAiNj_H4RzUk9PVA',includeItemsFromAllDrives=True,
                                supportsAllDrives=True,
                                spaces='drive',
                                fields='nextPageToken, ''files(id, name)',
                                orderBy='createdTime desc').execute()

    file_name = list_of_files['files'][0]['name']
    file_id = list_of_files['files'][0]['id']

    return(file_name, file_id)

def export_csv(service, file_id):

    '''
        Downloads the interested google drive csv into lambda and returns a dataframe

        :param service: Google api build connection
        :param file_id: The google drive file id to import to aws 
    '''

    request = service.files().export_media(fileId=file_id, 
                                                    mimeType='text/csv')

    file = BytesIO()
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print(F'Download {int(status.progress() * 100)}.')

    data=str(file.getvalue(),'utf-8')
    data = StringIO(data) 
    df = pd.read_csv(data)

    return(df)

##### Preprocessing Data Functions

def cleaning_department_names(df, col_name, col_name_1):
    
    ''' 
    This function returns consistent department names from different variations.
    Currently supports OSCAR II, Spend over 25k, Organogram and ONS FTE datasets.

    :param df: Dataframe of interest
    :param col_name: Column where department names are stored
    :param col_name_1: Column where alb names are stored
    '''

    # Conditions to capture department names
    conditions_departments = [df[col_name_1].str.contains("(?i)Driver and Vehicle Standards Agency", na=False),
                              df[col_name_1].str.contains("(?i)HM Courts and Tribunals Service", na=False),
                              df[col_name_1].str.contains("(?i)DVLA|(?i)Driver and Vehicle Licensing Agency", na=False),
                              df[col_name].str.contains("(?i)cab|(?i)cabinet office", na=False),
                              df[col_name].str.contains("(?i)bis|(?i)beis|(?i)Department for Business|(?i)department business energy|(?i)business innovation|(?i)net zero", 
                                                        na=False),
                              df[col_name].str.contains("(?i)dfe|(?i)Department for Educ|(?i)Department of Education|(?i)Deparment for Education|(?i)education",
                                                        na=False),
                              df[col_name].str.contains("(?i)DFT|(?i)Department for tran", na=False),
                              df[col_name].str.contains("(?i)international trade", na=False),
                              df[col_name].str.contains("(?i)DEFRA|(?i)DEFFRA|Department for Environment", na=False),
                              df[col_name].str.contains("(?i)Department for Work|(?i)DWP|(?i)WAP|(?i)work", na=False),  
                              df[col_name].str.contains("(?i)Justice|(?i)MOJ", na=False),
                              df[col_name].str.contains("(?i)HM Treasury", na=False),
                              df[col_name].str.contains("(?i)Home Office", na=False),
                              df[col_name].str.contains("(?i)MHCLG|(?i)Housing, Communities|Local|Ministry of Communities|Department for Levelling", na=False),
                              df[col_name].str.contains("(?i)DFID|(?i)FCDO|(?i)International Development|Foreign, Commonwealth", na=False),
                              df[col_name].str.contains("(?i)hmrc|(?i)HM Revenue|Her Majesty's Revenue", na=False),
                              df[col_name].str.contains("(?i)mod|(?i)defence|(?i)m0d", na=False),
                              df[col_name].str.contains("(?i)Culture, Media and Sport|(?i)culture media sport", na=False),
                             ]
                             
    # Assigning department names to condition                      
    choices_departments = ['DVSA'] + ["HMCTS"] + ["DVLA"] + ["CO"] + ["DESNZ"] + ["DfE"] + ["DfT"] + ["DIT"] + \
                            ["DEFRA"] + ["DWP"] + ["MOJ"] + ["HMT"] + ["HO"] + ["DLUHC"] + \
                            ["FCDO"] + ["HMRC"] + ["MOD"] + ["DCMS"] 
    # Applying condition
    df["department_name"] = np.select(conditions_departments, choices_departments, default = df[col_name])
                                                  
    return(df)


def alb_split_commission(df, col, department_correction):
    '''
        Splitting ALB's and correcting short names to allign with commission. Returns df with alb and departments
        for the commission in one column

        :param df: Dataframe of interest
        :param col: Column with ALB name
        :param department_correction: Column with department name

    '''
    condition = [df[col] == 'HMCTS',
                 df[col] == 'DVLA',
                 df[col] == 'DVSA',
                 df[col] == 'SLC',
                 df[col] == 'FSA',
                 df[col] == 'GDS',
                 df[department_correction] == 'DfE',
                 df[department_correction] == 'DfT',
                 df[department_correction] == 'DIT',
                 df[department_correction] == 'Defra',
                 df[department_correction] == 'Cabinet Office',
                 df[department_correction] == 'HM Treasury',
                 df[department_correction] == 'Home Office',
                 df[department_correction] == 'MoD',
                 df[department_correction] == 'MoJ',
                 df[department_correction] == 'DESNZ',
                 df[department_correction] == 'DSIT']
    
    choices = ['HMCTS', 'DVLA', 'DVSA', 'SLC', 'FSA', 'GDS' ,'DFE', 'DFT', 'DBT', 'DEFRA', 'CO', 'HMT', 'HO', 'MOD', 'MOJ', 'DESNZ&DSIT', 'DESNZ&DSIT']
    
    df['department_name'] = np.select(condition, choices, default = df[department_correction])
    
    return(df)


def wfc_calculation(df, empl_type, role_status):
    '''
        Calculation for different wfc metrics split by employement type and role status (filled or vacant)
        :param df: df with wfc data
        :param empl_type: list of relevent employement types. None is not needed
        :param role_status: list of relevent role status. None is not needed
    '''
    
    if empl_type != None:
        df = df[df['Employment Type'].isin(empl_type)]
    
    if role_status != None:
        df = df[df['Role Status'].isin(role_status)]

    df['FTE (Person)'] = pd.to_numeric(df['FTE (Person)'], errors='coerce')

    df = df.groupby('department_name')['FTE (Person)'].sum().reset_index()

    return(df)

def quarter_calculater(file_name):
    '''
        Function to calculate the quarter based on the file name 
        :param file_name: File name for wfc data
    '''

    date = pd.to_datetime(file_name.split("-")[1].replace('.csv','').strip())
    month = date.month

    if month >=4 and month < 7:
        quarter = 'Q4'
        year = date.year - 1
    elif month >=7 and month < 10:
        quarter = 'Q1'
        year = date.year
    elif month >=1 and month < 4:
        quarter = 'Q3'
        year = date.year - 1
    else:
        quarter = 'Q2'
        year = date.year

    year_1 = str(year + 1)[-2:]

    return(quarter + ' ' + str(year) + '/' + year_1)

def preprocessing(df, file_name):
    '''
        WFC data preprocessing function. This function takes the raw data, filters for the department of interest
        and outputs a df alligned with the commission format data
        :param df: df with raw wfc data
    '''
    
    if 'ALB, Agency, Business Unit or Organisation' in df.columns:
        data = alb_split_commission(df, 'ALB, Agency, Business Unit or Organisation', 'Department')
    elif 'ALB, Agency or Organisation' in df.columns:
        data = alb_split_commission(df, 'ALB, Agency or Organisation', 'Department')
    elif 'ALB, Agency, Business Unit or Organisation ' in df.columns:
        data = alb_split_commission(df, 'ALB, Agency, Business Unit or Organisation ', 'Department')
    else:
        raise Exception('Issue with ALB colum!')

    data = data[data['Profession'] == 'Digital, Data and Technology']

    list_of_dep = ['CO', 'DBT', 'DCMS', 'DLUHC', 'DVLA', 'DVSA', 'DWP', 'DEFRA', 'DFT', 'FCDO', 'HMCTS', 'HMRC', 'HMT',
                'HO', 'MOD', 'MOJ', 'SLC', 'GDS', 'Homes England', 'DESNZ&DSIT', 'DFE', 'DESNZ&DSIT']

    data = data[data['department_name'].isin(list_of_dep)]

    # Civil Service Headcount
    civil_serivce_ddat_fte = wfc_calculation(data, empl_type = ['Permanent', 'Fixed term'], role_status = ['Filled'])
    civil_serivce_ddat_fte = civil_serivce_ddat_fte.rename(columns={'FTE (Person)':'QM06009'})

    # Temporary Headcount
    contractors_ddat_fte = wfc_calculation(data, empl_type = ['Temporary'], role_status = ['Filled'])
    contractors_ddat_fte = contractors_ddat_fte.rename(columns={'FTE (Person)':'QM06010'})

    # Merging Data
    wfc_data = pd.merge(civil_serivce_ddat_fte, contractors_ddat_fte, on = 'department_name', how = 'outer')

    # Vacancy Rate
    filled_roles = wfc_calculation(data, empl_type=None,role_status = ['Filled'])
    vacant_roles = wfc_calculation(data, empl_type=None,role_status = ['Vacancy'])

    vacancy_rate = pd.merge(vacant_roles, filled_roles, on='department_name')
    vacancy_rate['QM06011'] = round(100* (vacancy_rate['FTE (Person)_x'] / (vacancy_rate['FTE (Person)_y'] + vacancy_rate['FTE (Person)_x'] ) ),1)
    vacancy_rate = vacancy_rate.drop(columns=['FTE (Person)_x', 'FTE (Person)_y'])

    wfc_data = pd.merge(wfc_data, vacancy_rate, on = 'department_name', how='outer')
    wfc_data['QM00004'] = quarter_calculater(file_name)
    wfc_data['QM00001'] = 'WFC'
    wfc_data = wfc_data.rename(columns={'department_name':'QM00003'})

    return(wfc_data)


def prepare_df_to_DynamoDB(df):
    """
    prepare_df_to_DynamoDB prepares csv data for upload to DynamoDB.

    :param df: csv file as data frame
    :return: returns transformaed data frame
    """ 

    df['QM00002'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['QM00006'] = 'Department'
    return df

def handler(event, context):
    """
    handler process csv file and uploads to DynamoDB.

    :param event: event element triggering lambda function
    """ 
    
    dynamoDB_handler_instance = DynamoDB_handler(questions_from_s3=True)
    
    # WFC Data
    client_google = app.google_api_connection('clientLibraryConfig-aws1.json')
    valid_files = app.wfc_folder_valid_files(client_google)

    # Check quarters already uploaded to prevent reupload of same data
    dynamoDB_records = dynamoDB_handler_instance.extract_filter_data(source = 'WFC')
    previous_quarters = set(map(lambda record: record['quarter']['S'], dynamoDB_records))

    # Exclude already uploaded quarters
    new_valid_files = valid_files.loc[~valid_files.quarter.isin(previous_quarters)]

    # Process and upload files to DynamoDB
    dfs = []
    for index, row in new_valid_files.iterrows():
        file_id = row['id']
        file_name = row['name']
        print(file_id, file_name)

        df = app.export_csv(client_google, file_id)
        df = app.preprocessing(df, file_name)
        df = app.prepare_df_to_DynamoDB(df)

        dfs.append(df)

    if len(dfs)>0:
        dynamoDB_handler_instance.upload_data_frame_to_DynamoDB(pd.concat(dfs))

    return 200

if __name__ == '__main__':
    handler(None, None)
