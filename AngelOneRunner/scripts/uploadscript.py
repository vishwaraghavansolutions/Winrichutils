# import pandas as pd
# import pandas_gbq
# from google.cloud import bigquery
# from google.cloud import storage
# # from datetime import datetime
# import datetime
# import pytz
# import os
# import numpy as np

# def download_file_from_gcs(bucket_name, source_blob_name, destination_file_name):
#     """Downloads a file from Google Cloud Storage."""
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(source_blob_name)
#     blob.download_to_filename(destination_file_name)
#     print(f'File downloaded from GCS: {source_blob_name}')

# def execute_sql_queries(table_name):
#     client = bigquery.Client()

#     if(table_name == 'liquiloans_master'):

#         column_name = 'name'

#     elif(table_name == 'mutualfunds_master' or table_name == 'equity_master'):

#         column_name = 'h_name'

#     elif(table_name == 'sip_master'):

#         column_name = 'c_name'

#     elif(table_name == 'vested'):

#         column_name = 'Client_Name'

#     elif(table_name == 'unify'):

#         column_name = 'Name'

#     elif(table_name == 'icici_pms'):

#         column_name = 'clientname'

#     elif(table_name == 'hbits'):

#         column_name = 'name_of_the_investor'

#     elif(table_name == 'insurance_icici'):

#         column_name = 'Customer_Full_Name'

#     elif(table_name == 'insurance_max_bupa'):

#         column_name = 'Full_Name'

#     elif(table_name == 'fixed_deposit'):

#         column_name = 'Customer_Name'

#     # First SQL query
#     query1 = f"""
#         UPDATE `elegant-tendril-399501.winrich_dev_v2.{table_name}` AS fd
#         SET master_customer_id = (
#             SELECT mcd.master_customer_id
#             FROM `elegant-tendril-399501.winrich_dev_v2.master_customers_data` AS mcd
#             WHERE LOWER(REPLACE(TRIM(fd.{column_name}), ' ', '')) = LOWER(REPLACE(TRIM(mcd.master_username), ' ', '')) 
#         )
#         WHERE fd.master_customer_id is null and Date(fd.created_at) = CURRENT_DATE();
#         """
#     # Second SQL query
#     query2 = f"""
#     UPDATE `elegant-tendril-399501.winrich_dev_v2.{table_name}` AS fd
#     SET master_customer_id = (
#         SELECT ons.master_customer_id
#         FROM `elegant-tendril-399501.winrich_dev_v2.other_names` AS ons
#         WHERE LOWER(REPLACE(TRIM(fd.{column_name}), ' ', '')) = LOWER(REPLACE(TRIM(ons.other_names), ' ', '')) 
#     )
#     WHERE fd.master_customer_id is null and Date(fd.created_at) = CURRENT_DATE();
#     """

#     # Third SQL query
#     query3 = """
#     DELETE FROM elegant-tendril-399501.winrich_dev_v2.sip_master
#     WHERE c_name = 'nan' AND
#     month1 = 0.0
#     AND month2 = 0.0
#     AND month3 = 0.0
#     AND month4 = 0.0
#     AND month5 = 0.0
#     AND month6 = 0.0
#     AND month7 = 0.0
#     AND month8 = 0.0
#     AND month9 = 0.0
#     AND month10 = 0.0
#     AND month11 = 0.0
#     AND month12 = 0.0;
#     """

#     # Execute the first SQL query
#     query_job1 = client.query(query1)
#     query_job1.result()  # Waits for the query to finish
#     print("First data mapping successful.")

#     # Execute the second SQL query
#     query_job2 = client.query(query2)
#     query_job2.result()  # Waits for the query to finish
#     print("Second data mapping successful.")

#     if(table_name == 'sip_master'):

#         # Execute the Third SQL query
#         query_job3 = client.query(query3)
#         query_job3.result()  # Waits for the query to finish
#         print("Null Rows Deleted From SIP Master")

# def get_table_schema(table_name):
#     print('under table_schema')
#     client = bigquery.Client()
#     dataset_ref = client.dataset('winrich_dev_v2')
#     table_ref = dataset_ref.table(table_name)
#     table = client.get_table(table_ref)

#     print('table: ', table)
    
#     schema = [(field.name, field.field_type) for field in table.schema]

#     print('schema: ', schema)
#     return schema

# def hello_gcs(event, context):
#     client = bigquery.Client()

#     file_name = event['name'].lower()
#     print('file_name: ', file_name)

#     dataset_ref = client.dataset('winrich_dev_v2')
#     tables = client.list_tables(dataset_ref)
#     table_names = [table.table_id for table in tables]

#     # table_name = next((table for table in table_names if table in file_name), file_name.split('.')[0])

#     print('%%%%%%%%%%%%%%%%%%%%%%%%%')
#     # print(table_name)

#     if 'deposit' in file_name:
#         table_name = 'fixed_deposit'
#     elif 'sipmom' in file_name:
#         table_name = 'sip_master'
#     elif 'equity' in file_name or 'stocks' in file_name:
#         table_name = 'equity_master'
#     elif 'liqui' in file_name:
#         table_name = 'liquiloans_master'
#     elif 'mutual' in file_name:
#         table_name = 'mutualfunds_master'
#     elif 'strata' in file_name:
#         table_name = 'strata'
#     elif 'unify' in file_name:
#         table_name = 'unify'
#     elif 'pms' in file_name or 'icici_pms' in file_name:
#         table_name = 'icici_pms'
#     elif 'hbits' in file_name:
#         table_name = 'hbits'
#     elif 'icici' in file_name:
#         table_name = 'insurance_icici'
#     elif 'max_bupa' in file_name:
#         table_name = 'insurance_max_bupa'
#     elif 'vested' in file_name or 'funded' in file_name or 'accounts' in file_name:
#         table_name = 'vested'
#     else:
#         table_name = file_name.split('.')[0]


#     print(f'File: {file_name}, Table: {table_name}')
#     current_time = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")

#     print('current time: ', current_time)
#     print('type: ', type(current_time))

#     try:
#         file_path = f'/tmp/{os.path.dirname(file_name)}'
#         os.makedirs(file_path, exist_ok=True)
#         file_path = f'/tmp/{file_name}'

#         print('file_path:', file_path)
#         download_file_from_gcs(event['bucket'], event['name'], file_path)

#         file_data = pd.read_csv(file_path)
#         print("CSV file loaded successfully")

#         print(f"Processing file: {file_name}")
#         print(f"Associated table: {table_name}")

#         file_data['created_at'] = datetime.datetime.now()
#         file_data['updated_at'] = datetime.datetime.now()

#         # print('time from datetime: ', datetime.datetime.now())

#         print('created_at: ', current_time)

#         table_schema = get_table_schema(table_name)
#         print("Schema for table {}: {}".format(table_name, table_schema))

#         # print('before data types: ', file_data.dtypes)

#         # Strip leading and trailing spaces from column names

#         file_data.columns = file_data.columns.str.strip()

#         if table_name == 'mutualfunds_master':

#             print('inside mutual funds')

#             column_mappings = {
#                 'sCode': 's_code',
#                 'Nature': 'nature',
#                 'Email': 'email',
#                 'Mobile': 'mobile',
#                 'FolioStartDate': 'folio_start_date',
#                 'AvgCost': 'avg_cost',
#                 'InvAmt': 'inv_amt',
#                 'TotalInvAmt': 'total_inv_amt',
#                 'CurNAV': 'cur_nav',
#                 'CurValue': 'cur_value',
#                 'DivAmt': 'div_amt',
#                 'NotionalGain': 'notional_gain',
#                 'ActualGain': 'actual_gain',
#                 'FolioXIRR': 'folio_xirr',
#                 'NatureXIRR': 'nature_xirr',
#                 'ClientXIRR': 'client_xirr',
#                 'NatureAbs': 'nature_abs',
#                 'ClientAbs': 'client_abs',
#                 'absReturn': 'abs_return',
#                 'BalUnit': 'bal_unit',
#                 'ValueDate': 'value_date',
#                 'ReportDate': 'report_date'
#             }

#             # Rename columns using the dictionary
#             file_data.rename(columns=column_mappings, inplace=True)


#             column_data_types = file_data.dtypes

#             print('after data types: ', column_data_types)
#             print('file data types')
#             print(column_data_types)

#             # Define column mappings
#             # column_mappings = {'sCode': 's_code'}

#             # Rename columns
#             # file_data.rename(columns=column_mappings, inplace=True)

#             # List of columns to be converted to string data type
#             str_columns = ['h_name', 'c_name', 's_code', 's_name', 'foliono', 'nature', 'folio_start_date', 'bal_unit' , 'email', 'mobile', 'value_date', 'report_date']

#             # List of columns to be converted to float data type
#             float_columns = ['avg_cost', 'inv_amt', 'total_inv_amt', 'cur_nav', 'cur_value', 'div_amt', 'notional_gain', 'actual_gain', 'folio_xirr', 'nature_xirr', 'client_xirr', 'nature_abs', 'client_abs', 'abs_return']

#             # Convert columns to string data type
#             file_data[str_columns] = file_data[str_columns].astype(str)

#             # Convert columns to float data type
#             file_data[float_columns] = file_data[float_columns].astype(float)

#             # print('file data columns: ', file_data)

#             # Convert 'FolioStartDate', 'ValueDate', and 'ReportDate' columns to datetime data type
#             # date_columns = ['created_at', 'updated_at']
#             # file_data[date_columns] = file_data[date_columns].apply(pd.to_datetime)


#         elif(table_name == 'liquiloans_master'):

#             print('Inside the liquiloans block')

#             # # Strip leading and trailing spaces from column names

#             # file_data.columns = file_data.columns.str.strip()

#             file_data['investori'] = file_data['investori'].astype(int)

#             print('file data name: ', file_data['name'])

#             file_data.rename(columns={'annualized_return': 'annualized_return_'}, inplace=True)

#             # file_data['name'] = file_data['name'].astype(str)

#             file_data['current_value'] = file_data['current_value'].astype(float)

#             file_data['annualized_return_'] = file_data['annualized_return_'].astype(float)


#         elif(table_name == 'sip_master'):

#             columns_to_convert = ['c_name', 'Month1', 'Month2', 'Month3', 'Month4', 'Month5', 'Month6', 'Month7', 'Month8', 'Month9', 'Month10', 'Month11', 'Month12']

#             for column in columns_to_convert:
#                 if column == 'c_name':
#                     file_data[column] = file_data[column].astype(str)
#                 else:
#                     file_data[column] = file_data[column].astype(float)

#         elif(table_name == 'icici_pms'):

#             print('under icici pms')

#             # Find the row index where the actual data starts (assuming it starts after a specific header row)
#             start_index = file_data[file_data.iloc[:, 0] == 'CLIENTCODE'].index[0]

#             # Remove the unwanted header rows
#             file_data = file_data.iloc[start_index:]

#             # Reset the column headers
#             file_data.columns = file_data.iloc[0]
#             file_data = file_data[1:]

#             # Keep only the relevant columns
#             columns_to_keep = ['CLIENTCODE', 'CLIENTNAME', 'AUM', 'PRODUCTCODE', 'PAN']
#             file_data = file_data[columns_to_keep]

#             # Reset the index
#             file_data.reset_index(drop=True, inplace=True)

#             file_data['created_at'] = datetime.datetime.now()
#             file_data['updated_at'] = datetime.datetime.now()

#             # Print the cleaned DataFrame (for debugging)
#             print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
#             print(file_data.head())

#             file_data.rename(columns={
#             'CLIENTCODE': 'clientcode',
#             'CLIENTNAME': 'clientname',
#             'AUM': 'aum',
#             'PRODUCTCODE': 'productcode',
#             'PAN': 'pan'
#             }, inplace=True)

#             file_data = file_data.astype({
#             'clientcode': str,
#             'clientname': str,
#             'aum': float,
#             'productcode': str,
#             'pan': str
#             })

#         elif(table_name == 'hbits'):

#             file_data.rename(columns={
#             'Name of the Investor': 'name_of_the_investor',
#             "Father's Name": 'fathers_name',
#             'Investment Amount': 'investment_amount',
#             'SPV': 'spv',
#             'Investment Amount LMS': 'investment_amount_lms',
#             'Property Name': 'property_name'
#             }, inplace=True)

#             file_data = file_data.astype({
#                 'name_of_the_investor': str,
#                 'fathers_name': str,
#                 'investment_amount': int,
#                 'spv': str,
#                 'investment_amount_lms': int,
#                 'property_name': str
#             })

#         elif(table_name == 'insurance_icici'):

#             file_data.rename(columns={
#             'Policy No': 'Policy_No',
#             'Customer Full Name': 'Customer_Full_Name',
#             'Product Name': 'Product_Name',
#             'Mobile No': 'Mobile_No',
#             'Due Date': 'Due_Date',
#             'Risk Com Date': 'Risk_Com_Date',
#             'Issuance Date': 'Issuance_Date',
#             'Premium Paying Term': 'Premium_Paying_Term',
#             'Premium Amount': 'Premium_Amount',
#             'Sum Assured': 'Sum_Assured',
#             'Bill Channel': 'Bill_Channel',
#             'Suspense Account Balance': 'Suspense_Account_Balance',
#             'Net Amount Due': 'Net_Amount_Due',
#             'City': 'City____________',
#             'Phone1': 'Phone1________',
#             'Phone2': 'Phone2_______',
#             'Email': 'Email',
#             'Payment Frequency': 'Payment_Frequency'
#             }, inplace=True)

#             # converting the Due_Date and Issuance_Date to be in date time ( timestamp ) type

#             file_data['Due_Date'] = pd.to_datetime(file_data['Due_Date'])

#             file_data['Issuance_Date'] = pd.to_datetime(file_data['Issuance_Date'])

#             file_data['Risk_Com_Date'] = pd.to_datetime(file_data['Risk_Com_Date'], format='%d-%b-%Y').dt.strftime('%Y-%m-%d')

#             # List of columns to be converted to string data type
#             str_columns = ['Policy_No', 'Customer_Full_Name', 'Product_Name', 'Bill_Channel', 'City____________', 'Phone1________', 'Phone2_______', 'Email' , 'Payment_Frequency']

#             # List of columns to be converted to int data type
#             int_columns = ['Mobile_No', 'Premium_Paying_Term', 'Premium_Amount', 'Sum_Assured', 'Suspense_Account_Balance', 'Net_Amount_Due']

#             # Convert columns to string data type
#             file_data[str_columns] = file_data[str_columns].astype(str)

#             # Convert columns to int data type
#             file_data[int_columns] = file_data[int_columns].astype(int)

#         elif table_name == 'insurance_max_bupa':

#             print('under max_bupa')

#             # Remove the "First Name" column if it exists in the DataFrame
#             if 'First Name' in file_data.columns:
#                 file_data.drop(columns=['First Name'], inplace=True)

#             if 'Last Name' in file_data.columns:
#                 file_data.drop(columns=['Last Name'], inplace=True)

#             # Define the column mapping
#             column_mapping = {
#                 'Full Name': 'Full_Name',
#                 'Application Number': 'Application_No',
#                 'Previous Policy Number': 'Old_Policy_Number',
#                 'Policy Number': 'New_Policy_Number',
#                 'Customer ID': 'Customer_ID',
#                 'DOB': 'DOB',
#                 'Plan Type': 'Plan_Type',
#                 'Product ID': 'Product_ID',
#                 'Product Genre': 'Product_Genre',
#                 'Insured Lives': 'Insured_Lives',
#                 'Insured Count': 'Insured_Count',
#                 'Renewal Premium': 'Renewal_Premium',
#                 '2 Years Renewal Premium (With Tax)': '_2_Years_Renewal_Premium__With_Tax_',
#                 'Current Status': 'Current_Status_Workstep',
#                 'Issued Premium': 'Issued_Premium',
#                 'Renewal Premium (Without Taxes)': 'Renewal_Premium__Without_Taxes_',
#                 'Loading Premium': 'Loading_Premium',
#                 'Sum Assured': 'Sum_Assured',
#                 'Individual Sum Assured': 'Individual_Sum_Assured',
#                 'Health Assurance Critical Illness/Criticare Sum Assured': 'Health_Assurance_Critical_Illness_Criticare_Sum_Assured',
#                 'Health Assurance Personal Accident/Accident Care Sum Assured': 'Health_Assurance_Personal_Accident_Accident_Care_Sum_Assured',
#                 'Health Assurance Hospital Cash/Hospicash Sum Assured': 'Health_Assurance_Hospital_Cash_Hospicash_Sum_Assured',
#                 'Login Branch': 'Branch',
#                 'Sales Branch': 'Sales_Branch',
#                 'Zone': 'Zone',
#                 'Renewal Channel': 'Renewal_Channel',
#                 'Renewal Sub Channel': 'Renewal_Sub_Channel',
#                 'Renewal Agent Code': 'Renewal_Agent_Code',
#                 'Renewal Agent Name': 'Renewal_Agent_Name',
#                 'Renewal Agent Type': 'Renewal_Agent_Type',
#                 'Pa Code': 'Pa_Code',
#                 'Conversion Date': 'Conversion_Date',
#                 'Agency Manager ID': 'Agency_Manager_ID',
#                 'Agency Manager Name': 'Agency_Manager_Name',
#                 'Renewal Logged Date': 'Renewal_Logged_Date',
#                 'Renewal Logged Month': 'Renewal_Logged_Month',
#                 'Renewal Issued Date': 'Renewal_Issued_Date',
#                 'Renewal Issued Month': 'Renewal_Issued_Month',
#                 'Maximus Status': 'Maximus_Status',
#                 'Lead Status': 'Lead_Status',
#                 'Sales Status': 'Sales_Status',
#                 'Hums Status': 'Hums_Status',
#                 'Hums Status Update Date': 'Hums_Status_Update_Date',
#                 'Current Team': 'Current_Team',
#                 'Current Status Ageing': 'Current_Status_Ageing',
#                 'Login Ageing': 'Login_Ageing',
#                 'Designation': 'Designation',
#                 'Policy Start Date': 'Policy_Start_Date',
#                 'Policy Expiry Date': 'Policy_Expiry_Date',
#                 'Is Portability': 'Is_Portability',
#                 'Is Split': 'Split_Flag',
#                 'Is Upsell': 'Upsell_Eligibility',
#                 'Upsell Limit': 'Upsell_Limit',
#                 'Plan Name': 'Plan_Name',
#                 'Renew Now': 'Renew_Now',
#                 'Whatsapp Communication for Policy Information': 'Whatsapp_Communication_for_Policy_Information',
#                 'Communication Acknowledgement(Over Ride DND)': 'Communication_Acknowledgement_Over_Ride_DND_',
#                 'Safe Guard': 'Safeguard_Rider_Taken',
#                 'Policy Tenure': 'Policy_Tenure',
#                 'Product Name': 'Product_Name'
#             }

#             # Filter the column mapping to include only columns present in the DataFrame
#             filtered_column_mapping = {k: v for k, v in column_mapping.items() if k in file_data.columns}

#             # Rename the columns
#             file_data.rename(columns=filtered_column_mapping, inplace=True)

#             # List of columns to be converted to int data type
#             int_columns = [
#                 "Application_No","Old_Policy_Number","Customer_ID","Product_ID","Insured_Lives","Renewal_Premium","_2_Years_Renewal_Premium__With_Tax_","Loading_Premium","Sum_Assured","Individual_Sum_Assured","Health_Assurance_Personal_Accident_Accident_Care_Sum_Assured","Health_Assurance_Hospital_Cash_Hospicash_Sum_Assured","Whatsapp_Communication_for_Policy_Information"
#             ]

#             # Filter the list to include only columns present in the DataFrame
#             existing_int_columns = [col for col in int_columns if col in file_data.columns]

#             # Replace non-finite values with 0
#             file_data[existing_int_columns] = file_data[existing_int_columns].replace([np.inf, -np.inf, np.nan], 0)


#             # List of columns to be converted to string data type
#             str_columns = [
#                 "Full_Name",
#                 "New_Policy_Number",
#                 "Plan_Type",
#                 "Product_Genre",
#                 "Insured_Count",
#                 "Current_Status_Workstep",
#                 "Issued_Premium",
#                 "Renewal_Premium__Without_Taxes_",
#                 "Branch",
#                 "Sales_Branch",
#                 "Zone",
#                 "Renewal_Channel",
#                 "Renewal_Sub_Channel",
#                 "Renewal_Agent_Code",
#                 "Renewal_Agent_Name",
#                 "Renewal_Agent_Type",
#                 "Pa_Code",
#                 "Conversion_Date",
#                 "Agency_Manager_ID",
#                 "Agency_Manager_Name",
#                 "Renewal_Logged_Date",
#                 "Renewal_Logged_Month",
#                 "Renewal_Issued_Date",
#                 "Renewal_Issued_Month",
#                 "Maximus_Status",
#                 "Lead_Status",
#                 "Sales_Status",
#                 "Hums_Status",
#                 "Hums_Status_Update_Date",
#                 "Current_Team",
#                 "Current_Status_Ageing",
#                 "Login_Ageing",
#                 "Designation",
#                 "Is_Portability",
#                 "Upsell_Eligibility",
#                 "Upsell_Limit",
#                 "Plan_Name",
#                 "Renew_Now",
#                 "Policy_Tenure",
#                 "Product_Name"
#             ]		

#             # Filter the list to include only columns present in the DataFrame
#             existing_str_columns = [col for col in str_columns if col in file_data.columns]				

#             # List of columns to be converted to bool data type
#             bool_columns = [
#                 'Communication_Acknowledgement_Over_Ride_DND_', 'Safeguard_Rider_Taken', 'Health_Assurance_Critical_Illness_Criticare_Sum_Assured', 'Split_Flag'
#             ]

#             # Filter the list to include only columns present in the DataFrame
#             existing_bool_columns = [col for col in bool_columns if col in file_data.columns]

#             # Convert columns to string data type
#             file_data[existing_str_columns] = file_data[existing_str_columns].astype(str)

#             # Convert columns to int data type
#             file_data[existing_int_columns] = file_data[existing_int_columns].astype(int)

#             # Convert columns to bool data type
#             file_data[existing_bool_columns] = file_data[existing_bool_columns].astype(bool)

#             file_data['Policy_Start_Date'] = pd.to_datetime(file_data['Policy_Start_Date'])
#             file_data['Policy_Expiry_Date'] = pd.to_datetime(file_data['Policy_Expiry_Date'])

#         elif table_name == 'equity_master':
#             file_data = file_data.astype({
#                 'c_name': str,
#                 'h_name': str,
#                 'ClientCode': str,
#                 'ScripCode': int,
#                 'Symbol': str,
#                 'PoolHoldings': float,
#                 'PledgeHoldings': float,
#                 'DPAccountHoldings': float,
#                 'NetHoldings': float,
#                 'TotalValue': float,
#                 'Bluechip': float,
#                 'Good': float,
#                 'Average': float,
#                 'Poor': float,
#                 'ScripName': str
#             })
#             print('holdingdate: ', file_data['HoldingDate'])
#             print('holding date type: ', type(file_data['HoldingDate']))
#             file_data['HoldingDate'] = pd.to_datetime(file_data['HoldingDate'])

#         elif table_name == 'unify':
#             print('under unify block')

#             file_data['Capital Invested'] = file_data['Capital Invested'].str.replace(',', '')
#             file_data['Capital Withdrwal'] = file_data['Capital Withdrwal'].str.replace(',', '')
#             file_data['Net Capital'] = file_data['Net Capital'].str.replace(',', '')
#             file_data['Assets'] = file_data['Assets'].str.replace(',', '')
#             file_data['TWRR'] = file_data['TWRR'].str.rstrip('%')
#             file_data['IRR'] = file_data['IRR'].str.rstrip('%')

#             # print('file_data --------->', file_data)

#             file_data = file_data.astype({
#             'Name': str,
#             'Strategy': str,
#             # 'Inception': 'Inception',
#             'Capital Invested': int,
#             'Capital Withdrwal': int,
#             'Net Capital': int,
#             'Assets': int,
#             'TWRR': float,
#             'IRR': float
#             })

#             file_data.rename(columns={
#             'Name': 'Name',
#             'Strategy': 'Strategy',
#             'Inception': 'Inception',
#             'Capital Invested': 'Capital_Invested',
#             'Capital Withdrwal': 'Capital_Withdrwal',
#             'Net Capital': 'Net_Capital',
#             'Assets': 'Assets',
#             'TWRR': 'TWRR',
#             'IRR': 'IRR'
#             }, inplace=True)

#         elif table_name == 'fixed_deposit':
#             print('under fixed_deposit block')

#             file_data.drop(columns=['Sr.No'], inplace=True)

#             file_data['Interest Start Date'] = pd.to_datetime(file_data['Interest Start Date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

#             # print('file_data --------->', file_data)

#             file_data = file_data.astype({
#             'Depositt ID': int,
#             'Customer ID': int,
#             # 'Interest Start Date': int,
#             'Application No': int,
#             'Customer Name': str,
#             'PAN': str,
#             'Rate': float,
#             'Month': int,
#             'Amount': int,
#             'Interest Amount': int,
#             'Maturity Amount': int
#             })

#             file_data.rename(columns={
#             'Depositt ID': 'Depositt_ID',
#             'Customer ID': 'Customer_ID',
#             'Interest Start Date': 'Interest_Start_Date',
#             'Application No': 'Application_No',
#             'Customer Name': 'Customer_Name',
#             'PAN': 'PAN',
#             'Rate': 'Rate',
#             'Month': 'Month',
#             'Amount': 'Amount',
#             'Interest Amount': 'Interest_Amount',
#             'Maturity Amount': 'Maturity_Amount'
#             }, inplace=True)


#         elif table_name == 'vested':
#             print('under vested block')

#             file_data['Account Created On'] = pd.to_datetime(file_data['Account Created On'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
#             file_data['KYC Approved On'] = pd.to_datetime(file_data['KYC Approved On'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
#             file_data['First Funded On'] = pd.to_datetime(file_data['First Funded On'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
#             file_data['Last Login'] = pd.to_datetime(file_data['Last Login'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')


#             file_data.rename(columns={
#             'Vested User ID': 'Vested_User_ID',
#             'Client Name': 'Client_Name',
#             'Email': 'Email',
#             'Phone Number': 'Phone_Number',
#             'Account Created On': 'Account_Created_On',
#             'KYC Approved On': 'KYC_Approved_On',
#             'First Funded On': 'First_Funded_On',
#             'Last Login': 'Last_Login',
#             'Equity Value (USD)': 'Equity_Value__USD_', 
#             'Cash Value (USD)': 'Cash_Value__USD_', 
#             'Unrealized P&L': 'Unrealized_P_L',
#             'Pricing Tier': 'Pricing_Tier'
#             }, inplace=True)
        

#         # if table_name == 'strata':
#         #     pass

#         # column_data_types = file_data.dtypes
#         # print('file data types')
#         # print(column_data_types)

#         # for index, row in file_data.iterrows():
#         #     if index >= 10:  # Check if we've printed 10 rows already
#         #         break 
#         #     for column in file_data.columns:
#         #         cell_value = row[column]
#         #         cell_data_type = type(cell_value)
#         #         print(f"Row: {index}, Column: {column}, Data Type: {cell_data_type}, Value: {cell_value}")

#         # file_data['created_at'] = current_time
#         # file_data['updated_at'] = current_time
#         # print('roshan')
#         # print(file_data)

#         print('current time: ', current_time)
#         print('type: ', type(current_time))

#         # current_date = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")

#         # print('Now Current date: ', current_date)

#         pandas_gbq.to_gbq(file_data, 'winrich_dev_v2.' + table_name, project_id='elegant-tendril-399501', if_exists='append', location='US')
#         # pandas_gbq.to_gbq(file_data, 'temp.' + table_name, project_id='elegant-tendril-399501', if_exists='append', location='asia-south1')
#         print(f"Data transfer successful for file: {file_name}")

#         execute_sql_queries(table_name)

#         print('Mapping done successfully')

#         metadata = {'Event_ID': context.event_id, 'Event_type': context.event_type, 'Bucket_name': event['bucket'], 'File_name': file_name, 'created_at': (current_time), 'updated_at': (current_time), 'status_flag': 1, 'status': 'success', 'failure_reason': None}
#         print('meta data: ', metadata)
#         # print('meta data data type: ', type(metadata))
#         metadata_df = pd.DataFrame.from_records([metadata])

#         # print('metadata_df')

#         # print(metadata_df)

#         print("Appending metadata to the metadata table")
#         pandas_gbq.to_gbq(metadata_df, 'winrich_dev_v2.gcs_bq_data_transfer_status_tracker', project_id='elegant-tendril-399501', if_exists='append', location='US')
#         print("Metadata appended successfully")

#     except Exception as e:
#         print(f"An error occurred while processing file: {file_name}. Error: {str(e)}")
#         print('**************************')
#         print(repr(e))

#         metadata_failure = {'Event_ID': context.event_id, 'Event_type': context.event_type, 'Bucket_name': event['bucket'], 'File_name': file_name, 'created_at': current_time, 'updated_at': current_time, 'status_flag': 0, 'status': 'fail', 'failure_reason': str(e) + '\n' + repr(e)}
#         metadata_failure_df = pd.DataFrame.from_records([metadata_failure])

#         print("Appending failure metadata to the metadata table")
#         pandas_gbq.to_gbq(metadata_failure_df, 'winrich_dev_v2.gcs_bq_data_transfer_status_tracker', project_id='elegant-tendril-399501', if_exists='append', location='US')
#         print("Failure metadata appended successfully")


# ************************************************************************************( Version-2 )******************************************************************************************************

# import pandas as pd
# import pandas_gbq
# from google.cloud import bigquery
# from google.cloud import storage
# # from datetime import datetime
# import datetime
# import pytz
# import os
# import numpy as np

# def download_file_from_gcs(bucket_name, source_blob_name, destination_file_name):
#     """Downloads a file from Google Cloud Storage."""
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(source_blob_name)
#     blob.download_to_filename(destination_file_name)
#     print(f'File downloaded from GCS: {source_blob_name}')

# def execute_sql_queries(table_name):
#     client = bigquery.Client()

#     if(table_name == 'liquiloans_master'):

#         column_name = 'name'

#     elif(table_name == 'mutualfunds_master' or table_name == 'equity_master'):

#         column_name = 'h_name'

#     elif(table_name == 'sip_master'):

#         column_name = 'c_name'

#     elif(table_name == 'vested'):

#         column_name = 'Client_Name'

#     elif(table_name == 'unify'):

#         column_name = 'Name'

#     elif(table_name == 'icici_pms'):

#         column_name = 'clientname'

#     elif(table_name == 'hbits'):

#         column_name = 'name_of_the_investor'

#     elif(table_name == 'insurance_icici'):

#         column_name = 'Customer_Full_Name'

#     elif(table_name == 'insurance_max_bupa'):

#         column_name = 'Full_Name'

#     elif(table_name == 'fixed_deposit'):

#         column_name = 'Customer_Name'

#     # First SQL query
#     query1 = f"""
#         UPDATE `elegant-tendril-399501.winrich_dev_v2.{table_name}` AS fd
#         SET master_customer_id = (
#             SELECT mcd.master_customer_id
#             FROM `elegant-tendril-399501.winrich_dev_v2.master_customers_data` AS mcd
#             WHERE LOWER(REPLACE(TRIM(fd.{column_name}), ' ', '')) = LOWER(REPLACE(TRIM(mcd.master_username), ' ', '')) 
#         )
#         WHERE fd.master_customer_id is null and Date(fd.created_at) = CURRENT_DATE();
#         """
#     # Second SQL query
#     query2 = f"""
#     UPDATE `elegant-tendril-399501.winrich_dev_v2.{table_name}` AS fd
#     SET master_customer_id = (
#         SELECT ons.master_customer_id
#         FROM `elegant-tendril-399501.winrich_dev_v2.other_names` AS ons
#         WHERE LOWER(REPLACE(TRIM(fd.{column_name}), ' ', '')) = LOWER(REPLACE(TRIM(ons.other_names), ' ', '')) 
#     )
#     WHERE fd.master_customer_id is null and Date(fd.created_at) = CURRENT_DATE();
#     """
    
#     print('Query-2')
#     print(query2)

#     if table_name == 'equity_master':
#         category_update_query_by_condition = f"""
#         UPDATE `elegant-tendril-399501.winrich_dev_v2.equity_master`
#         SET category = CASE
#             WHEN LOWER(symbol) LIKE '%gold%' THEN 'G'
#             WHEN LOWER(symbol) LIKE '%sgb%' OR (REGEXP_CONTAINS(LOWER(symbol), '[a-z]') AND REGEXP_CONTAINS(LOWER(symbol), '[0-9]')) THEN 'B'
#             ELSE 'E'
#         END
#         WHERE TRUE
#         """
#         # Execute the category_update_by_condition SQL query
#         query_job_category1 = client.query(category_update_query_by_condition)
#         query_job_category1.result()  # Waits for the query to finish
#         print(category_update_query_by_condition)
#         print("Category based on the conditions updated successfully.")

#         category_update_by_exception_list = f"""
            
#             UPDATE `elegant-tendril-399501.winrich_dev_v2.{table_name}` AS em
#             SET category = (
#                 SELECT excep_list.category
#                 FROM `elegant-tendril-399501.winrich_dev_v2.equity_master_category_exception_list` AS excep_list
#                 WHERE LOWER(REPLACE(TRIM(em.symbol), ' ', '')) = LOWER(REPLACE(TRIM(excep_list.symbol), ' ', ''))
#                 AND excep_list.category IS NOT NULL AND excep_list.category <> ''
#             )
#             WHERE EXISTS (
#                 SELECT 1
#                 FROM `elegant-tendril-399501.winrich_dev_v2.equity_master_category_exception_list` AS excep_list
#                 WHERE LOWER(REPLACE(TRIM(em.symbol), ' ', '')) = LOWER(REPLACE(TRIM(excep_list.symbol), ' ', ''))
#             );

#         """

#         # Execute the category_update_by_exception_list SQL query
#         query_job_category2 = client.query(category_update_by_exception_list)
#         query_job_category2.result()  # Waits for the query to finish
#         print(category_update_by_exception_list)
#         print("Category based on the exception list updated successfully.")

#     # Third SQL query
#     query3 = """
#     DELETE FROM elegant-tendril-399501.winrich_dev_v2.sip_master
#     WHERE c_name = 'nan' AND
#     month1 = 0.0
#     AND month2 = 0.0
#     AND month3 = 0.0
#     AND month4 = 0.0
#     AND month5 = 0.0
#     AND month6 = 0.0
#     AND month7 = 0.0
#     AND month8 = 0.0
#     AND month9 = 0.0
#     AND month10 = 0.0
#     AND month11 = 0.0
#     AND month12 = 0.0;
#     """

#     # Execute the first SQL query
#     query_job1 = client.query(query1)
#     query_job1.result()  # Waits for the query to finish
#     print("First data mapping successful.")

#     # Execute the second SQL query
#     query_job2 = client.query(query2)
#     query_job2.result()  # Waits for the query to finish
#     print("Second data mapping successful.")

#     if(table_name == 'sip_master'):

#         # Execute the Third SQL query
#         query_job3 = client.query(query3)
#         query_job3.result()  # Waits for the query to finish
#         print("Null Rows Deleted From SIP Master")

# def get_table_schema(table_name):
#     print('under table_schema')
#     client = bigquery.Client()
#     dataset_ref = client.dataset('winrich_dev_v2')
#     table_ref = dataset_ref.table(table_name)
#     table = client.get_table(table_ref)

#     print('table: ', table)
    
#     schema = [(field.name, field.field_type) for field in table.schema]

#     print('schema: ', schema)
#     return schema

# def hello_gcs(event, context):
#     client = bigquery.Client()

#     file_name = event['name'].lower()
#     print('file_name: ', file_name)

#     dataset_ref = client.dataset('winrich_dev_v2')
#     tables = client.list_tables(dataset_ref)
#     table_names = [table.table_id for table in tables]

#     # table_name = next((table for table in table_names if table in file_name), file_name.split('.')[0])

#     print('%%%%%%%%%%%%%%%%%%%%%%%%%')
#     # print(table_name)

#     if 'deposit' in file_name:
#         table_name = 'fixed_deposit'
#     elif 'sipmom' in file_name:
#         table_name = 'sip_master'
#     elif 'equity' in file_name or 'stocks' in file_name:
#         table_name = 'equity_master'
#     elif 'liqui' in file_name:
#         table_name = 'liquiloans_master'
#     elif 'mutual' in file_name:
#         table_name = 'mutualfunds_master'
#     elif 'strata' in file_name:
#         table_name = 'strata'
#     elif 'unify' in file_name:
#         table_name = 'unify'
#     elif 'pms' in file_name or 'icici_pms' in file_name:
#         table_name = 'icici_pms'
#     elif 'hbits' in file_name:
#         table_name = 'hbits'
#     elif 'icici' in file_name:
#         table_name = 'insurance_icici'
#     elif 'max_bupa' in file_name:
#         table_name = 'insurance_max_bupa'
#     elif 'vested' in file_name or 'funded' in file_name or 'accounts' in file_name:
#         table_name = 'vested'
#     else:
#         table_name = file_name.split('.')[0]


#     print(f'File: {file_name}, Table: {table_name}')
#     current_time = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")

#     print('current time: ', current_time)
#     print('type: ', type(current_time))

#     try:
#         file_path = f'/tmp/{os.path.dirname(file_name)}'
#         os.makedirs(file_path, exist_ok=True)
#         file_path = f'/tmp/{file_name}'

#         print('file_path:', file_path)
#         download_file_from_gcs(event['bucket'], event['name'], file_path)

#         file_data = pd.read_csv(file_path)
#         print("CSV file loaded successfully")

#         print(f"Processing file: {file_name}")
#         print(f"Associated table: {table_name}")

#         file_data['created_at'] = datetime.datetime.now()
#         file_data['updated_at'] = datetime.datetime.now()

#         # print('time from datetime: ', datetime.datetime.now())

#         print('created_at: ', current_time)

#         table_schema = get_table_schema(table_name)
#         print("Schema for table {}: {}".format(table_name, table_schema))

#         # print('before data types: ', file_data.dtypes)

#         # Strip leading and trailing spaces from column names

#         file_data.columns = file_data.columns.str.strip()

#         if table_name == 'mutualfunds_master':

#             print('inside mutual funds')

#             column_mappings = {
#                 'sCode': 's_code',
#                 'Nature': 'nature',
#                 'Email': 'email',
#                 'Mobile': 'mobile',
#                 'FolioStartDate': 'folio_start_date',
#                 'AvgCost': 'avg_cost',
#                 'InvAmt': 'inv_amt',
#                 'TotalInvAmt': 'total_inv_amt',
#                 'CurNAV': 'cur_nav',
#                 'CurValue': 'cur_value',
#                 'DivAmt': 'div_amt',
#                 'NotionalGain': 'notional_gain',
#                 'ActualGain': 'actual_gain',
#                 'FolioXIRR': 'folio_xirr',
#                 'NatureXIRR': 'nature_xirr',
#                 'ClientXIRR': 'client_xirr',
#                 'NatureAbs': 'nature_abs',
#                 'ClientAbs': 'client_abs',
#                 'absReturn': 'abs_return',
#                 'BalUnit': 'bal_unit',
#                 'ValueDate': 'value_date',
#                 'ReportDate': 'report_date'
#             }

#             # Rename columns using the dictionary
#             file_data.rename(columns=column_mappings, inplace=True)


#             column_data_types = file_data.dtypes

#             print('after data types: ', column_data_types)
#             print('file data types')
#             print(column_data_types)

#             # Define column mappings
#             # column_mappings = {'sCode': 's_code'}

#             # Rename columns
#             # file_data.rename(columns=column_mappings, inplace=True)

#             # List of columns to be converted to string data type
#             str_columns = ['h_name', 'c_name', 's_code', 's_name', 'foliono', 'nature', 'folio_start_date', 'bal_unit' , 'email', 'mobile', 'value_date', 'report_date']

#             # List of columns to be converted to float data type
#             float_columns = ['avg_cost', 'inv_amt', 'total_inv_amt', 'cur_nav', 'cur_value', 'div_amt', 'notional_gain', 'actual_gain', 'folio_xirr', 'nature_xirr', 'client_xirr', 'nature_abs', 'client_abs', 'abs_return']

#             # Convert columns to string data type
#             file_data[str_columns] = file_data[str_columns].astype(str)

#             # Convert columns to float data type
#             file_data[float_columns] = file_data[float_columns].astype(float)

#             # print('file data columns: ', file_data)

#             # Convert 'FolioStartDate', 'ValueDate', and 'ReportDate' columns to datetime data type
#             # date_columns = ['created_at', 'updated_at']
#             # file_data[date_columns] = file_data[date_columns].apply(pd.to_datetime)


#         elif(table_name == 'liquiloans_master'):

#             print('Inside the liquiloans block')

#             # # Strip leading and trailing spaces from column names

#             # file_data.columns = file_data.columns.str.strip()

#             file_data['investori'] = file_data['investori'].astype(int)

#             print('file data name: ', file_data['name'])

#             file_data.rename(columns={'annualized_return': 'annualized_return_'}, inplace=True)

#             # file_data['name'] = file_data['name'].astype(str)

#             file_data['current_value'] = file_data['current_value'].astype(float)

#             file_data['annualized_return_'] = file_data['annualized_return_'].astype(float)


#         elif(table_name == 'sip_master'):

#             columns_to_convert = ['c_name', 'Month1', 'Month2', 'Month3', 'Month4', 'Month5', 'Month6', 'Month7', 'Month8', 'Month9', 'Month10', 'Month11', 'Month12']

#             for column in columns_to_convert:
#                 if column == 'c_name':
#                     file_data[column] = file_data[column].astype(str)
#                 else:
#                     file_data[column] = file_data[column].astype(float)

#         elif(table_name == 'icici_pms'):

#             print('under icici pms')

#             # Find the row index where the actual data starts (assuming it starts after a specific header row)
#             start_index = file_data[file_data.iloc[:, 0] == 'CLIENTCODE'].index[0]

#             # Remove the unwanted header rows
#             file_data = file_data.iloc[start_index:]

#             # Reset the column headers
#             file_data.columns = file_data.iloc[0]
#             file_data = file_data[1:]

#             # Keep only the relevant columns
#             columns_to_keep = ['CLIENTCODE', 'CLIENTNAME', 'AUM', 'PRODUCTCODE', 'PAN']
#             file_data = file_data[columns_to_keep]

#             # Reset the index
#             file_data.reset_index(drop=True, inplace=True)

#             file_data['created_at'] = datetime.datetime.now()
#             file_data['updated_at'] = datetime.datetime.now()

#             # Print the cleaned DataFrame (for debugging)
#             print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
#             print(file_data.head())

#             file_data.rename(columns={
#             'CLIENTCODE': 'clientcode',
#             'CLIENTNAME': 'clientname',
#             'AUM': 'aum',
#             'PRODUCTCODE': 'productcode',
#             'PAN': 'pan'
#             }, inplace=True)

#             file_data = file_data.astype({
#             'clientcode': str,
#             'clientname': str,
#             'aum': float,
#             'productcode': str,
#             'pan': str
#             })

#         elif(table_name == 'hbits'):

#             file_data.rename(columns={
#             'Name of the Investor': 'name_of_the_investor',
#             "Father's Name": 'fathers_name',
#             'Investment Amount': 'investment_amount',
#             'SPV': 'spv',
#             'Investment Amount LMS': 'investment_amount_lms',
#             'Property Name': 'property_name'
#             }, inplace=True)

#             file_data = file_data.astype({
#                 'name_of_the_investor': str,
#                 'fathers_name': str,
#                 'investment_amount': int,
#                 'spv': str,
#                 'investment_amount_lms': int,
#                 'property_name': str
#             })

#         elif(table_name == 'insurance_icici'):

#             file_data.rename(columns={
#             'Policy No': 'Policy_No',
#             'Customer Full Name': 'Customer_Full_Name',
#             'Product Name': 'Product_Name',
#             'Mobile No': 'Mobile_No',
#             'Due Date': 'Due_Date',
#             'Risk Com Date': 'Risk_Com_Date',
#             'Issuance Date': 'Issuance_Date',
#             'Premium Paying Term': 'Premium_Paying_Term',
#             'Premium Amount': 'Premium_Amount',
#             'Sum Assured': 'Sum_Assured',
#             'Bill Channel': 'Bill_Channel',
#             'Suspense Account Balance': 'Suspense_Account_Balance',
#             'Net Amount Due': 'Net_Amount_Due',
#             'City': 'City____________',
#             'Phone1': 'Phone1________',
#             'Phone2': 'Phone2_______',
#             'Email': 'Email',
#             'Payment Frequency': 'Payment_Frequency'
#             }, inplace=True)

#             # converting the Due_Date and Issuance_Date to be in date time ( timestamp ) type

#             file_data['Due_Date'] = pd.to_datetime(file_data['Due_Date'])

#             file_data['Issuance_Date'] = pd.to_datetime(file_data['Issuance_Date'])

#             file_data['Risk_Com_Date'] = pd.to_datetime(file_data['Risk_Com_Date'], format='%d-%b-%Y').dt.strftime('%Y-%m-%d')

#             # List of columns to be converted to string data type
#             str_columns = ['Policy_No', 'Customer_Full_Name', 'Product_Name', 'Bill_Channel', 'City____________', 'Phone1________', 'Phone2_______', 'Email' , 'Payment_Frequency']

#             # List of columns to be converted to int data type
#             int_columns = ['Mobile_No', 'Premium_Paying_Term', 'Premium_Amount', 'Sum_Assured', 'Suspense_Account_Balance', 'Net_Amount_Due']

#             # Convert columns to string data type
#             file_data[str_columns] = file_data[str_columns].astype(str)

#             # Convert columns to int data type
#             file_data[int_columns] = file_data[int_columns].astype(int)

#         elif table_name == 'insurance_max_bupa':

#             print('under max_bupa')

#             # Remove the "First Name" column if it exists in the DataFrame
#             if 'First Name' in file_data.columns:
#                 file_data.drop(columns=['First Name'], inplace=True)

#             if 'Last Name' in file_data.columns:
#                 file_data.drop(columns=['Last Name'], inplace=True)

#             # Define the column mapping
#             column_mapping = {
#                 'Full Name': 'Full_Name',
#                 'Application Number': 'Application_No',
#                 'Previous Policy Number': 'Old_Policy_Number',
#                 'Policy Number': 'New_Policy_Number',
#                 'Customer ID': 'Customer_ID',
#                 'DOB': 'DOB',
#                 'Plan Type': 'Plan_Type',
#                 'Product ID': 'Product_ID',
#                 'Product Genre': 'Product_Genre',
#                 'Insured Lives': 'Insured_Lives',
#                 'Insured Count': 'Insured_Count',
#                 'Renewal Premium': 'Renewal_Premium',
#                 '2 Years Renewal Premium (With Tax)': '_2_Years_Renewal_Premium__With_Tax_',
#                 'Current Status': 'Current_Status_Workstep',
#                 'Issued Premium': 'Issued_Premium',
#                 'Renewal Premium (Without Taxes)': 'Renewal_Premium__Without_Taxes_',
#                 'Loading Premium': 'Loading_Premium',
#                 'Sum Assured': 'Sum_Assured',
#                 'Individual Sum Assured': 'Individual_Sum_Assured',
#                 'Health Assurance Critical Illness/Criticare Sum Assured': 'Health_Assurance_Critical_Illness_Criticare_Sum_Assured',
#                 'Health Assurance Personal Accident/Accident Care Sum Assured': 'Health_Assurance_Personal_Accident_Accident_Care_Sum_Assured',
#                 'Health Assurance Hospital Cash/Hospicash Sum Assured': 'Health_Assurance_Hospital_Cash_Hospicash_Sum_Assured',
#                 'Login Branch': 'Branch',
#                 'Sales Branch': 'Sales_Branch',
#                 'Zone': 'Zone',
#                 'Renewal Channel': 'Renewal_Channel',
#                 'Renewal Sub Channel': 'Renewal_Sub_Channel',
#                 'Renewal Agent Code': 'Renewal_Agent_Code',
#                 'Renewal Agent Name': 'Renewal_Agent_Name',
#                 'Renewal Agent Type': 'Renewal_Agent_Type',
#                 'Pa Code': 'Pa_Code',
#                 'Conversion Date': 'Conversion_Date',
#                 'Agency Manager ID': 'Agency_Manager_ID',
#                 'Agency Manager Name': 'Agency_Manager_Name',
#                 'Renewal Logged Date': 'Renewal_Logged_Date',
#                 'Renewal Logged Month': 'Renewal_Logged_Month',
#                 'Renewal Issued Date': 'Renewal_Issued_Date',
#                 'Renewal Issued Month': 'Renewal_Issued_Month',
#                 'Maximus Status': 'Maximus_Status',
#                 'Lead Status': 'Lead_Status',
#                 'Sales Status': 'Sales_Status',
#                 'Hums Status': 'Hums_Status',
#                 'Hums Status Update Date': 'Hums_Status_Update_Date',
#                 'Current Team': 'Current_Team',
#                 'Current Status Ageing': 'Current_Status_Ageing',
#                 'Login Ageing': 'Login_Ageing',
#                 'Designation': 'Designation',
#                 'Policy Start Date': 'Policy_Start_Date',
#                 'Policy Expiry Date': 'Policy_Expiry_Date',
#                 'Is Portability': 'Is_Portability',
#                 'Is Split': 'Split_Flag',
#                 'Is Upsell': 'Upsell_Eligibility',
#                 'Upsell Limit': 'Upsell_Limit',
#                 'Plan Name': 'Plan_Name',
#                 'Renew Now': 'Renew_Now',
#                 'Whatsapp Communication for Policy Information': 'Whatsapp_Communication_for_Policy_Information',
#                 'Communication Acknowledgement(Over Ride DND)': 'Communication_Acknowledgement_Over_Ride_DND_',
#                 'Safe Guard': 'Safeguard_Rider_Taken',
#                 'Policy Tenure': 'Policy_Tenure',
#                 'Product Name': 'Product_Name'
#             }

#             # Filter the column mapping to include only columns present in the DataFrame
#             filtered_column_mapping = {k: v for k, v in column_mapping.items() if k in file_data.columns}

#             # Rename the columns
#             file_data.rename(columns=filtered_column_mapping, inplace=True)

#             # List of columns to be converted to int data type
#             int_columns = [
#                 "Application_No","Old_Policy_Number","Customer_ID","Product_ID","Insured_Lives","Renewal_Premium","_2_Years_Renewal_Premium__With_Tax_","Loading_Premium","Sum_Assured","Individual_Sum_Assured","Health_Assurance_Personal_Accident_Accident_Care_Sum_Assured","Health_Assurance_Hospital_Cash_Hospicash_Sum_Assured","Whatsapp_Communication_for_Policy_Information"
#             ]

#             # Filter the list to include only columns present in the DataFrame
#             existing_int_columns = [col for col in int_columns if col in file_data.columns]

#             # Replace non-finite values with 0
#             file_data[existing_int_columns] = file_data[existing_int_columns].replace([np.inf, -np.inf, np.nan], 0)


#             # List of columns to be converted to string data type
#             str_columns = [
#                 "Full_Name",
#                 "New_Policy_Number",
#                 "Plan_Type",
#                 "Product_Genre",
#                 "Insured_Count",
#                 "Current_Status_Workstep",
#                 "Issued_Premium",
#                 "Renewal_Premium__Without_Taxes_",
#                 "Branch",
#                 "Sales_Branch",
#                 "Zone",
#                 "Renewal_Channel",
#                 "Renewal_Sub_Channel",
#                 "Renewal_Agent_Code",
#                 "Renewal_Agent_Name",
#                 "Renewal_Agent_Type",
#                 "Pa_Code",
#                 "Conversion_Date",
#                 "Agency_Manager_ID",
#                 "Agency_Manager_Name",
#                 "Renewal_Logged_Date",
#                 "Renewal_Logged_Month",
#                 "Renewal_Issued_Date",
#                 "Renewal_Issued_Month",
#                 "Maximus_Status",
#                 "Lead_Status",
#                 "Sales_Status",
#                 "Hums_Status",
#                 "Hums_Status_Update_Date",
#                 "Current_Team",
#                 "Current_Status_Ageing",
#                 "Login_Ageing",
#                 "Designation",
#                 "Is_Portability",
#                 "Upsell_Eligibility",
#                 "Upsell_Limit",
#                 "Plan_Name",
#                 "Renew_Now",
#                 "Policy_Tenure",
#                 "Product_Name"
#             ]		

#             # Filter the list to include only columns present in the DataFrame
#             existing_str_columns = [col for col in str_columns if col in file_data.columns]				

#             # List of columns to be converted to bool data type
#             bool_columns = [
#                 'Communication_Acknowledgement_Over_Ride_DND_', 'Safeguard_Rider_Taken', 'Health_Assurance_Critical_Illness_Criticare_Sum_Assured', 'Split_Flag'
#             ]

#             # Filter the list to include only columns present in the DataFrame
#             existing_bool_columns = [col for col in bool_columns if col in file_data.columns]

#             # Convert columns to string data type
#             file_data[existing_str_columns] = file_data[existing_str_columns].astype(str)

#             # Convert columns to int data type
#             file_data[existing_int_columns] = file_data[existing_int_columns].astype(int)

#             # Convert columns to bool data type
#             file_data[existing_bool_columns] = file_data[existing_bool_columns].astype(bool)

#             file_data['Policy_Start_Date'] = pd.to_datetime(file_data['Policy_Start_Date'])
#             file_data['Policy_Expiry_Date'] = pd.to_datetime(file_data['Policy_Expiry_Date'])

#         elif table_name == 'equity_master':
#             file_data = file_data.astype({
#                 'c_name': str,
#                 'h_name': str,
#                 'ClientCode': str,
#                 'ScripCode': int,
#                 'Symbol': str,
#                 'PoolHoldings': float,
#                 'PledgeHoldings': float,
#                 'DPAccountHoldings': float,
#                 'NetHoldings': float,
#                 'TotalValue': float,
#                 'Bluechip': float,
#                 'Good': float,
#                 'Average': float,
#                 'Poor': float,
#                 'ScripName': str
#             })
#             print('holdingdate: ', file_data['HoldingDate'])
#             print('holding date type: ', type(file_data['HoldingDate']))
#             file_data['HoldingDate'] = pd.to_datetime(file_data['HoldingDate'])

#         elif table_name == 'unify':
#             print('under unify block')

#             file_data['Capital Invested'] = file_data['Capital Invested'].str.replace(',', '')
#             file_data['Capital Withdrwal'] = file_data['Capital Withdrwal'].str.replace(',', '')
#             file_data['Net Capital'] = file_data['Net Capital'].str.replace(',', '')
#             file_data['Assets'] = file_data['Assets'].str.replace(',', '')
#             file_data['TWRR'] = file_data['TWRR'].str.rstrip('%')
#             file_data['IRR'] = file_data['IRR'].str.rstrip('%')

#             # print('file_data --------->', file_data)

#             file_data = file_data.astype({
#             'Name': str,
#             'Strategy': str,
#             # 'Inception': 'Inception',
#             'Capital Invested': int,
#             'Capital Withdrwal': int,
#             'Net Capital': int,
#             'Assets': int,
#             'TWRR': float,
#             'IRR': float
#             })

#             file_data.rename(columns={
#             'Name': 'Name',
#             'Strategy': 'Strategy',
#             'Inception': 'Inception',
#             'Capital Invested': 'Capital_Invested',
#             'Capital Withdrwal': 'Capital_Withdrwal',
#             'Net Capital': 'Net_Capital',
#             'Assets': 'Assets',
#             'TWRR': 'TWRR',
#             'IRR': 'IRR'
#             }, inplace=True)

#         elif table_name == 'fixed_deposit':
#             print('under fixed_deposit block')

#             file_data.drop(columns=['Sr.No'], inplace=True)

#             file_data['Interest Start Date'] = pd.to_datetime(file_data['Interest Start Date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

#             # print('file_data --------->', file_data)

#             file_data = file_data.astype({
#             'Depositt ID': int,
#             'Customer ID': int,
#             # 'Interest Start Date': int,
#             'Application No': int,
#             'Customer Name': str,
#             'PAN': str,
#             'Rate': float,
#             'Month': int,
#             'Amount': int,
#             'Interest Amount': int,
#             'Maturity Amount': int
#             })

#             file_data.rename(columns={
#             'Depositt ID': 'Depositt_ID',
#             'Customer ID': 'Customer_ID',
#             'Interest Start Date': 'Interest_Start_Date',
#             'Application No': 'Application_No',
#             'Customer Name': 'Customer_Name',
#             'PAN': 'PAN',
#             'Rate': 'Rate',
#             'Month': 'Month',
#             'Amount': 'Amount',
#             'Interest Amount': 'Interest_Amount',
#             'Maturity Amount': 'Maturity_Amount'
#             }, inplace=True)


#         elif table_name == 'vested':
#             print('under vested block')

#             file_data['Account Created On'] = pd.to_datetime(file_data['Account Created On'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
#             file_data['KYC Approved On'] = pd.to_datetime(file_data['KYC Approved On'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
#             file_data['First Funded On'] = pd.to_datetime(file_data['First Funded On'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
#             file_data['Last Login'] = pd.to_datetime(file_data['Last Login'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')


#             file_data.rename(columns={
#             'Vested User ID': 'Vested_User_ID',
#             'Client Name': 'Client_Name',
#             'Email': 'Email',
#             'Phone Number': 'Phone_Number',
#             'Account Created On': 'Account_Created_On',
#             'KYC Approved On': 'KYC_Approved_On',
#             'First Funded On': 'First_Funded_On',
#             'Last Login': 'Last_Login',
#             'Equity Value (USD)': 'Equity_Value__USD_', 
#             'Cash Value (USD)': 'Cash_Value__USD_', 
#             'Unrealized P&L': 'Unrealized_P_L',
#             'Pricing Tier': 'Pricing_Tier'
#             }, inplace=True)
        

#         # if table_name == 'strata':
#         #     pass

#         # column_data_types = file_data.dtypes
#         # print('file data types')
#         # print(column_data_types)

#         # for index, row in file_data.iterrows():
#         #     if index >= 10:  # Check if we've printed 10 rows already
#         #         break 
#         #     for column in file_data.columns:
#         #         cell_value = row[column]
#         #         cell_data_type = type(cell_value)
#         #         print(f"Row: {index}, Column: {column}, Data Type: {cell_data_type}, Value: {cell_value}")

#         # file_data['created_at'] = current_time
#         # file_data['updated_at'] = current_time
#         # print('roshan')
#         # print(file_data)

#         print('current time: ', current_time)
#         print('type: ', type(current_time))

#         # current_date = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")

#         # print('Now Current date: ', current_date)

#         pandas_gbq.to_gbq(file_data, 'winrich_dev_v2.' + table_name, project_id='elegant-tendril-399501', if_exists='append', location='US')
#         # pandas_gbq.to_gbq(file_data, 'temp.' + table_name, project_id='elegant-tendril-399501', if_exists='append', location='asia-south1')
#         print(f"Data transfer successful for file: {file_name}")

#         execute_sql_queries(table_name)

#         print('Mapping done successfully')

#         metadata = {'Event_ID': context.event_id, 'Event_type': context.event_type, 'Bucket_name': event['bucket'], 'File_name': file_name, 'created_at': (current_time), 'updated_at': (current_time), 'status_flag': 1, 'status': 'success', 'failure_reason': None}
#         print('meta data: ', metadata)
#         # print('meta data data type: ', type(metadata))
#         metadata_df = pd.DataFrame.from_records([metadata])

#         # print('metadata_df')

#         # print(metadata_df)

#         print("Appending metadata to the metadata table")
#         pandas_gbq.to_gbq(metadata_df, 'winrich_dev_v2.gcs_bq_data_transfer_status_tracker', project_id='elegant-tendril-399501', if_exists='append', location='US')
#         print("Metadata appended successfully")

#     except Exception as e:
#         print(f"An error occurred while processing file: {file_name}. Error: {str(e)}")
#         print('**************************')
#         print(repr(e))

#         metadata_failure = {'Event_ID': context.event_id, 'Event_type': context.event_type, 'Bucket_name': event['bucket'], 'File_name': file_name, 'created_at': current_time, 'updated_at': current_time, 'status_flag': 0, 'status': 'fail', 'failure_reason': str(e) + '\n' + repr(e)}
#         metadata_failure_df = pd.DataFrame.from_records([metadata_failure])

#         print("Appending failure metadata to the metadata table")
#         pandas_gbq.to_gbq(metadata_failure_df, 'winrich_dev_v2.gcs_bq_data_transfer_status_tracker', project_id='elegant-tendril-399501', if_exists='append', location='US')
#         print("Failure metadata appended successfully")




# ***************************************************************************************************(Version-3)****************************************************************************************************



# import pandas as pd
# import pandas_gbq
# from google.cloud import bigquery
# from google.cloud import storage
# # from datetime import datetime
# import datetime
# import pytz
# import os
# import numpy as np

# def download_file_from_gcs(bucket_name, source_blob_name, destination_file_name):
#     """Downloads a file from Google Cloud Storage."""
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(source_blob_name)
#     blob.download_to_filename(destination_file_name)
#     print(f'File downloaded from GCS: {source_blob_name}')

# def execute_sql_queries(table_name):
#     client = bigquery.Client()

#     if(table_name == 'liquiloans_master'):

#         column_name = 'name'

#     elif(table_name == 'mutualfunds_master' or table_name == 'equity_master'):

#         column_name = 'h_name'

#     elif(table_name == 'sip_master'):

#         column_name = 'c_name'

#     elif(table_name == 'bonds'):

#         column_name = 'Name'

#     elif(table_name == 'vested'):

#         column_name = 'Client_Name'

#     elif(table_name == 'unify'):

#         column_name = 'Name'

#     elif(table_name == 'icici_pms'):

#         column_name = 'clientname'

#     elif(table_name == 'hbits'):

#         column_name = 'name_of_the_investor'

#     elif(table_name == 'insurance_icici'):

#         column_name = 'Customer_Full_Name'

#     elif(table_name == 'insurance_max_bupa'):

#         column_name = 'Full_Name'

#     elif(table_name == 'fixed_deposit'):

#         column_name = 'Customer_Name'

#     elif(table_name == 'strata'):

#         column_name = 'name_on_pan'

#     # First SQL query
#     query1 = f"""
#         UPDATE `elegant-tendril-399501.winrich_dev_v2.{table_name}` AS fd
#         SET master_customer_id = (
#             SELECT mcd.master_customer_id
#             FROM `elegant-tendril-399501.winrich_dev_v2.master_customers_data` AS mcd
#             WHERE LOWER(REPLACE(TRIM(fd.{column_name}), ' ', '')) = LOWER(REPLACE(TRIM(mcd.master_username), ' ', '')) 
#         )
#         WHERE fd.master_customer_id is null;
#         """
#     # Second SQL query
#     query2 = f"""
#     UPDATE `elegant-tendril-399501.winrich_dev_v2.{table_name}` AS fd
#     SET master_customer_id = (
#         SELECT ons.master_customer_id
#         FROM `elegant-tendril-399501.winrich_dev_v2.other_names` AS ons
#         WHERE LOWER(REPLACE(TRIM(fd.{column_name}), ' ', '')) = LOWER(REPLACE(TRIM(ons.other_names), ' ', '')) 
#     )
#     WHERE fd.master_customer_id is null;
#     """
    
#     print('Query-2')
#     print(query2)

#     if table_name == 'equity_master':
#         category_update_query_by_condition = f"""
#         UPDATE `elegant-tendril-399501.winrich_dev_v2.equity_master`
#         SET category = CASE
#             WHEN LOWER(symbol) LIKE '%gold%' OR LOWER(symbol) LIKE '%sgb%' THEN 'G'
#             WHEN (REGEXP_CONTAINS(LOWER(symbol), '[a-z]') AND REGEXP_CONTAINS(LOWER(symbol), '[0-9]')) THEN 'B'
#             ELSE 'E'
#         END
#         WHERE TRUE
#         """
#         # Execute the category_update_by_condition SQL query
#         query_job_category1 = client.query(category_update_query_by_condition)
#         query_job_category1.result()  # Waits for the query to finish
#         print(category_update_query_by_condition)
#         print("Category based on the conditions updated successfully.")

#         category_update_by_exception_list = f"""
            
#             UPDATE `elegant-tendril-399501.winrich_dev_v2.{table_name}` AS em
#             SET category = (
#                 SELECT excep_list.category
#                 FROM `elegant-tendril-399501.winrich_dev_v2.equity_master_category_exception_list` AS excep_list
#                 WHERE LOWER(REPLACE(TRIM(em.symbol), ' ', '')) = LOWER(REPLACE(TRIM(excep_list.symbol), ' ', ''))
#                 AND excep_list.category IS NOT NULL AND excep_list.category <> ''
#             )
#             WHERE EXISTS (
#                 SELECT 1
#                 FROM `elegant-tendril-399501.winrich_dev_v2.equity_master_category_exception_list` AS excep_list
#                 WHERE LOWER(REPLACE(TRIM(em.symbol), ' ', '')) = LOWER(REPLACE(TRIM(excep_list.symbol), ' ', ''))
#             );

#         """

#         # Execute the category_update_by_exception_list SQL query
#         query_job_category2 = client.query(category_update_by_exception_list)
#         query_job_category2.result()  # Waits for the query to finish
#         print(category_update_by_exception_list)
#         print("Category based on the exception list updated successfully.")

#     # Third SQL Query
#     query3 = """
#     DELETE FROM elegant-tendril-399501.winrich_dev_v2.sip_master
#     WHERE c_name = 'nan' AND
#     month1 = 0.0
#     AND month2 = 0.0
#     AND month3 = 0.0
#     AND month4 = 0.0
#     AND month5 = 0.0
#     AND month6 = 0.0
#     AND month7 = 0.0
#     AND month8 = 0.0
#     AND month9 = 0.0
#     AND month10 = 0.0
#     AND month11 = 0.0
#     AND month12 = 0.0;
#     """

#     # Fourth SQL Query
#     query4 = """
#     DELETE FROM `elegant-tendril-399501.winrich_dev_v2.icici_pms`
#     where clientcode = 'nan' and clientname = 'nan' and productcode = 'nan' and pan = 'nan';
#     """

#     # Execute the first SQL query
#     query_job1 = client.query(query1)
#     query_job1.result()  # Waits for the query to finish
#     print(query1)
#     print("First data mapping successful.")

#     # Execute the second SQL query
#     query_job2 = client.query(query2)
#     query_job2.result()  # Waits for the query to finish
#     print(query2)
#     print("Second data mapping successful.")

#     if(table_name == 'sip_master'):

#         # Execute the Third SQL query
#         query_job3 = client.query(query3)
#         query_job3.result()  # Waits for the query to finish
#         print("Null Rows Deleted From SIP Master")

#     if(table_name == 'icici_pms'):

#         # Execute the Fourth SQL query
#         query_job4 = client.query(query4)
#         query_job4.result()  # Waits for the query to finish
#         print("Null Rows Deleted From icici_pms")

# def get_table_schema(table_name):
#     print('under table_schema')
#     client = bigquery.Client()
#     dataset_ref = client.dataset('winrich_dev_v2')
#     table_ref = dataset_ref.table(table_name)
#     table = client.get_table(table_ref)

#     print('table: ', table)
    
#     schema = [(field.name, field.field_type) for field in table.schema]

#     print('schema: ', schema)
#     return schema

# def hello_gcs(event, context):
#     client = bigquery.Client()

#     file_name = event['name'].lower()
#     print('file_name: ', file_name)

#     dataset_ref = client.dataset('winrich_dev_v2')
#     tables = client.list_tables(dataset_ref)
#     table_names = [table.table_id for table in tables]

#     # table_name = next((table for table in table_names if table in file_name), file_name.split('.')[0])

#     print('%%%%%%%%%%%%%%%%%%%%%%%%%')
#     # print(table_name)

#     if 'deposit' in file_name:
#         table_name = 'fixed_deposit'
#     elif 'sipmom' in file_name:
#         table_name = 'sip_master'
#     elif 'equity' in file_name or 'stocks' in file_name:
#         table_name = 'equity_master'
#     elif 'golden' in file_name:
#         table_name = 'bonds'
#     elif 'liqui' in file_name:
#         table_name = 'liquiloans_master'
#     elif 'mutual' in file_name:
#         table_name = 'mutualfunds_master'
#     elif 'strata' in file_name:
#         table_name = 'strata'
#     elif 'unify' in file_name:
#         table_name = 'unify'
#     elif 'pms' in file_name or 'icici_pms' in file_name:
#         table_name = 'icici_pms'
#     elif 'hbits' in file_name:
#         table_name = 'hbits'
#     elif 'icici' in file_name:
#         table_name = 'insurance_icici'
#     elif 'max_bupa' in file_name:
#         table_name = 'insurance_max_bupa'
#     elif 'vested' in file_name or 'funded' in file_name or 'accounts' in file_name:
#         table_name = 'vested'
#     else:
#         # table_name = file_name.split('.')[0]
#         print('This function only process all the financial products csv files')
#         return None


#     print(f'File: {file_name}, Table: {table_name}')
#     current_time = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")

#     print('current time: ', current_time)
#     print('type: ', type(current_time))

#     try:
#         file_path = f'/tmp/{os.path.dirname(file_name)}'
#         os.makedirs(file_path, exist_ok=True)
#         file_path = f'/tmp/{file_name}'

#         print('file_path:', file_path)
#         download_file_from_gcs(event['bucket'], event['name'], file_path)

#         file_data = pd.read_csv(file_path)
#         print("CSV file loaded successfully")

#         print(f"Processing file: {file_name}")
#         print(f"Associated table: {table_name}")

#         file_data['created_at'] = datetime.datetime.now()
#         file_data['updated_at'] = datetime.datetime.now()

#         # print('time from datetime: ', datetime.datetime.now())

#         print('created_at: ', current_time)

#         table_schema = get_table_schema(table_name)
#         print("Schema for table {}: {}".format(table_name, table_schema))

#         # print('before data types: ', file_data.dtypes)

#         # Strip leading and trailing spaces from column names

#         file_data.columns = file_data.columns.str.strip()

#         # Checking whether the today's file is already processed or not

#         # Perform a query
#         is_file_already_processed_query = f"""
#                 select * from `elegant-tendril-399501.winrich_dev_v2.{table_name}`
#                 where Date(created_at) = current_date();
#         """
#         query_job = client.query(is_file_already_processed_query)
#         # print(query_job)

#         data_from_corresponding_table = []

#         # Process query results
#         for row in query_job:

#             data_from_corresponding_table.append(row)

#         print(data_from_corresponding_table)

#         if(data_from_corresponding_table and data_from_corresponding_table is not None and len(data_from_corresponding_table) > 0):
#             print('The file: ' + str(file_name) + ' is already processed')
#             return None
#         print('Need to process the data!')

#         if table_name == 'mutualfunds_master':

#             print('inside mutual funds')

#             column_mappings = {
#                 'sCode': 's_code',
#                 'Nature': 'nature',
#                 'Email': 'email',
#                 'Mobile': 'mobile',
#                 'FolioStartDate': 'folio_start_date',
#                 'AvgCost': 'avg_cost',
#                 'InvAmt': 'inv_amt',
#                 'TotalInvAmt': 'total_inv_amt',
#                 'CurNAV': 'cur_nav',
#                 'CurValue': 'cur_value',
#                 'DivAmt': 'div_amt',
#                 'NotionalGain': 'notional_gain',
#                 'ActualGain': 'actual_gain',
#                 'FolioXIRR': 'folio_xirr',
#                 'NatureXIRR': 'nature_xirr',
#                 'ClientXIRR': 'client_xirr',
#                 'NatureAbs': 'nature_abs',
#                 'ClientAbs': 'client_abs',
#                 'absReturn': 'abs_return',
#                 'BalUnit': 'bal_unit',
#                 'ValueDate': 'value_date',
#                 'ReportDate': 'report_date'
#             }

#             # Rename columns using the dictionary
#             file_data.rename(columns=column_mappings, inplace=True)


#             column_data_types = file_data.dtypes

#             print('after data types: ', column_data_types)
#             print('file data types')
#             print(column_data_types)

#             # Define column mappings
#             # column_mappings = {'sCode': 's_code'}

#             # Rename columns
#             # file_data.rename(columns=column_mappings, inplace=True)

#             # List of columns to be converted to string data type
#             str_columns = ['h_name', 'c_name', 's_code', 's_name', 'foliono', 'nature', 'folio_start_date', 'bal_unit' , 'email', 'mobile', 'value_date', 'report_date']

#             # List of columns to be converted to float data type
#             float_columns = ['avg_cost', 'inv_amt', 'total_inv_amt', 'cur_nav', 'cur_value', 'div_amt', 'notional_gain', 'actual_gain', 'folio_xirr', 'nature_xirr', 'client_xirr', 'nature_abs', 'client_abs', 'abs_return']

#             # Convert columns to string data type
#             file_data[str_columns] = file_data[str_columns].astype(str)

#             # Convert columns to float data type
#             file_data[float_columns] = file_data[float_columns].astype(float)

#             # print('file data columns: ', file_data)

#             # Convert 'FolioStartDate', 'ValueDate', and 'ReportDate' columns to datetime data type
#             # date_columns = ['created_at', 'updated_at']
#             # file_data[date_columns] = file_data[date_columns].apply(pd.to_datetime)


#         elif(table_name == 'liquiloans_master'):

#             print('Inside the liquiloans block')

#             # # Strip leading and trailing spaces from column names

#             # file_data.columns = file_data.columns.str.strip()

#             file_data['investori'] = file_data['investori'].astype(int)

#             print('file data name: ', file_data['name'])

#             file_data.rename(columns={'annualized_return': 'annualized_return_'}, inplace=True)

#             file_data['name'] = file_data['name'].astype(str)

#             # Convert current_value column to float, coercing errors
#             file_data['current_value'] = pd.to_numeric(file_data['current_value'], errors='coerce')
            
#             # Optionally, fill NaN values with a default value, e.g., 0.0
#             file_data['current_value'].fillna(0.0, inplace=True)

#             # Convert current_value column to float, coercing errors
#             file_data['annualized_return_'] = pd.to_numeric(file_data['annualized_return_'], errors='coerce')
            
#             # Optionally, fill NaN values with a default value, e.g., 0.0
#             file_data['annualized_return_'].fillna(0.0, inplace=True)


#         elif(table_name == 'sip_master'):

#             columns_to_convert = ['c_name', 'Month1', 'Month2', 'Month3', 'Month4', 'Month5', 'Month6', 'Month7', 'Month8', 'Month9', 'Month10', 'Month11', 'Month12']

#             for column in columns_to_convert:
#                 if column == 'c_name':
#                     file_data[column] = file_data[column].astype(str)
#                 else:
#                     file_data[column] = file_data[column].astype(float)

#         elif(table_name == 'bonds'):

#             # Keep only the relevant columns
#             columns_to_keep = ['Name', 'Email', 'Phone', 'Order Date', 'Issuer Name', 'Coupon', 'Maturity Date', 'Units', 'Price', 'Investment Amount']
#             file_data = file_data[columns_to_keep]

#             file_data.rename(columns={
#                 'Name': 'Name',
#                 'Email': 'Email',
#                 'Phone': 'Phone',
#                 'Order Date': 'Order_Date',
#                 'Issuer Name': 'Issuer_Name',
#                 'Coupon': 'Coupon',
#                 'Maturity Date': 'Maturity_Date',
#                 'Units': 'Units',
#                 'Price': 'Price',
#                 'Investment Amount': 'Investment_Amount',
#             }, inplace=True)

#             # Convert columns to appropriate types
#             file_data['Phone'] = pd.to_numeric(file_data['Phone'], errors='coerce').fillna(0).astype(int)
#             file_data['Units'] = pd.to_numeric(file_data['Units'], errors='coerce').fillna(0).astype(int)
#             file_data['Coupon'] = pd.to_numeric(file_data['Coupon'], errors='coerce').fillna(0.0).astype(float)
#             file_data['Price'] = pd.to_numeric(file_data['Price'], errors='coerce').fillna(0.0).astype(float)
#             file_data['Investment_Amount'] = pd.to_numeric(file_data['Investment_Amount'], errors='coerce').fillna(0.0).astype(float)
#             file_data['Order_Date'] = file_data['Order_Date'].astype(str)
#             file_data['Issuer_Name'] = file_data['Issuer_Name'].astype(str)
#             file_data['Maturity_Date'] = file_data['Maturity_Date'].astype(str)
#             file_data['Name'] = file_data['Name'].astype(str)
#             file_data['Email'] = file_data['Email'].astype(str)

#             file_data['created_at'] = datetime.datetime.now()
#             file_data['updated_at'] = datetime.datetime.now()

#         elif(table_name == 'icici_pms'):

#             print('under icici pms')

#             # Find the row index where the actual data starts (assuming it starts after a specific header row)
#             start_index = file_data[file_data.iloc[:, 0] == 'CLIENTCODE'].index[0]

#             # Remove the unwanted header rows
#             file_data = file_data.iloc[start_index:]

#             # Reset the column headers
#             file_data.columns = file_data.iloc[0]
#             file_data = file_data[1:]

#             # Keep only the relevant columns
#             columns_to_keep = ['CLIENTCODE', 'CLIENTNAME', 'AUM', 'PRODUCTCODE', 'PAN']
#             file_data = file_data[columns_to_keep]

#             # Reset the index
#             file_data.reset_index(drop=True, inplace=True)

#             file_data['created_at'] = datetime.datetime.now()
#             file_data['updated_at'] = datetime.datetime.now()

#             # Print the cleaned DataFrame (for debugging)
#             print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
#             print(file_data.head())

#             file_data.rename(columns={
#             'CLIENTCODE': 'clientcode',
#             'CLIENTNAME': 'clientname',
#             'AUM': 'aum',
#             'PRODUCTCODE': 'productcode',
#             'PAN': 'pan'
#             }, inplace=True)

#             file_data = file_data.astype({
#             'clientcode': str,
#             'clientname': str,
#             'aum': float,
#             'productcode': str,
#             'pan': str
#             })

#         elif(table_name == 'hbits'):

#             file_data.rename(columns={
#             'Name of the Investor': 'name_of_the_investor',
#             "Father's Name": 'fathers_name',
#             'Investment Amount': 'investment_amount',
#             'SPV': 'spv',
#             'Investment Amount LMS': 'investment_amount_lms',
#             'Property Name': 'property_name'
#             }, inplace=True)

#             file_data = file_data.astype({
#                 'name_of_the_investor': str,
#                 'fathers_name': str,
#                 'investment_amount': int,
#                 'spv': str,
#                 'investment_amount_lms': int,
#                 'property_name': str
#             })

#         elif(table_name == 'insurance_icici'):

#             file_data.rename(columns={
#             'Policy No': 'Policy_No',
#             'Customer Full Name': 'Customer_Full_Name',
#             'Product Name': 'Product_Name',
#             'Mobile No': 'Mobile_No',
#             'Due Date': 'Due_Date',
#             'Risk Com Date': 'Risk_Com_Date',
#             'Issuance Date': 'Issuance_Date',
#             'Premium Paying Term': 'Premium_Paying_Term',
#             'Premium Amount': 'Premium_Amount',
#             'Sum Assured': 'Sum_Assured',
#             'Bill Channel': 'Bill_Channel',
#             'Suspense Account Balance': 'Suspense_Account_Balance',
#             'Net Amount Due': 'Net_Amount_Due',
#             'City': 'City____________',
#             'Phone1': 'Phone1________',
#             'Phone2': 'Phone2_______',
#             'Email': 'Email',
#             'Payment Frequency': 'Payment_Frequency'
#             }, inplace=True)

#             # converting the Due_Date and Issuance_Date to be in date time ( timestamp ) type

#             file_data['Due_Date'] = pd.to_datetime(file_data['Due_Date'])

#             file_data['Issuance_Date'] = pd.to_datetime(file_data['Issuance_Date'])

#             file_data['Risk_Com_Date'] = pd.to_datetime(file_data['Risk_Com_Date'], format='%d-%b-%Y').dt.strftime('%Y-%m-%d')

#             # List of columns to be converted to string data type
#             str_columns = ['Policy_No', 'Customer_Full_Name', 'Product_Name', 'Bill_Channel', 'City____________', 'Phone1________', 'Phone2_______', 'Email' , 'Payment_Frequency']

#             # List of columns to be converted to int data type
#             int_columns = ['Mobile_No', 'Premium_Paying_Term', 'Premium_Amount', 'Sum_Assured', 'Suspense_Account_Balance', 'Net_Amount_Due']

#             # Convert columns to string data type
#             file_data[str_columns] = file_data[str_columns].astype(str)

#             # Convert columns to int data type
#             file_data[int_columns] = file_data[int_columns].astype(int)

#         elif table_name == 'insurance_max_bupa':

#             print('under max_bupa')

#             # Remove the "First Name" column if it exists in the DataFrame
#             if 'First Name' in file_data.columns:
#                 file_data.drop(columns=['First Name'], inplace=True)

#             if 'Last Name' in file_data.columns:
#                 file_data.drop(columns=['Last Name'], inplace=True)

#             # Define the column mapping
#             column_mapping = {
#                 'Full Name': 'Full_Name',
#                 'Application Number': 'Application_No',
#                 'Previous Policy Number': 'Old_Policy_Number',
#                 'Policy Number': 'New_Policy_Number',
#                 'Customer ID': 'Customer_ID',
#                 'DOB': 'DOB',
#                 'Plan Type': 'Plan_Type',
#                 'Product ID': 'Product_ID',
#                 'Product Genre': 'Product_Genre',
#                 'Insured Lives': 'Insured_Lives',
#                 'Insured Count': 'Insured_Count',
#                 'Renewal Premium': 'Renewal_Premium',
#                 '2 Years Renewal Premium (With Tax)': '_2_Years_Renewal_Premium__With_Tax_',
#                 'Current Status': 'Current_Status_Workstep',
#                 'Issued Premium': 'Issued_Premium',
#                 'Renewal Premium (Without Taxes)': 'Renewal_Premium__Without_Taxes_',
#                 'Loading Premium': 'Loading_Premium',
#                 'Sum Assured': 'Sum_Assured',
#                 'Individual Sum Assured': 'Individual_Sum_Assured',
#                 'Health Assurance Critical Illness/Criticare Sum Assured': 'Health_Assurance_Critical_Illness_Criticare_Sum_Assured',
#                 'Health Assurance Personal Accident/Accident Care Sum Assured': 'Health_Assurance_Personal_Accident_Accident_Care_Sum_Assured',
#                 'Health Assurance Hospital Cash/Hospicash Sum Assured': 'Health_Assurance_Hospital_Cash_Hospicash_Sum_Assured',
#                 'Login Branch': 'Branch',
#                 'Sales Branch': 'Sales_Branch',
#                 'Zone': 'Zone',
#                 'Renewal Channel': 'Renewal_Channel',
#                 'Renewal Sub Channel': 'Renewal_Sub_Channel',
#                 'Renewal Agent Code': 'Renewal_Agent_Code',
#                 'Renewal Agent Name': 'Renewal_Agent_Name',
#                 'Renewal Agent Type': 'Renewal_Agent_Type',
#                 'Pa Code': 'Pa_Code',
#                 'Conversion Date': 'Conversion_Date',
#                 'Agency Manager ID': 'Agency_Manager_ID',
#                 'Agency Manager Name': 'Agency_Manager_Name',
#                 'Renewal Logged Date': 'Renewal_Logged_Date',
#                 'Renewal Logged Month': 'Renewal_Logged_Month',
#                 'Renewal Issued Date': 'Renewal_Issued_Date',
#                 'Renewal Issued Month': 'Renewal_Issued_Month',
#                 'Maximus Status': 'Maximus_Status',
#                 'Lead Status': 'Lead_Status',
#                 'Sales Status': 'Sales_Status',
#                 'Hums Status': 'Hums_Status',
#                 'Hums Status Update Date': 'Hums_Status_Update_Date',
#                 'Current Team': 'Current_Team',
#                 'Current Status Ageing': 'Current_Status_Ageing',
#                 'Login Ageing': 'Login_Ageing',
#                 'Designation': 'Designation',
#                 'Policy Start Date': 'Policy_Start_Date',
#                 'Policy Expiry Date': 'Policy_Expiry_Date',
#                 'Is Portability': 'Is_Portability',
#                 'Is Split': 'Split_Flag',
#                 'Is Upsell': 'Upsell_Eligibility',
#                 'Upsell Limit': 'Upsell_Limit',
#                 'Plan Name': 'Plan_Name',
#                 'Renew Now': 'Renew_Now',
#                 'Whatsapp Communication for Policy Information': 'Whatsapp_Communication_for_Policy_Information',
#                 'Communication Acknowledgement(Over Ride DND)': 'Communication_Acknowledgement_Over_Ride_DND_',
#                 'Safe Guard': 'Safeguard_Rider_Taken',
#                 'Policy Tenure': 'Policy_Tenure',
#                 'Product Name': 'Product_Name'
#             }

#             # Filter the column mapping to include only columns present in the DataFrame
#             filtered_column_mapping = {k: v for k, v in column_mapping.items() if k in file_data.columns}

#             # Rename the columns
#             file_data.rename(columns=filtered_column_mapping, inplace=True)

#             # List of columns to be converted to int data type
#             int_columns = [
#                 "Application_No","Old_Policy_Number","Customer_ID","Product_ID","Insured_Lives","Renewal_Premium","_2_Years_Renewal_Premium__With_Tax_","Loading_Premium","Sum_Assured","Individual_Sum_Assured","Health_Assurance_Personal_Accident_Accident_Care_Sum_Assured","Health_Assurance_Hospital_Cash_Hospicash_Sum_Assured","Whatsapp_Communication_for_Policy_Information"
#             ]

#             # Filter the list to include only columns present in the DataFrame
#             existing_int_columns = [col for col in int_columns if col in file_data.columns]

#             # Replace non-finite values with 0
#             file_data[existing_int_columns] = file_data[existing_int_columns].replace([np.inf, -np.inf, np.nan], 0)


#             # List of columns to be converted to string data type
#             str_columns = [
#                 "Full_Name",
#                 "New_Policy_Number",
#                 "Plan_Type",
#                 "Product_Genre",
#                 "Insured_Count",
#                 "Current_Status_Workstep",
#                 "Issued_Premium",
#                 "Renewal_Premium__Without_Taxes_",
#                 "Branch",
#                 "Sales_Branch",
#                 "Zone",
#                 "Renewal_Channel",
#                 "Renewal_Sub_Channel",
#                 "Renewal_Agent_Code",
#                 "Renewal_Agent_Name",
#                 "Renewal_Agent_Type",
#                 "Pa_Code",
#                 "Conversion_Date",
#                 "Agency_Manager_ID",
#                 "Agency_Manager_Name",
#                 "Renewal_Logged_Date",
#                 "Renewal_Logged_Month",
#                 "Renewal_Issued_Date",
#                 "Renewal_Issued_Month",
#                 "Maximus_Status",
#                 "Lead_Status",
#                 "Sales_Status",
#                 "Hums_Status",
#                 "Hums_Status_Update_Date",
#                 "Current_Team",
#                 "Current_Status_Ageing",
#                 "Login_Ageing",
#                 "Designation",
#                 "Is_Portability",
#                 "Upsell_Eligibility",
#                 "Upsell_Limit",
#                 "Plan_Name",
#                 "Renew_Now",
#                 "Policy_Tenure",
#                 "Product_Name"
#             ]		

#             # Filter the list to include only columns present in the DataFrame
#             existing_str_columns = [col for col in str_columns if col in file_data.columns]				

#             # List of columns to be converted to bool data type
#             bool_columns = [
#                 'Communication_Acknowledgement_Over_Ride_DND_', 'Safeguard_Rider_Taken', 'Health_Assurance_Critical_Illness_Criticare_Sum_Assured', 'Split_Flag'
#             ]

#             # Filter the list to include only columns present in the DataFrame
#             existing_bool_columns = [col for col in bool_columns if col in file_data.columns]

#             # Convert columns to string data type
#             file_data[existing_str_columns] = file_data[existing_str_columns].astype(str)

#             # Convert columns to int data type
#             file_data[existing_int_columns] = file_data[existing_int_columns].astype(int)

#             # Convert columns to bool data type
#             file_data[existing_bool_columns] = file_data[existing_bool_columns].astype(bool)

#             file_data['Policy_Start_Date'] = pd.to_datetime(file_data['Policy_Start_Date'])
#             file_data['Policy_Expiry_Date'] = pd.to_datetime(file_data['Policy_Expiry_Date'])

#         elif table_name == 'equity_master':

#             # Filter out rows where 'Client Name' is empty
#             file_data = file_data[file_data['Client Name'].notna()]
            
#             # Print skipped rows
#             skipped_rows = file_data[file_data['Client Name'].isna()]
#             for index, row in skipped_rows.iterrows():
#                 print(f"Skipping row {index} due to missing 'Client Name': {row}")

#             file_data = file_data.astype({
#                 'Client Code': str,
#                 'Client Name': str,
#                 'Scrip Code': str,
#                 'Symbol': str,
#                 'Pool Holdings': float,
#                 'Pledge Holdings': float,
#                 'DP Account Holdings': float,
#                 'Net Holdings': float,
#                 'Total Value (?)': float,
#                 'Bluechip (?)': float,
#                 'Good (?)': float,
#                 'Average (?)': float,
#                 'Poor (?)': float
#             })

#             # Copy 'Client Name' to 'c_name'
#             file_data['c_name'] = file_data['Client Name']

#             file_data.rename(columns={
#             'Client Code': 'clientcode',
#             'Client Name': 'h_name',
#             'Scrip Code': 'scripcode',
#             'Symbol': 'symbol',
#             'Pool Holdings': 'poolholdings',
#             'Pledge Holdings': 'pledgeholdings',
#             'DP Account Holdings': 'dpaccountholdings',
#             'Net Holdings': 'netholdings',
#             'Total Value (?)': 'totalvalue',
#             'Bluechip (?)': 'bluechip',
#             'Good (?)': 'good',
#             'Average (?)': 'average',
#             'Poor (?)': 'poor'
#             }, inplace=True)

#         elif table_name == 'unify':
#             print('under unify block')

#             file_data['Capital Invested'] = file_data['Capital Invested'].str.replace(',', '')
#             file_data['Capital Withdrwal'] = file_data['Capital Withdrwal'].str.replace(',', '')
#             file_data['Net Capital'] = file_data['Net Capital'].str.replace(',', '')
#             file_data['Assets'] = file_data['Assets'].str.replace(',', '')
#             file_data['TWRR'] = file_data['TWRR'].str.rstrip('%')
#             file_data['IRR'] = file_data['IRR'].str.rstrip('%')

#             # print('file_data --------->', file_data)

#             file_data = file_data.astype({
#             'Name': str,
#             'Strategy': str,
#             # 'Inception': 'Inception',
#             'Capital Invested': int,
#             'Capital Withdrwal': int,
#             'Net Capital': int,
#             'Assets': int,
#             'TWRR': float,
#             'IRR': float
#             })

#             file_data.rename(columns={
#             'Name': 'Name',
#             'Strategy': 'Strategy',
#             'Inception': 'Inception',
#             'Capital Invested': 'Capital_Invested',
#             'Capital Withdrwal': 'Capital_Withdrwal',
#             'Net Capital': 'Net_Capital',
#             'Assets': 'Assets',
#             'TWRR': 'TWRR',
#             'IRR': 'IRR'
#             }, inplace=True)

#         elif table_name == 'fixed_deposit':
#             print('under fixed_deposit block')

#             file_data.drop(columns=['Sr.No'], inplace=True)

#             file_data['Interest Start Date'] = pd.to_datetime(file_data['Interest Start Date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

#             # print('file_data --------->', file_data)

#             file_data = file_data.astype({
#             'Depositt ID': int,
#             'Customer ID': int,
#             # 'Interest Start Date': int,
#             'Application No': int,
#             'Customer Name': str,
#             'PAN': str,
#             'Rate': float,
#             'Month': int,
#             'Amount': int,
#             'Interest Amount': int,
#             'Maturity Amount': int
#             })

#             file_data.rename(columns={
#             'Depositt ID': 'Depositt_ID',
#             'Customer ID': 'Customer_ID',
#             'Interest Start Date': 'Interest_Start_Date',
#             'Application No': 'Application_No',
#             'Customer Name': 'Customer_Name',
#             'PAN': 'PAN',
#             'Rate': 'Rate',
#             'Month': 'Month',
#             'Amount': 'Amount',
#             'Interest Amount': 'Interest_Amount',
#             'Maturity Amount': 'Maturity_Amount'
#             }, inplace=True)


#         elif table_name == 'vested':
#             print('under vested block')

#             # file_data['Account Created On'] = pd.to_datetime(file_data['Account Created On'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
#             # file_data['KYC Approved On'] = pd.to_datetime(file_data['KYC Approved On'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
#             # file_data['First Funded On'] = pd.to_datetime(file_data['First Funded On'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
#             # file_data['Last Login'] = pd.to_datetime(file_data['Last Login'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')


#             file_data.rename(columns={
#             'Vested User ID': 'Vested_User_ID',
#             'Client Name': 'Client_Name',
#             'Email': 'Email',
#             'Phone Number': 'Phone_Number',
#             'Account Created On': 'Account_Created_On',
#             'KYC Approved On': 'KYC_Approved_On',
#             'First Funded On': 'First_Funded_On',
#             'Last Login': 'Last_Login',
#             'Equity Value (USD)': 'Equity_Value__USD_', 
#             'Cash Value (USD)': 'Cash_Value__USD_', 
#             'Unrealized P&L': 'Unrealized_P_L',
#             'Pricing Tier': 'Pricing_Tier'
#             }, inplace=True)

#             # Convert numeric fields to float if they are represented as strings
#             numeric_fields = ['Equity_Value__USD_', 'Cash_Value__USD_', 'Unrealized_P_L']
#             for field in numeric_fields:
#                 file_data[field] = pd.to_numeric(file_data[field], errors='coerce')
            
#             # Convert dates from various formats to 'YYYY-MM-DD' format
#             date_fields = ['Account_Created_On', 'KYC_Approved_On', 'First_Funded_On', 'Last_Login']
#             for field in date_fields:
#                 file_data[field] = pd.to_datetime(file_data[field], errors='coerce').dt.strftime('%Y-%m-%d')
        

#         elif table_name == 'strata':
#             print('under strata block')

#             file_data.rename(columns={
#             'CP Name': 'CP_Name',
#             'IM Name': 'IM_Name',
#             'name_on_pan': 'name_on_pan',
#             'Amt Deal Value': 'Amt_Deal_Value',
#             'Amt Received': 'Amt_Received',
#             'status_name': 'status_name',
#             'asset_name': 'asset_name'
#             }, inplace=True)

#             # Convert numeric fields to float if they are represented as strings
#             numeric_fields = ['Amt_Deal_Value', 'Amt_Received']
#             for field in numeric_fields:
#                 file_data[field] = pd.to_numeric(file_data[field], errors='coerce')

#         # column_data_types = file_data.dtypes
#         # print('file data types')
#         # print(column_data_types)

#         # for index, row in file_data.iterrows():
#         #     if index >= 10:  # Check if we've printed 10 rows already
#         #         break 
#         #     for column in file_data.columns:
#         #         cell_value = row[column]
#         #         cell_data_type = type(cell_value)
#         #         print(f"Row: {index}, Column: {column}, Data Type: {cell_data_type}, Value: {cell_value}")

#         # file_data['created_at'] = current_time
#         # file_data['updated_at'] = current_time
#         # print('roshan')
#         # print(file_data)

#         print('current time: ', current_time)
#         print('type: ', type(current_time))

#         # current_date = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")

#         # print('Now Current date: ', current_date)

#         pandas_gbq.to_gbq(file_data, 'winrich_dev_v2.' + table_name, project_id='elegant-tendril-399501', if_exists='append', location='US')
#         # pandas_gbq.to_gbq(file_data, 'temp.' + table_name, project_id='elegant-tendril-399501', if_exists='append', location='asia-south1')
#         print(f"Data transfer successful for file: {file_name}")

#         execute_sql_queries(table_name)

#         print('Mapping done successfully')

#         metadata = {'Event_ID': context.event_id, 'Event_type': context.event_type, 'Bucket_name': event['bucket'], 'File_name': file_name, 'created_at': (current_time), 'updated_at': (current_time), 'status_flag': 1, 'status': 'success', 'failure_reason': None}
#         print('meta data: ', metadata)
#         # print('meta data data type: ', type(metadata))
#         metadata_df = pd.DataFrame.from_records([metadata])

#         # print('metadata_df')

#         # print(metadata_df)

#         print("Appending metadata to the metadata table")
#         pandas_gbq.to_gbq(metadata_df, 'winrich_dev_v2.gcs_bq_data_transfer_status_tracker', project_id='elegant-tendril-399501', if_exists='append', location='US')
#         print("Metadata appended successfully")

#     except Exception as e:
#         print(f"An error occurred while processing file: {file_name}. Error: {str(e)}")
#         print('**************************')
#         print(repr(e))

#         metadata_failure = {'Event_ID': context.event_id, 'Event_type': context.event_type, 'Bucket_name': event['bucket'], 'File_name': file_name, 'created_at': current_time, 'updated_at': current_time, 'status_flag': 0, 'status': 'fail', 'failure_reason': str(e) + '\n' + repr(e)}
#         metadata_failure_df = pd.DataFrame.from_records([metadata_failure])

#         print("Appending failure metadata to the metadata table")
#         pandas_gbq.to_gbq(metadata_failure_df, 'winrich_dev_v2.gcs_bq_data_transfer_status_tracker', project_id='elegant-tendril-399501', if_exists='append', location='US')
#         print("Failure metadata appended successfully")


# ***************************************************************************************************(Version-3)****************************************************************************************************


# import pandas as pd
# import pandas_gbq
# from google.cloud import bigquery
# from google.cloud import storage
# # from datetime import datetime
# import datetime
# import pytz
# import os
# import numpy as np

# def download_file_from_gcs(bucket_name, source_blob_name, destination_file_name):
#     """Downloads a file from Google Cloud Storage."""
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(source_blob_name)
#     blob.download_to_filename(destination_file_name)
#     print(f'File downloaded from GCS: {source_blob_name}')

# def execute_sql_queries(table_name):
#     client = bigquery.Client()

#     if(table_name == 'liquiloans_master'):

#         column_name = 'name'

#     elif(table_name == 'mutualfunds_master' or table_name == 'equity_master'):

#         column_name = 'h_name'

#     elif(table_name == 'sip_master'):

#         column_name = 'c_name'

#     elif(table_name == 'bonds'):

#         column_name = 'Name'

#     elif(table_name == 'vested'):

#         column_name = 'Client_Name'

#     elif(table_name == 'unify'):

#         column_name = 'Name'

#     elif(table_name == 'icici_pms'):

#         column_name = 'clientname'

#     elif(table_name == 'hbits'):

#         column_name = 'name_of_the_investor'

#     elif(table_name == 'insurance_icici'):

#         column_name = 'Customer_Full_Name'

#     elif(table_name == 'insurance_max_bupa'):

#         column_name = 'Full_Name'

#     elif(table_name == 'fixed_deposit'):

#         column_name = 'Customer_Name'

#     elif(table_name == 'strata'):

#         column_name = 'name_on_pan'

#     elif(table_name == 'insurance_bse'):

#         column_name = 'Customer_First_Name'

#     # First SQL query
#     query1 = f"""
#         UPDATE `elegant-tendril-399501.winrich_dev_v2.{table_name}` AS fd
#         SET master_customer_id = (
#             SELECT mcd.master_customer_id
#             FROM `elegant-tendril-399501.winrich_dev_v2.master_customers_data` AS mcd
#             WHERE LOWER(REPLACE(TRIM(fd.{column_name}), ' ', '')) = LOWER(REPLACE(TRIM(mcd.master_username), ' ', '')) 
#         )
#         WHERE fd.master_customer_id is null;
#         """
#     # Second SQL query
#     query2 = f"""
#     UPDATE `elegant-tendril-399501.winrich_dev_v2.{table_name}` AS fd
#     SET master_customer_id = (
#         SELECT ons.master_customer_id
#         FROM `elegant-tendril-399501.winrich_dev_v2.other_names` AS ons
#         WHERE LOWER(REPLACE(TRIM(fd.{column_name}), ' ', '')) = LOWER(REPLACE(TRIM(ons.other_names), ' ', '')) 
#     )
#     WHERE fd.master_customer_id is null;
#     """
    
#     print('Query-2')
#     print(query2)

#     if table_name == 'equity_master':
#         category_update_query_by_condition = f"""
#         UPDATE `elegant-tendril-399501.winrich_dev_v2.equity_master`
#         SET category = CASE
#             WHEN LOWER(symbol) LIKE '%gold%' OR LOWER(symbol) LIKE '%sgb%' THEN 'G'
#             WHEN (REGEXP_CONTAINS(LOWER(symbol), '[a-z]') AND REGEXP_CONTAINS(LOWER(symbol), '[0-9]')) THEN 'B'
#             ELSE 'E'
#         END
#         WHERE TRUE
#         """
#         # Execute the category_update_by_condition SQL query
#         query_job_category1 = client.query(category_update_query_by_condition)
#         query_job_category1.result()  # Waits for the query to finish
#         print(category_update_query_by_condition)
#         print("Category based on the conditions updated successfully.")

#         category_update_by_exception_list = f"""
            
#             UPDATE `elegant-tendril-399501.winrich_dev_v2.{table_name}` AS em
#             SET category = (
#                 SELECT excep_list.category
#                 FROM `elegant-tendril-399501.winrich_dev_v2.equity_master_category_exception_list` AS excep_list
#                 WHERE LOWER(REPLACE(TRIM(em.symbol), ' ', '')) = LOWER(REPLACE(TRIM(excep_list.symbol), ' ', ''))
#                 AND excep_list.category IS NOT NULL AND excep_list.category <> ''
#             )
#             WHERE EXISTS (
#                 SELECT 1
#                 FROM `elegant-tendril-399501.winrich_dev_v2.equity_master_category_exception_list` AS excep_list
#                 WHERE LOWER(REPLACE(TRIM(em.symbol), ' ', '')) = LOWER(REPLACE(TRIM(excep_list.symbol), ' ', ''))
#             );

#         """

#         # Execute the category_update_by_exception_list SQL query
#         query_job_category2 = client.query(category_update_by_exception_list)
#         query_job_category2.result()  # Waits for the query to finish
#         print(category_update_by_exception_list)
#         print("Category based on the exception list updated successfully.")

#     # Third SQL Query
#     query3 = """
#     DELETE FROM elegant-tendril-399501.winrich_dev_v2.sip_master
#     WHERE c_name = 'nan' AND
#     month1 = 0.0
#     AND month2 = 0.0
#     AND month3 = 0.0
#     AND month4 = 0.0
#     AND month5 = 0.0
#     AND month6 = 0.0
#     AND month7 = 0.0
#     AND month8 = 0.0
#     AND month9 = 0.0
#     AND month10 = 0.0
#     AND month11 = 0.0
#     AND month12 = 0.0;
#     """

#     # Fourth SQL Query
#     query4 = """
#     DELETE FROM `elegant-tendril-399501.winrich_dev_v2.icici_pms`
#     where clientcode = 'nan' and clientname = 'nan' and productcode = 'nan' and pan = 'nan';
#     """

#     # Execute the first SQL query
#     query_job1 = client.query(query1)
#     query_job1.result()  # Waits for the query to finish
#     print(query1)
#     print("First data mapping successful.")

#     # Execute the second SQL query
#     query_job2 = client.query(query2)
#     query_job2.result()  # Waits for the query to finish
#     print(query2)
#     print("Second data mapping successful.")

#     if(table_name == 'sip_master'):

#         # Execute the Third SQL query
#         query_job3 = client.query(query3)
#         query_job3.result()  # Waits for the query to finish
#         print("Null Rows Deleted From SIP Master")

#     if(table_name == 'icici_pms'):

#         # Execute the Fourth SQL query
#         query_job4 = client.query(query4)
#         query_job4.result()  # Waits for the query to finish
#         print("Null Rows Deleted From icici_pms")

# def get_table_schema(table_name):
#     print('under table_schema')
#     client = bigquery.Client()
#     dataset_ref = client.dataset('winrich_dev_v2')
#     table_ref = dataset_ref.table(table_name)
#     table = client.get_table(table_ref)

#     print('table: ', table)
    
#     schema = [(field.name, field.field_type) for field in table.schema]

#     print('schema: ', schema)
#     return schema

# def hello_gcs(event, context):
#     client = bigquery.Client()

#     file_name = event['name'].lower()
#     print('file_name: ', file_name)

#     dataset_ref = client.dataset('winrich_dev_v2')
#     tables = client.list_tables(dataset_ref)
#     table_names = [table.table_id for table in tables]

#     # table_name = next((table for table in table_names if table in file_name), file_name.split('.')[0])

#     print('%%%%%%%%%%%%%%%%%%%%%%%%%')
#     # print(table_name)

#     if 'deposit' in file_name:
#         table_name = 'fixed_deposit'
#     elif 'sipmom' in file_name:
#         table_name = 'sip_master'
#     elif 'equity' in file_name or 'stocks' in file_name:
#         table_name = 'equity_master'
#     elif 'golden' in file_name:
#         table_name = 'bonds'
#     elif 'liqui' in file_name:
#         table_name = 'liquiloans_master'
#     elif 'mutual' in file_name:
#         table_name = 'mutualfunds_master'
#     elif 'strata' in file_name:
#         table_name = 'strata'
#     elif 'unify' in file_name:
#         table_name = 'unify'
#     elif 'pms' in file_name or 'icici_pms' in file_name:
#         table_name = 'icici_pms'
#     elif 'hbits' in file_name:
#         table_name = 'hbits'
#     elif 'icici' in file_name:
#         table_name = 'insurance_icici'
#     elif 'max_bupa' in file_name:
#         table_name = 'insurance_max_bupa'
#     elif 'bse' in file_name:
#         table_name = 'insurance_bse'
#     elif 'vested' in file_name or 'funded' in file_name or 'accounts' in file_name:
#         table_name = 'vested'
#     else:
#         # table_name = file_name.split('.')[0]
#         print('This function only process all the financial products csv files')
#         return None


#     print(f'File: {file_name}, Table: {table_name}')
#     current_time = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")

#     print('current time: ', current_time)
#     print('type: ', type(current_time))

#     try:
#         file_path = f'/tmp/{os.path.dirname(file_name)}'
#         os.makedirs(file_path, exist_ok=True)
#         file_path = f'/tmp/{file_name}'

#         print('file_path:', file_path)
#         download_file_from_gcs(event['bucket'], event['name'], file_path)

#         file_data = pd.read_csv(file_path)
#         print("CSV file loaded successfully")

#         print(f"Processing file: {file_name}")
#         print(f"Associated table: {table_name}")

#         file_data['created_at'] = datetime.datetime.now()
#         file_data['updated_at'] = datetime.datetime.now()

#         # print('time from datetime: ', datetime.datetime.now())

#         print('created_at: ', current_time)

#         table_schema = get_table_schema(table_name)
#         print("Schema for table {}: {}".format(table_name, table_schema))

#         # print('before data types: ', file_data.dtypes)

#         # Strip leading and trailing spaces from column names

#         file_data.columns = file_data.columns.str.strip()

#         # Checking whether the today's file is already processed or not

#         # Perform a query
#         is_file_already_processed_query = f"""
#                 select * from `elegant-tendril-399501.winrich_dev_v2.{table_name}`
#                 where Date(created_at) = current_date();
#         """
#         query_job = client.query(is_file_already_processed_query)
#         # print(query_job)

#         data_from_corresponding_table = []

#         # Process query results
#         for row in query_job:

#             data_from_corresponding_table.append(row)

#         print(data_from_corresponding_table)

#         if(data_from_corresponding_table and data_from_corresponding_table is not None and len(data_from_corresponding_table) > 0):
#             print('The file: ' + str(file_name) + ' is already processed')
#             return None
#         print('Need to process the data!')

#         if table_name == 'mutualfunds_master':

#             print('inside mutual funds')

#             column_mappings = {
#                 'sCode': 's_code',
#                 'Nature': 'nature',
#                 'Email': 'email',
#                 'Mobile': 'mobile',
#                 'FolioStartDate': 'folio_start_date',
#                 'AvgCost': 'avg_cost',
#                 'InvAmt': 'inv_amt',
#                 'TotalInvAmt': 'total_inv_amt',
#                 'CurNAV': 'cur_nav',
#                 'CurValue': 'cur_value',
#                 'DivAmt': 'div_amt',
#                 'NotionalGain': 'notional_gain',
#                 'ActualGain': 'actual_gain',
#                 'FolioXIRR': 'folio_xirr',
#                 'NatureXIRR': 'nature_xirr',
#                 'ClientXIRR': 'client_xirr',
#                 'NatureAbs': 'nature_abs',
#                 'ClientAbs': 'client_abs',
#                 'absReturn': 'abs_return',
#                 'BalUnit': 'bal_unit',
#                 'ValueDate': 'value_date',
#                 'ReportDate': 'report_date'
#             }

#             # Rename columns using the dictionary
#             file_data.rename(columns=column_mappings, inplace=True)


#             column_data_types = file_data.dtypes

#             print('after data types: ', column_data_types)
#             print('file data types')
#             print(column_data_types)

#             # Define column mappings
#             # column_mappings = {'sCode': 's_code'}

#             # Rename columns
#             # file_data.rename(columns=column_mappings, inplace=True)

#             # List of columns to be converted to string data type
#             str_columns = ['h_name', 'c_name', 's_code', 's_name', 'foliono', 'nature', 'folio_start_date', 'bal_unit' , 'email', 'mobile', 'value_date', 'report_date']

#             # List of columns to be converted to float data type
#             float_columns = ['avg_cost', 'inv_amt', 'total_inv_amt', 'cur_nav', 'cur_value', 'div_amt', 'notional_gain', 'actual_gain', 'folio_xirr', 'nature_xirr', 'client_xirr', 'nature_abs', 'client_abs', 'abs_return']

#             # Convert columns to string data type
#             file_data[str_columns] = file_data[str_columns].astype(str)

#             # Convert columns to float data type
#             file_data[float_columns] = file_data[float_columns].astype(float)

#             # print('file data columns: ', file_data)

#             # Convert 'FolioStartDate', 'ValueDate', and 'ReportDate' columns to datetime data type
#             # date_columns = ['created_at', 'updated_at']
#             # file_data[date_columns] = file_data[date_columns].apply(pd.to_datetime)


#         elif(table_name == 'liquiloans_master'):

#             print('Inside the liquiloans block')

#             # # Strip leading and trailing spaces from column names

#             # file_data.columns = file_data.columns.str.strip()

#             file_data['investori'] = file_data['investori'].astype(int)

#             print('file data name: ', file_data['name'])

#             file_data.rename(columns={'annualized_return': 'annualized_return_'}, inplace=True)

#             file_data['name'] = file_data['name'].astype(str)

#             # Convert current_value column to float, coercing errors
#             file_data['current_value'] = pd.to_numeric(file_data['current_value'], errors='coerce')
            
#             # Optionally, fill NaN values with a default value, e.g., 0.0
#             file_data['current_value'].fillna(0.0, inplace=True)

#             # Convert current_value column to float, coercing errors
#             file_data['annualized_return_'] = pd.to_numeric(file_data['annualized_return_'], errors='coerce')
            
#             # Optionally, fill NaN values with a default value, e.g., 0.0
#             file_data['annualized_return_'].fillna(0.0, inplace=True)


#         elif(table_name == 'sip_master'):

#             columns_to_convert = ['c_name', 'Month1', 'Month2', 'Month3', 'Month4', 'Month5', 'Month6', 'Month7', 'Month8', 'Month9', 'Month10', 'Month11', 'Month12']

#             for column in columns_to_convert:
#                 if column == 'c_name':
#                     file_data[column] = file_data[column].astype(str)
#                 else:
#                     file_data[column] = file_data[column].astype(float)

#         elif(table_name == 'bonds'):

#             # Keep only the relevant columns
#             columns_to_keep = ['Name', 'Email', 'Phone', 'Order Date', 'Issuer Name', 'Coupon', 'Maturity Date', 'Units', 'Price', 'Investment Amount']
#             file_data = file_data[columns_to_keep]

#             file_data.rename(columns={
#                 'Name': 'Name',
#                 'Email': 'Email',
#                 'Phone': 'Phone',
#                 'Order Date': 'Order_Date',
#                 'Issuer Name': 'Issuer_Name',
#                 'Coupon': 'Coupon',
#                 'Maturity Date': 'Maturity_Date',
#                 'Units': 'Units',
#                 'Price': 'Price',
#                 'Investment Amount': 'Investment_Amount',
#             }, inplace=True)

#             # Convert columns to appropriate types
#             file_data['Phone'] = pd.to_numeric(file_data['Phone'], errors='coerce').fillna(0).astype(int)
#             file_data['Units'] = pd.to_numeric(file_data['Units'], errors='coerce').fillna(0).astype(int)
#             file_data['Coupon'] = pd.to_numeric(file_data['Coupon'], errors='coerce').fillna(0.0).astype(float)
#             file_data['Price'] = pd.to_numeric(file_data['Price'], errors='coerce').fillna(0.0).astype(float)
#             file_data['Investment_Amount'] = pd.to_numeric(file_data['Investment_Amount'], errors='coerce').fillna(0.0).astype(float)
#             file_data['Order_Date'] = file_data['Order_Date'].astype(str)
#             file_data['Issuer_Name'] = file_data['Issuer_Name'].astype(str)
#             file_data['Maturity_Date'] = file_data['Maturity_Date'].astype(str)
#             file_data['Name'] = file_data['Name'].astype(str)
#             file_data['Email'] = file_data['Email'].astype(str)

#             file_data['created_at'] = datetime.datetime.now()
#             file_data['updated_at'] = datetime.datetime.now()

#         elif(table_name == 'icici_pms'):

#             print('under icici pms')

#             # Find the row index where the actual data starts (assuming it starts after a specific header row)
#             start_index = file_data[file_data.iloc[:, 0] == 'CLIENTCODE'].index[0]

#             # Remove the unwanted header rows
#             file_data = file_data.iloc[start_index:]

#             # Reset the column headers
#             file_data.columns = file_data.iloc[0]
#             file_data = file_data[1:]

#             # Keep only the relevant columns
#             columns_to_keep = ['CLIENTCODE', 'CLIENTNAME', 'AUM', 'PRODUCTCODE', 'PAN']
#             file_data = file_data[columns_to_keep]

#             # Reset the index
#             file_data.reset_index(drop=True, inplace=True)

#             file_data['created_at'] = datetime.datetime.now()
#             file_data['updated_at'] = datetime.datetime.now()

#             # Print the cleaned DataFrame (for debugging)
#             print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
#             print(file_data.head())

#             file_data.rename(columns={
#             'CLIENTCODE': 'clientcode',
#             'CLIENTNAME': 'clientname',
#             'AUM': 'aum',
#             'PRODUCTCODE': 'productcode',
#             'PAN': 'pan'
#             }, inplace=True)

#             file_data = file_data.astype({
#             'clientcode': str,
#             'clientname': str,
#             'aum': float,
#             'productcode': str,
#             'pan': str
#             })

#         elif(table_name == 'hbits'):

#             file_data.rename(columns={
#             'Name of the Investor': 'name_of_the_investor',
#             "Father's Name": 'fathers_name',
#             'Investment Amount': 'investment_amount',
#             'SPV': 'spv',
#             'Investment Amount LMS': 'investment_amount_lms',
#             'Property Name': 'property_name'
#             }, inplace=True)

#             file_data = file_data.astype({
#                 'name_of_the_investor': str,
#                 'fathers_name': str,
#                 'investment_amount': int,
#                 'spv': str,
#                 'investment_amount_lms': int,
#                 'property_name': str
#             })

#         elif(table_name == 'insurance_icici'):

#             file_data.rename(columns={
#             'Policy No': 'Policy_No',
#             'Customer Full Name': 'Customer_Full_Name',
#             'Product Name': 'Product_Name',
#             'Mobile No': 'Mobile_No',
#             'Due Date': 'Due_Date',
#             'Risk Com Date': 'Risk_Com_Date',
#             'Issuance Date': 'Issuance_Date',
#             'Premium Paying Term': 'Premium_Paying_Term',
#             'Premium Amount': 'Premium_Amount',
#             'Sum Assured': 'Sum_Assured',
#             'Bill Channel': 'Bill_Channel',
#             'Suspense Account Balance': 'Suspense_Account_Balance',
#             'Net Amount Due': 'Net_Amount_Due',
#             'City': 'City____________',
#             'Phone1': 'Phone1________',
#             'Phone2': 'Phone2_______',
#             'Email': 'Email',
#             'Payment Frequency': 'Payment_Frequency'
#             }, inplace=True)

#             # converting the Due_Date and Issuance_Date to be in date time ( timestamp ) type

#             file_data['Due_Date'] = pd.to_datetime(file_data['Due_Date'])

#             file_data['Issuance_Date'] = pd.to_datetime(file_data['Issuance_Date'])

#             file_data['Risk_Com_Date'] = pd.to_datetime(file_data['Risk_Com_Date'], format='%d-%b-%Y').dt.strftime('%Y-%m-%d')

#             # List of columns to be converted to string data type
#             str_columns = ['Policy_No', 'Customer_Full_Name', 'Product_Name', 'Bill_Channel', 'City____________', 'Phone1________', 'Phone2_______', 'Email' , 'Payment_Frequency']

#             # List of columns to be converted to int data type
#             int_columns = ['Mobile_No', 'Premium_Paying_Term', 'Premium_Amount', 'Sum_Assured', 'Suspense_Account_Balance', 'Net_Amount_Due']

#             # Convert columns to string data type
#             file_data[str_columns] = file_data[str_columns].astype(str)

#             # Convert columns to int data type
#             file_data[int_columns] = file_data[int_columns].astype(int)

#         elif table_name == 'insurance_max_bupa':

#             print('under max_bupa')

#             # Remove the "First Name" column if it exists in the DataFrame
#             if 'First Name' in file_data.columns:
#                 file_data.drop(columns=['First Name'], inplace=True)

#             if 'Last Name' in file_data.columns:
#                 file_data.drop(columns=['Last Name'], inplace=True)

#             # Define the column mapping
#             column_mapping = {
#                 'Full Name': 'Full_Name',
#                 'Application Number': 'Application_No',
#                 'Previous Policy Number': 'Old_Policy_Number',
#                 'Policy Number': 'New_Policy_Number',
#                 'Customer ID': 'Customer_ID',
#                 'DOB': 'DOB',
#                 'Plan Type': 'Plan_Type',
#                 'Product ID': 'Product_ID',
#                 'Product Genre': 'Product_Genre',
#                 'Insured Lives': 'Insured_Lives',
#                 'Insured Count': 'Insured_Count',
#                 'Renewal Premium': 'Renewal_Premium',
#                 '2 Years Renewal Premium (With Tax)': '_2_Years_Renewal_Premium__With_Tax_',
#                 'Current Status': 'Current_Status_Workstep',
#                 'Issued Premium': 'Issued_Premium',
#                 'Renewal Premium (Without Taxes)': 'Renewal_Premium__Without_Taxes_',
#                 'Loading Premium': 'Loading_Premium',
#                 'Sum Assured': 'Sum_Assured',
#                 'Individual Sum Assured': 'Individual_Sum_Assured',
#                 'Health Assurance Critical Illness/Criticare Sum Assured': 'Health_Assurance_Critical_Illness_Criticare_Sum_Assured',
#                 'Health Assurance Personal Accident/Accident Care Sum Assured': 'Health_Assurance_Personal_Accident_Accident_Care_Sum_Assured',
#                 'Health Assurance Hospital Cash/Hospicash Sum Assured': 'Health_Assurance_Hospital_Cash_Hospicash_Sum_Assured',
#                 'Login Branch': 'Branch',
#                 'Sales Branch': 'Sales_Branch',
#                 'Zone': 'Zone',
#                 'Renewal Channel': 'Renewal_Channel',
#                 'Renewal Sub Channel': 'Renewal_Sub_Channel',
#                 'Renewal Agent Code': 'Renewal_Agent_Code',
#                 'Renewal Agent Name': 'Renewal_Agent_Name',
#                 'Renewal Agent Type': 'Renewal_Agent_Type',
#                 'Pa Code': 'Pa_Code',
#                 'Conversion Date': 'Conversion_Date',
#                 'Agency Manager ID': 'Agency_Manager_ID',
#                 'Agency Manager Name': 'Agency_Manager_Name',
#                 'Renewal Logged Date': 'Renewal_Logged_Date',
#                 'Renewal Logged Month': 'Renewal_Logged_Month',
#                 'Renewal Issued Date': 'Renewal_Issued_Date',
#                 'Renewal Issued Month': 'Renewal_Issued_Month',
#                 'Maximus Status': 'Maximus_Status',
#                 'Lead Status': 'Lead_Status',
#                 'Sales Status': 'Sales_Status',
#                 'Hums Status': 'Hums_Status',
#                 'Hums Status Update Date': 'Hums_Status_Update_Date',
#                 'Current Team': 'Current_Team',
#                 'Current Status Ageing': 'Current_Status_Ageing',
#                 'Login Ageing': 'Login_Ageing',
#                 'Designation': 'Designation',
#                 'Policy Start Date': 'Policy_Start_Date',
#                 'Policy Expiry Date': 'Policy_Expiry_Date',
#                 'Is Portability': 'Is_Portability',
#                 'Is Split': 'Split_Flag',
#                 'Is Upsell': 'Upsell_Eligibility',
#                 'Upsell Limit': 'Upsell_Limit',
#                 'Plan Name': 'Plan_Name',
#                 'Renew Now': 'Renew_Now',
#                 'Whatsapp Communication for Policy Information': 'Whatsapp_Communication_for_Policy_Information',
#                 'Communication Acknowledgement(Over Ride DND)': 'Communication_Acknowledgement_Over_Ride_DND_',
#                 'Safe Guard': 'Safeguard_Rider_Taken',
#                 'Policy Tenure': 'Policy_Tenure',
#                 'Product Name': 'Product_Name'
#             }

#             # Filter the column mapping to include only columns present in the DataFrame
#             filtered_column_mapping = {k: v for k, v in column_mapping.items() if k in file_data.columns}

#             # Rename the columns
#             file_data.rename(columns=filtered_column_mapping, inplace=True)

#             # List of columns to be converted to int data type
#             int_columns = [
#                 "Application_No","Old_Policy_Number","Customer_ID","Product_ID","Insured_Lives","Renewal_Premium","_2_Years_Renewal_Premium__With_Tax_","Loading_Premium","Sum_Assured","Individual_Sum_Assured","Health_Assurance_Personal_Accident_Accident_Care_Sum_Assured","Health_Assurance_Hospital_Cash_Hospicash_Sum_Assured","Whatsapp_Communication_for_Policy_Information"
#             ]

#             # Filter the list to include only columns present in the DataFrame
#             existing_int_columns = [col for col in int_columns if col in file_data.columns]

#             # Replace non-finite values with 0
#             file_data[existing_int_columns] = file_data[existing_int_columns].replace([np.inf, -np.inf, np.nan], 0)


#             # List of columns to be converted to string data type
#             str_columns = [
#                 "Full_Name",
#                 "New_Policy_Number",
#                 "Plan_Type",
#                 "Product_Genre",
#                 "Insured_Count",
#                 "Current_Status_Workstep",
#                 "Issued_Premium",
#                 "Renewal_Premium__Without_Taxes_",
#                 "Branch",
#                 "Sales_Branch",
#                 "Zone",
#                 "Renewal_Channel",
#                 "Renewal_Sub_Channel",
#                 "Renewal_Agent_Code",
#                 "Renewal_Agent_Name",
#                 "Renewal_Agent_Type",
#                 "Pa_Code",
#                 "Conversion_Date",
#                 "Agency_Manager_ID",
#                 "Agency_Manager_Name",
#                 "Renewal_Logged_Date",
#                 "Renewal_Logged_Month",
#                 "Renewal_Issued_Date",
#                 "Renewal_Issued_Month",
#                 "Maximus_Status",
#                 "Lead_Status",
#                 "Sales_Status",
#                 "Hums_Status",
#                 "Hums_Status_Update_Date",
#                 "Current_Team",
#                 "Current_Status_Ageing",
#                 "Login_Ageing",
#                 "Designation",
#                 "Is_Portability",
#                 "Upsell_Eligibility",
#                 "Upsell_Limit",
#                 "Plan_Name",
#                 "Renew_Now",
#                 "Policy_Tenure",
#                 "Product_Name"
#             ]		

#             # Filter the list to include only columns present in the DataFrame
#             existing_str_columns = [col for col in str_columns if col in file_data.columns]				

#             # List of columns to be converted to bool data type
#             bool_columns = [
#                 'Communication_Acknowledgement_Over_Ride_DND_', 'Safeguard_Rider_Taken', 'Health_Assurance_Critical_Illness_Criticare_Sum_Assured', 'Split_Flag'
#             ]

#             # Filter the list to include only columns present in the DataFrame
#             existing_bool_columns = [col for col in bool_columns if col in file_data.columns]

#             # Convert columns to string data type
#             file_data[existing_str_columns] = file_data[existing_str_columns].astype(str)

#             # Convert columns to int data type
#             file_data[existing_int_columns] = file_data[existing_int_columns].astype(int)

#             # Convert columns to bool data type
#             file_data[existing_bool_columns] = file_data[existing_bool_columns].astype(bool)

#             file_data['Policy_Start_Date'] = pd.to_datetime(file_data['Policy_Start_Date'])
#             file_data['Policy_Expiry_Date'] = pd.to_datetime(file_data['Policy_Expiry_Date'])

#         elif table_name == 'equity_master':

#             # Filter out rows where 'Client Name' is empty
#             file_data = file_data[file_data['Client Name'].notna()]
            
#             # Print skipped rows
#             skipped_rows = file_data[file_data['Client Name'].isna()]
#             for index, row in skipped_rows.iterrows():
#                 print(f"Skipping row {index} due to missing 'Client Name': {row}")

#             file_data = file_data.astype({
#                 'Client Code': str,
#                 'Client Name': str,
#                 'Scrip Code': str,
#                 'Symbol': str,
#                 'Pool Holdings': float,
#                 'Pledge Holdings': float,
#                 'DP Account Holdings': float,
#                 'Net Holdings': float,
#                 'Total Value (?)': float,
#                 'Bluechip (?)': float,
#                 'Good (?)': float,
#                 'Average (?)': float,
#                 'Poor (?)': float
#             })

#             # Copy 'Client Name' to 'c_name'
#             file_data['c_name'] = file_data['Client Name']

#             file_data.rename(columns={
#             'Client Code': 'clientcode',
#             'Client Name': 'h_name',
#             'Scrip Code': 'scripcode',
#             'Symbol': 'symbol',
#             'Pool Holdings': 'poolholdings',
#             'Pledge Holdings': 'pledgeholdings',
#             'DP Account Holdings': 'dpaccountholdings',
#             'Net Holdings': 'netholdings',
#             'Total Value (?)': 'totalvalue',
#             'Bluechip (?)': 'bluechip',
#             'Good (?)': 'good',
#             'Average (?)': 'average',
#             'Poor (?)': 'poor'
#             }, inplace=True)

#         elif table_name == 'unify':
#             print('under unify block')

#             file_data['Capital Invested'] = file_data['Capital Invested'].str.replace(',', '')
#             file_data['Capital Withdrwal'] = file_data['Capital Withdrwal'].str.replace(',', '')
#             file_data['Net Capital'] = file_data['Net Capital'].str.replace(',', '')
#             file_data['Assets'] = file_data['Assets'].str.replace(',', '')
#             file_data['TWRR'] = file_data['TWRR'].str.rstrip('%')
#             file_data['IRR'] = file_data['IRR'].str.rstrip('%')

#             # print('file_data --------->', file_data)

#             file_data = file_data.astype({
#             'Name': str,
#             'Strategy': str,
#             # 'Inception': 'Inception',
#             'Capital Invested': int,
#             'Capital Withdrwal': int,
#             'Net Capital': int,
#             'Assets': int,
#             'TWRR': float,
#             'IRR': float
#             })

#             file_data.rename(columns={
#             'Name': 'Name',
#             'Strategy': 'Strategy',
#             'Inception': 'Inception',
#             'Capital Invested': 'Capital_Invested',
#             'Capital Withdrwal': 'Capital_Withdrwal',
#             'Net Capital': 'Net_Capital',
#             'Assets': 'Assets',
#             'TWRR': 'TWRR',
#             'IRR': 'IRR'
#             }, inplace=True)

#         elif table_name == 'fixed_deposit':
#             print('under fixed_deposit block')

#             file_data.drop(columns=['Sr.No'], inplace=True)

#             file_data['Interest Start Date'] = pd.to_datetime(file_data['Interest Start Date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

#             # print('file_data --------->', file_data)

#             file_data = file_data.astype({
#             'Depositt ID': int,
#             'Customer ID': int,
#             # 'Interest Start Date': int,
#             'Application No': int,
#             'Customer Name': str,
#             'PAN': str,
#             'Rate': float,
#             'Month': int,
#             'Amount': int,
#             'Interest Amount': int,
#             'Maturity Amount': int
#             })

#             file_data.rename(columns={
#             'Depositt ID': 'Depositt_ID',
#             'Customer ID': 'Customer_ID',
#             'Interest Start Date': 'Interest_Start_Date',
#             'Application No': 'Application_No',
#             'Customer Name': 'Customer_Name',
#             'PAN': 'PAN',
#             'Rate': 'Rate',
#             'Month': 'Month',
#             'Amount': 'Amount',
#             'Interest Amount': 'Interest_Amount',
#             'Maturity Amount': 'Maturity_Amount'
#             }, inplace=True)


#         elif table_name == 'vested':
#             print('under vested block')

#             # file_data['Account Created On'] = pd.to_datetime(file_data['Account Created On'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
#             # file_data['KYC Approved On'] = pd.to_datetime(file_data['KYC Approved On'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
#             # file_data['First Funded On'] = pd.to_datetime(file_data['First Funded On'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
#             # file_data['Last Login'] = pd.to_datetime(file_data['Last Login'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')


#             file_data.rename(columns={
#             'Vested User ID': 'Vested_User_ID',
#             'Client Name': 'Client_Name',
#             'Email': 'Email',
#             'Phone Number': 'Phone_Number',
#             'Account Created On': 'Account_Created_On',
#             'KYC Approved On': 'KYC_Approved_On',
#             'First Funded On': 'First_Funded_On',
#             'Last Login': 'Last_Login',
#             'Equity Value (USD)': 'Equity_Value__USD_', 
#             'Cash Value (USD)': 'Cash_Value__USD_', 
#             'Unrealized P&L': 'Unrealized_P_L',
#             'Pricing Tier': 'Pricing_Tier'
#             }, inplace=True)

#             # Convert numeric fields to float if they are represented as strings
#             numeric_fields = ['Equity_Value__USD_', 'Cash_Value__USD_', 'Unrealized_P_L']
#             for field in numeric_fields:
#                 file_data[field] = pd.to_numeric(file_data[field], errors='coerce')
            
#             # Convert dates from various formats to 'YYYY-MM-DD' format
#             date_fields = ['Account_Created_On', 'KYC_Approved_On', 'First_Funded_On', 'Last_Login']
#             for field in date_fields:
#                 file_data[field] = pd.to_datetime(file_data[field], errors='coerce').dt.strftime('%Y-%m-%d')

#         elif table_name == 'insurance_bse':
#             print('under insurance bse block')

#             file_data = file_data.astype({
#             "Month": str,
#             "Months": str,
#             "Ref ID": str,
#             "Associate Code": str,
#             "POSP Name": str,
#             "Region": str,
#             "Agent PAN": str,
#             "Channel": str,
#             "Customer First Name": str,
#             "Business Type": str,
#             "Product type": str,
#             "Coverage Type": str,
#             "Policy Number": str,
#             "Policy Number clean": str,
#             "Policy Issue Date": str,
#             "Start Date": str,
#             "Expiry Date": str,
#             "Payment Date": str,
#             "Insurer": str,
#             "Product Name": str,
#             "Policy Term": str,
#             "Premium Payment Term": str,
#             "Premium Payment Frequency": str,
#             "Sum Insured": str,
#             "OD Start Date": str,
#             "OD End Date": str,
#             "OD Year": str,
#             "TP Start Date": str,
#             "TP End Date": str,
#             "TP Year": str,
#             "RTO State": str,
#             "RTO City": str,
#             "Vehicle Type": str,
#             "Vehicle Registration No": str,
#             "Vehicle Registration Date": str,
#             "Vehicle Manufacture": str,
#             "Vehicle Make Model": str,
#             "Vehicle Engine No": str,
#             "Vehicle Chassis No": str,
#             "GVW": str,
#             "Vehicle Cubic Capacity": str,
#             "Vehicle Seat Capacity": str,
#             "Fuel": str,
#             "Vehicle Category": str,
#             "NCB": str,
#             "IDV": str,
#             "OD Premium Amount": str,
#             "TP Premium Amount": str,
#             "CPA": str,
#             "Net Premium": str,
#             "GST": str,
#             "Gross Premium Amount": str,
#             "% On OD Payout": str,
#             "% On TP Payout": str,
#             "% Net Payout": str,
#             "POSP PayOut": str,
#             "Expected PayOut Amount": str,
#             "BQP Points less": str,
#             "POSP/BQP": str,
#             "Final Status": str
#             })

#             file_data.rename(columns={
#             "Month": "Month",
#             "Months": "Months",
#             "Ref ID": "Ref_ID",
#             "Associate Code": "Associate_Code",
#             "POSP Name": "POSP_Name",
#             "Region": "Region",
#             "Agent PAN": "Agent_PAN",
#             "Channel": "Channel",
#             "Customer First Name": "Customer_First_Name",
#             "Business Type": "Business_Type",
#             "Product type": "Product_type",
#             "Coverage Type": "Coverage_Type",
#             "Policy Number": "Policy_Number",
#             "Policy Number clean": "Policy_Number_clean",
#             "Policy Issue Date": "Policy_Issue_Date",
#             "Start Date": "Start_Date",
#             "Expiry Date": "Expiry_Date",
#             "Payment Date": "Payment_Date",
#             "Insurer": "Insurer",
#             "Product Name": "Product_Name",
#             "Policy Term": "Policy_Term",
#             "Premium Payment Term": "Premium_Payment_Term",
#             "Premium Payment Frequency": "Premium_Payment_Frequency",
#             "Sum Insured": "Sum_Insured",
#             "OD Start Date": "OD_Start_Date",
#             "OD End Date": "OD_End_Date",
#             "OD Year": "OD_Year",
#             "TP Start Date": "TP_Start_Date",
#             "TP End Date": "TP_End_Date",
#             "TP Year": "TP_Year",
#             "RTO State": "RTO_State",
#             "RTO City": "RTO_City",
#             "Vehicle Type": "Vehicle_Type",
#             "Vehicle Registration No": "Vehicle_Registration_No",
#             "Vehicle Registration Date": "Vehicle_Registration_Date",
#             "Vehicle Manufacture": "Vehicle_Manufacture",
#             "Vehicle Make Model": "Vehicle_Make_Model",
#             "Vehicle Engine No": "Vehicle_Engine_No",
#             "Vehicle Chassis No": "Vehicle_Chassis_No",
#             "GVW": "GVW",
#             "Vehicle Cubic Capacity": "Vehicle_Cubic_Capacity",
#             "Vehicle Seat Capacity": "Vehicle_Seat_Capacity",
#             "Fuel": "Fuel",
#             "Vehicle Category": "Vehicle_Category",
#             "NCB": "NCB",
#             "IDV": "IDV",
#             "OD Premium Amount": "OD_Premium_Amount",
#             "TP Premium Amount": "TP_Premium_Amount",
#             "CPA": "CPA",
#             "Net Premium": "Net_Premium",
#             "GST": "GST",
#             "Gross Premium Amount": "Gross_Premium_Amount",
#             "% On OD Payout": "__On_OD_Payout",
#             "% On TP Payout": "__On_TP_Payout",
#             "% Net Payout": "__Net_Payout",
#             "POSP PayOut": "POSP_PayOut",
#             "Expected PayOut Amount": "Expected_PayOut_Amount",
#             "BQP Points less": "BQP_Points_less",
#             "POSP/BQP": "POSP_BQP",
#             "Final Status": "Final_Status"
#             }, inplace=True)
        

#         elif table_name == 'strata':
#             print('under strata block')

#             file_data.rename(columns={
#             'CP Name': 'CP_Name',
#             'IM Name': 'IM_Name',
#             'name_on_pan': 'name_on_pan',
#             'Amt Deal Value': 'Amt_Deal_Value',
#             'Amt Received': 'Amt_Received',
#             'status_name': 'status_name',
#             'asset_name': 'asset_name'
#             }, inplace=True)

#             # Convert numeric fields to float if they are represented as strings
#             numeric_fields = ['Amt_Deal_Value', 'Amt_Received']
#             for field in numeric_fields:
#                 file_data[field] = pd.to_numeric(file_data[field], errors='coerce')

#         # column_data_types = file_data.dtypes
#         # print('file data types')
#         # print(column_data_types)

#         # for index, row in file_data.iterrows():
#         #     if index >= 10:  # Check if we've printed 10 rows already
#         #         break 
#         #     for column in file_data.columns:
#         #         cell_value = row[column]
#         #         cell_data_type = type(cell_value)
#         #         print(f"Row: {index}, Column: {column}, Data Type: {cell_data_type}, Value: {cell_value}")

#         # file_data['created_at'] = current_time
#         # file_data['updated_at'] = current_time
#         # print('roshan')
#         # print(file_data)

#         print('current time: ', current_time)
#         print('type: ', type(current_time))

#         # current_date = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")

#         # print('Now Current date: ', current_date)

#         pandas_gbq.to_gbq(file_data, 'winrich_dev_v2.' + table_name, project_id='elegant-tendril-399501', if_exists='append', location='US')
#         # pandas_gbq.to_gbq(file_data, 'temp.' + table_name, project_id='elegant-tendril-399501', if_exists='append', location='asia-south1')
#         print(f"Data transfer successful for file: {file_name}")

#         execute_sql_queries(table_name)

#         print('Mapping done successfully')

#         metadata = {'Event_ID': context.event_id, 'Event_type': context.event_type, 'Bucket_name': event['bucket'], 'File_name': file_name, 'created_at': (current_time), 'updated_at': (current_time), 'status_flag': 1, 'status': 'success', 'failure_reason': None}
#         print('meta data: ', metadata)
#         # print('meta data data type: ', type(metadata))
#         metadata_df = pd.DataFrame.from_records([metadata])

#         # print('metadata_df')

#         # print(metadata_df)

#         print("Appending metadata to the metadata table")
#         pandas_gbq.to_gbq(metadata_df, 'winrich_dev_v2.gcs_bq_data_transfer_status_tracker', project_id='elegant-tendril-399501', if_exists='append', location='US')
#         print("Metadata appended successfully")

#     except Exception as e:
#         print(f"An error occurred while processing file: {file_name}. Error: {str(e)}")
#         print('**************************')
#         print(repr(e))

#         metadata_failure = {'Event_ID': context.event_id, 'Event_type': context.event_type, 'Bucket_name': event['bucket'], 'File_name': file_name, 'created_at': current_time, 'updated_at': current_time, 'status_flag': 0, 'status': 'fail', 'failure_reason': str(e) + '\n' + repr(e)}
#         metadata_failure_df = pd.DataFrame.from_records([metadata_failure])

#         print("Appending failure metadata to the metadata table")
#         pandas_gbq.to_gbq(metadata_failure_df, 'winrich_dev_v2.gcs_bq_data_transfer_status_tracker', project_id='elegant-tendril-399501', if_exists='append', location='US')
#         print("Failure metadata appended successfully")


# *******************************************************************************( Version-4 )****************************************************************************************


import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.cloud import storage
import functions_framework
# from datetime import datetime
import datetime
import pytz
import os
import numpy as np
import requests

def download_file_from_gcs(bucket_name, source_blob_name, destination_file_name):
    """Downloads a file from Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f'File downloaded from GCS: {source_blob_name}')


def send_direct_mf_to_windesk(client):

    query = f"""
                select h_name, c_name, s_code, s_name, foliono, nature, folio_start_date, 
                    bal_unit, avg_cost, inv_amt, total_inv_amt, cur_nav, cur_value, div_amt, 
                    notional_gain as national_gain, actual_gain, folio_xirr, nature_xirr, client_xirr, nature_abs, 
                    client_abs, abs_return, value_date, report_date, email, mobile, master_customer_id as customerid
                from `elegant-tendril-399501.winrich_dev_v2.mutualfunds_master`
                where Date(created_at) = current_date() AND fund_type = 'direct'
        """

    query_job = client.query(query)
    rows = query_job.result()

    results = [dict(row) for row in rows]


    api_url = "https://windesk.winrich.in/backend/api/v2/windesk/direct_mutualfunds"

    payload = {
        "data" : results
    }

    # 4. Send the Request
    response = requests.post(api_url, json=payload)
    response.raise_for_status()

    print(f"Success! direct mutual_funds sent to windesk with response {response}")


def sync_and_email_total_investment(client):

    query = """
        SELECT
            SUM(sum_of_all) AS total_sum_of_all
        FROM
            (SELECT
                COALESCE((SELECT SUM(cur_val) FROM `elegant-tendril-399501.winrich_dev_v2.bonds_v2` 
                WHERE master_customer_id = mcd.master_customer_id
                AND
                Date(created_at) = (
                    SELECT 
                        MAX(DATE(created_at)) 
                    FROM 
                        `elegant-tendril-399501.winrich_dev_v2.bonds_v2`
                    WHERE 
                        DATE(created_at) <= CURRENT_DATE()
                )
                GROUP BY master_customer_id), 0) 
                AS sum_of_all
            FROM
                `elegant-tendril-399501.winrich_dev_v2.master_customers_data` AS mcd
            ) AS top_investors
    """

    query_job = client.query(query)
    results = query_job.result()
    
    # Extract the value (default to 0 if none)
    total_sum = 0
    for row in results:
        total_sum = row.total_sum_of_all or 0

    # 3. Prepare Email via API
    api_url = "https://advisory.winwizeresearch.in/api/method/stock_portfolio_management.api.send_email"
    
    # Formatting the number for readability (e.g., 1,234,567.89)
    formatted_sum = f"₹{total_sum:,.2f}"
 

    html_message = f"""
    <div style="font-family: Arial, sans-serif; padding: 20px; border: 1px solid #eee; border-radius: 8px;">
        <h2 style="color: #2c3e50;">Portfolio Summary Report</h2>
        <p>Hello,</p>
        <p>The latest calculation for the total bond investment value has been completed.</p>
        <div style="background-color: #f8f9fa; padding: 15px; border-left: 5px solid #27ae60; margin: 20px 0;">
            <span style="font-size: 14px; color: #7f8c8d;">TOTAL SUM OF BONDS</span><br/>
            <strong style="font-size: 24px; color: #27ae60;">{formatted_sum}</strong>
        </div>
        <p style="font-size: 12px; color: #95a5a6;">This is an automated notification from the Winrich Dev System.</p>
    </div>
    """

    payload = {
        "recipient": "ashok@winrich.in",
        "subject": f"Investment Update for Bonds: Total Sum {formatted_sum}",
        "message": html_message
    }

    # 4. Send the Request
    response = requests.post(api_url, json=payload)
    response.raise_for_status()

    print(f"Success! Total sum {formatted_sum} sent to recipient.")


def execute_sql_queries(table_name):
    client = bigquery.Client()

    if(table_name == 'liquiloans_master'):

        column_name = 'name'

    elif(table_name == 'mutualfunds_master'):

        column_name = 'c_name'

    elif(table_name == 'equity_master'):

        column_name = 'h_name'

    elif(table_name == 'sip_master'):

        column_name = 'c_name'

    elif(table_name == 'bonds'):

        column_name = 'Name'

    elif(table_name == 'bonds_v2'):

        column_name = 'client_name'

    elif(table_name == 'vested'):

        column_name = 'Client_Name'

    elif(table_name == 'unify'):

        column_name = 'Name'

    elif(table_name == 'icici_pms'):

        column_name = 'clientname'

    elif(table_name == 'ask_pms'):

        column_name = 'CLIENT_NAME'

    elif(table_name == 'hbits'):

        column_name = 'name_of_the_investor'

    # elif(table_name == 'insurance_icici'):

    #     column_name = 'Customer_Full_Name'

    # elif(table_name == 'insurance_max_bupa'):

    #     column_name = 'Full_Name'

    elif(table_name == 'insurance_v2'):

        column_name = 'ClientName'

    elif(table_name == 'fixed_deposit'):

        column_name = 'Customer_Name'

    elif(table_name == 'strata'):

        column_name = 'name_on_pan'

    # elif(table_name == 'insurance_bse'):

    #     column_name = 'Customer_First_Name'
        
    # elif(table_name == 'insurance_hdfc'):

    #     column_name = 'Client_Name'



    if table_name == 'equity_master':
        # category_update_query_by_condition = f"""
        # UPDATE `elegant-tendril-399501.winrich_dev_v2.equity_master`
        # SET category = CASE
        #     WHEN LOWER(symbol) LIKE '%gold%' OR LOWER(symbol) LIKE '%sgb%' OR LOWER(symbol) LIKE '%silver%' THEN 'G'
        #     WHEN (REGEXP_CONTAINS(LOWER(symbol), '[a-z]') AND REGEXP_CONTAINS(LOWER(symbol), '[0-9]')) THEN 'B'
        #     ELSE 'E'
        # END
        # WHERE TRUE
        # """
        
        category_update_query_by_condition = f"""
        UPDATE `elegant-tendril-399501.winrich_dev_v2.equity_master` AS em
        SET category = COALESCE(ec.category, 'E')
        FROM `elegant-tendril-399501.winrich_dev_v2.equity_category` AS ec
        WHERE LOWER(em.symbol) = LOWER(ec.symbol);
        """

        # Execute the category_update_by_condition SQL query
        query_job_category1 = client.query(category_update_query_by_condition)
        query_job_category1.result()  # Waits for the query to finish
        print(category_update_query_by_condition)
        print("Category based on the conditions updated successfully.")


        new_symbol_insert_into_equity_category = f"""
        INSERT INTO `elegant-tendril-399501.winrich_dev_v2.equity_category` (symbol, category)
            SELECT DISTINCT
                em.symbol,
                CAST(NULL AS STRING) AS category
            FROM `elegant-tendril-399501.winrich_dev_v2.equity_master` em
            LEFT JOIN `elegant-tendril-399501.winrich_dev_v2.equity_category` ec
                ON LOWER(em.symbol) = LOWER(ec.symbol)
            WHERE em.symbol IS NOT NULL
            AND ec.symbol IS NULL;
        """

        # Execute the new_symbol_insert_into_equity_category SQL query
        query_job_category2 = client.query(new_symbol_insert_into_equity_category)
        query_job_category2.result()  # Waits for the query to finish
        print(new_symbol_insert_into_equity_category)
        print("new Symbols inserted *into equity_category successfully.")

        category_update_by_exception_list = f"""
            
            UPDATE `elegant-tendril-399501.winrich_dev_v2.{table_name}` AS em
            SET category = (
                SELECT excep_list.category
                FROM `elegant-tendril-399501.winrich_dev_v2.equity_master_category_exception_list` AS excep_list
                WHERE LOWER(REPLACE(TRIM(em.symbol), ' ', '')) = LOWER(REPLACE(TRIM(excep_list.symbol), ' ', ''))
                AND excep_list.category IS NOT NULL AND excep_list.category <> ''
            )
            WHERE EXISTS (
                SELECT 1
                FROM `elegant-tendril-399501.winrich_dev_v2.equity_master_category_exception_list` AS excep_list
                WHERE LOWER(REPLACE(TRIM(em.symbol), ' ', '')) = LOWER(REPLACE(TRIM(excep_list.symbol), ' ', ''))
            );

        """

        # Execute the category_update_by_exception_list SQL query
        query_job_category2 = client.query(category_update_by_exception_list)
        query_job_category2.result()  # Waits for the query to finish
        print(category_update_by_exception_list)
        print("Category based on the exception list updated successfully.")

        # DELETE the data that belongs to category-B (bonds) for current date

        # Since we're getting bonds data from Golden Pie csv file & from the equity file we've only equity (category != 'G') & gold data (category = 'G')
        # delete_bonds_query = """
        # DELETE FROM `elegant-tendril-399501.winrich_dev_v2.equity_master`
        # WHERE category = 'B' AND DATE(created_at) = current_date();
        # """

        # query_job_delete_bonds = client.query(delete_bonds_query)
        # query_job_delete_bonds.result()
        # print("bonds data deleted successfully from equity table!")
    
    if table_name == 'bonds_v2':
        delete_mismatched_equity_data_from_bonds_v2(client)

    
    # First SQL query
    query1 = f"""
    UPDATE `elegant-tendril-399501.winrich_dev_v2.{table_name}` AS fd
    SET master_customer_id = (
        SELECT ons.master_customer_id
        FROM `elegant-tendril-399501.winrich_dev_v2.other_names` AS ons
        WHERE LOWER(REPLACE(TRIM(fd.{column_name}), ' ', '')) = LOWER(REPLACE(TRIM(ons.other_names), ' ', '')) 
    )
    WHERE fd.master_customer_id is null;
    """
    
    # Second SQL query
    query2 = f"""
        UPDATE `elegant-tendril-399501.winrich_dev_v2.{table_name}` AS fd
        SET master_customer_id = (
            SELECT mcd.master_customer_id
            FROM `elegant-tendril-399501.winrich_dev_v2.master_customers_data` AS mcd
            WHERE LOWER(REPLACE(TRIM(fd.{column_name}), ' ', '')) = LOWER(REPLACE(TRIM(mcd.master_username), ' ', '')) 
        )
        WHERE fd.master_customer_id is null;
    """


    # Third SQL Query
    query3 = """
    DELETE FROM elegant-tendril-399501.winrich_dev_v2.sip_master
    WHERE c_name = 'nan' AND
    month1 = 0.0
    AND month2 = 0.0
    AND month3 = 0.0
    AND month4 = 0.0
    AND month5 = 0.0
    AND month6 = 0.0
    AND month7 = 0.0
    AND month8 = 0.0
    AND month9 = 0.0
    AND month10 = 0.0
    AND month11 = 0.0
    AND month12 = 0.0;
    """

    # Fourth SQL Query
    query4 = """
    DELETE FROM `elegant-tendril-399501.winrich_dev_v2.icici_pms`
    where clientcode = 'nan' and clientname = 'nan' and productcode = 'nan' and pan = 'nan';
    """

    # Fourth SQL Query
    query5 = """
    DELETE FROM `elegant-tendril-399501.winrich_dev_v2.equity_master` AS fd
    WHERE LOWER(REPLACE(TRIM(fd.symbol), ' ', '')) IN (
    SELECT LOWER(REPLACE(TRIM(mcd.symbol), ' ', ''))
    FROM `elegant-tendril-399501.winrich_dev_v2.unused_equity_symbols` AS mcd
    ) AND DATE(created_at) = current_date();
    """

    # Execute the first SQL query
    print("query1", query1)
    query_job1 = client.query(query1)
    query_job1.result()  # Waits for the query to finish
    print("First data mapping successful.")

    # Execute the second SQL query
    print("query2", query2)
    query_job2 = client.query(query2)
    query_job2.result()  # Waits for the query to finish
    print("Second data mapping successful.")


    if(table_name == 'sip_master'):

        # Execute the Third SQL query
        query_job3 = client.query(query3)
        query_job3.result()  # Waits for the query to finish
        print("Null Rows Deleted From SIP Master")

    if(table_name == 'icici_pms'):

        # Execute the Fourth SQL query
        query_job4 = client.query(query4)
        query_job4.result()  # Waits for the query to finish
        print("Null Rows Deleted From icici_pms")

    if(table_name == 'equity_master'):

        # Execute the Fourth SQL query
        # query_job5 = client.query(query5)
        # query_job5.result()  # Waits for the query to finish
        # print("Rows having the unused symbols Deleted From equity_master table")
        print('')


def get_table_schema(table_name):
    print('under table_schema')
    client = bigquery.Client()
    dataset_ref = client.dataset('winrich_dev_v2')
    table_ref = dataset_ref.table(table_name)
    table = client.get_table(table_ref)

    print('table: ', table)
    
    schema = [(field.name, field.field_type) for field in table.schema]

    print('schema: ', schema)
    return schema

def is_event_processed(event_id, client):
    query = f"""
        SELECT COUNT(*) as count FROM `elegant-tendril-399501.winrich_dev_v2.gcs_bq_data_transfer_status_tracker`
        WHERE Event_ID = '{event_id}'
    """
    result = client.query(query).result()
    for row in result:
        return row["count"] > 0
    return False

def insert_gold_bonds_into_equity_master(gold_data, client):
    table_id = "elegant-tendril-399501.winrich_dev_v2.equity_master"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,                # TODO: replace with explicit schema when stable
        ignore_unknown_values=True,      # skips extra keys in your dicts
        max_bad_records=0
    )

    try:
        load_job = client.load_table_from_json(
            gold_data, table_id, job_config=job_config, location="US"
        )
        load_job.result()  # wait until committed
        print(f"Inserted {len(gold_data)} rows via LOAD job.")
    except Exception as e:
        print("Load job failed:", e)
        raise

    # Safe to run DML now (no streaming buffer)
    execute_sql_queries("equity_master")

# Insert Non Angel one equity customer- stock data into equity master 
def insert_from_winwize_into_equity_master(client):
    table_id = "elegant-tendril-399501.winrich_dev_v2.equity_master"
    api_url = "http://52.27.234.146:8000/api/method/stock_portfolio_management.portfolio_master.doctype.customer_winwize_stocks.api.fetch_winwize_equity_stocks"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,                # TODO: replace with explicit schema when stable
        ignore_unknown_values=True,      # skips extra keys in your dicts
        max_bad_records=0
    )

    try:
        resp = requests.get(api_url, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        print("Winwize equity API response payload:", payload)

        # Support wrapper {"message": {"status":"success","data":[...]}} or {"data": [...]} or plain list/dict
        records = []
        if isinstance(payload, list):
            records = payload
        elif isinstance(payload, dict):
            # nested under message.data
            if "message" in payload and isinstance(payload["message"], dict) and "data" in payload["message"]:
                records = payload["message"].get("data", [])
            elif "data" in payload:
                records = payload.get("data", [])
            else:
                # treat the dict as a single record
                records = [payload]
        else:
            print("Unexpected payload type from API; aborting push.", payload)
            return
        
        if not isinstance(records, list):
            print("Winwize equity API did not return a list; aborting push.")
            return
        
        records = [r if isinstance(r, dict) else dict(r) for r in records]
        if not records:
            print("API returned empty list; nothing to push.")
            return
        
        load_job = client.load_table_from_json(
            records, table_id, job_config=job_config, location="US"
        )
        load_job.result()  # wait until committed
        print(f"Inserted {len(records)} rows via insert_from_winwize_into_equity_master LOAD job.")
    except Exception as e:
        print("insert_from_winwize_into_equity_master Load job failed:", e)
        raise

# Clean & fast match finder between client dp holdings and gold_bonds_isin_scripnames
def normalize(value: str) -> str:
    """Normalize strings for comparison — lower, strip, and remove spaces."""
    return str(value).strip().lower().replace(" ", "")

def process_gold_bonds_from_holdings(client_dp_holdings_arr, client):
    query = f"""
        SELECT * FROM `elegant-tendril-399501.winrich_dev_v2.bonds_v2_data_mismatch` WHERE type = 'gold bonds'; 
    """

    query_job = client.query(query)
    rows = query_job.result()  # Wait for query to complete

    # Convert each row (Row object) to a Python dict
    gold_bonds_isin_scripnames = [dict(row) for row in rows]

    # 1️⃣ Create lookup set from result_list (80 records)
    keyset = {
        (normalize(r["isin"]), normalize(r["scrip_name"]))
        for r in gold_bonds_isin_scripnames
    }

    # 2️⃣ Filter matches from client_dp_holdings_data (10k rows)
    gold_data = [
        row for row in client_dp_holdings_arr
        if (normalize(row["isin"]), normalize(row["scrip_name"])) in keyset
    ]

    print(gold_data)

    # 3️⃣ Display results
    print(f"✅ Total matched rows: {len(gold_data)}")

    insert_gold_bonds_into_equity_master(gold_data, client)

def process_unlisted_esops_equity(client_dp_holdings_arr, client):


    query = f"""
            DELETE FROM `elegant-tendril-399501.winrich_dev_v2.equity_master`
            WHERE Date(created_at) = current_date() AND isin in (
                SELECT distinct isin FROM `elegant-tendril-399501.winrich_dev_v2.unlisted_esops_equity`
            );
    """


    print(f'query for deleting already processed unlisted_esops_equity data')
    print(query)

    # Execute the Fourth SQL query
    query_job = client.query(query)
    query_job.result()  # Waits for the query to finish
    print("deleted the already processed today's of unlisted esops data & we're processing a new file")


    query = f"""
        SELECT * FROM `elegant-tendril-399501.winrich_dev_v2.unlisted_esops_equity`; 
    """

    query_job = client.query(query)
    rows = query_job.result()  # Wait for query to complete

    # Convert each row (Row object) to a Python dict
    unlisted_isin_scripnames = [dict(row) for row in rows]

    # 1️⃣ Create lookup set from result_list (80 records)
    keyset = {
        (normalize(r["isin"]), normalize(r["scrip_name"]))
        for r in unlisted_isin_scripnames
    }

    # 2️⃣ Filter matches from client_dp_holdings_data (10k rows)
    unlisted_isin_scripnames_data = [
        row for row in client_dp_holdings_arr
        if (normalize(row["isin"]), normalize(row["scrip_name"])) in keyset
    ]

    print(unlisted_isin_scripnames_data)

    # 3️⃣ Display results
    print(f"✅ Total unlisted_isin_scripnames_data matched rows: {len(unlisted_isin_scripnames_data)}")

    table_id = "elegant-tendril-399501.winrich_dev_v2.equity_master"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,                # TODO: replace with explicit schema when stable
        ignore_unknown_values=True,      # skips extra keys in your dicts
        max_bad_records=0
    )

    try:
        load_job = client.load_table_from_json(
            unlisted_isin_scripnames_data, table_id, job_config=job_config, location="US"
        )
        load_job.result()  # wait until committed
        print(f"Inserted {len(unlisted_isin_scripnames_data)} rows via unlisted_esops_equity LOAD job.")

        # Map customer id for unlisted unnlisted esops

        # First SQL query
        query1 = f"""
            UPDATE `elegant-tendril-399501.winrich_dev_v2.equity_master` AS fd
            SET master_customer_id = (
                SELECT mcd.master_customer_id
                FROM `elegant-tendril-399501.winrich_dev_v2.master_customers_data` AS mcd
                WHERE LOWER(REPLACE(TRIM(fd.h_name), ' ', '')) = LOWER(REPLACE(TRIM(mcd.master_username), ' ', '')) 
            )
            WHERE fd.master_customer_id is null AND isin in (
                SELECT distinct isin FROM `elegant-tendril-399501.winrich_dev_v2.unlisted_esops_equity`
            );
        """
        # Second SQL query
        query2 = f"""
            UPDATE `elegant-tendril-399501.winrich_dev_v2.equity_master` AS fd
            SET master_customer_id = (
                SELECT ons.master_customer_id
                FROM `elegant-tendril-399501.winrich_dev_v2.other_names` AS ons
                WHERE LOWER(REPLACE(TRIM(fd.h_name), ' ', '')) = LOWER(REPLACE(TRIM(ons.other_names), ' ', '')) 
            )
            WHERE fd.master_customer_id is null AND isin in (
                SELECT distinct isin FROM `elegant-tendril-399501.winrich_dev_v2.unlisted_esops_equity`
            );
        """

        # Execute the first SQL query
        print("query1", query1)
        query_job1 = client.query(query1)
        query_job1.result()  # Waits for the query to finish
        print("First data of unlisted esops mapping successful.")

        # Execute the second SQL query
        print("query2", query2)
        query_job2 = client.query(query2)
        query_job2.result()  # Waits for the query to finish
        print("Second data of unlisted esops mapping successful.")


        # Category mapping for unlisted esops

        category_update_query_by_condition = f"""
        UPDATE `elegant-tendril-399501.winrich_dev_v2.equity_master` AS em
        SET category = COALESCE(ec.category, 'E')
        FROM `elegant-tendril-399501.winrich_dev_v2.equity_category` AS ec
        WHERE LOWER(em.symbol) = LOWER(ec.symbol) AND isin in (
            SELECT distinct isin FROM `elegant-tendril-399501.winrich_dev_v2.unlisted_esops_equity`
        );
        """

        # Execute the category_update_by_condition SQL query
        query_job_category1 = client.query(category_update_query_by_condition)
        query_job_category1.result()  # Waits for the query to finish
        print(category_update_query_by_condition)
        print("Category based on the conditions updated for unlisted esops successfully.")

    except Exception as e:
        print("Load job of unlisted_esops_equity failed:", e)
        raise

    



def delete_mismatched_equity_data_from_bonds_v2(client):

    # client = bigquery.Client()

    # query = f"""
    #             DELETE FROM `elegant-tendril-399501.winrich_dev_v2.bonds_v2`
    #                 WHERE LOWER(REPLACE(TRIM(isin), ' ', '')) IN (
    #                 SELECT LOWER(REPLACE(TRIM(isin), ' ', ''))
    #                 FROM `elegant-tendril-399501.winrich_dev_v2.bonds_v2_data_mismatch`
    #                 ) AND Date(created_at) = current_date();
    #     """

    query = """
        DELETE FROM `elegant-tendril-399501.winrich_dev_v2.bonds_v2` t
        WHERE EXISTS (
            SELECT 1 
            FROM `elegant-tendril-399501.winrich_dev_v2.bonds_v2_data_mismatch` m
            WHERE LOWER(REPLACE(TRIM(t.isin), ' ', '')) = LOWER(REPLACE(TRIM(m.isin), ' ', ''))
        )
        AND DATE(t.created_at) = CURRENT_DATE();
    """

    print(query)

    # Execute the first SQL query
    query_job = client.query(query)
    query_job.result(timeout=900) # Waits for the query to finish

    print('**********************************')
    print('mismatched equity data has been delete requested successfully')

def delete_mismatched_bonds_data():

    client = bigquery.Client()

    query = f"""
                DELETE
                    FROM 
                    `elegant-tendril-399501.winrich_dev_v2.equity_master` AS nc
                    WHERE 
                    EXISTS (
                        SELECT 
                        1
                        FROM 
                        `elegant-tendril-399501.winrich_dev_v2.equity_data_mismatch` AS mcd
                        WHERE 
                        LOWER(REPLACE(TRIM(nc.symbol), ' ', '')) = LOWER(REPLACE(TRIM(mcd.Symbol), ' ', ''))
                    ) AND Date(created_at) = current_date();
        """

                        #     LOWER(REPLACE(TRIM(nc.clientcode), ' ', '')) = LOWER(REPLACE(TRIM(mcd.Client_Code), ' ', '')) AND
                        # LOWER(REPLACE(TRIM(nc.h_name), ' ', '')) = LOWER(REPLACE(TRIM(mcd.Client_Name), ' ', '')) AND 

    print(query)

    # Execute the first SQL query
    query_job = client.query(query)
    query_job.result()  # Waits for the query to finish

    print('**********************************')
    print('mismatched bonds data has been deleted successfully')

@functions_framework.cloud_event
def hello_gcs(cloud_event):

    data = cloud_event.data

    client = bigquery.Client()

    # Extract event metadata
    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    # Extract data fields
    bucket = data["bucket"]
    file_name = data["name"]
    metageneration = data.get("metageneration")
    timeCreated = data.get("timeCreated")
    updated = data.get("updated")

    if is_event_processed(event_id, client):
        print(f"Event {event_id} already processed. Skipping.")
        return jsonify({'status': 200, 'msg': 'Duplicate event skipped'}), 200

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {file_name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")

    # file_name = event['name'].lower()
    file_name = data["name"]
    print('file_name: ', file_name)

    dataset_ref = client.dataset('winrich_dev_v2')
    tables = client.list_tables(dataset_ref)
    table_names = [table.table_id for table in tables]

    # table_name = next((table for table in table_names if table in file_name), file_name.split('.')[0])

    print('%%%%%%%%%%%%%%%%%%%%%%%%%')

    normalized_file_name = file_name.lower()

    if 'deposit' in normalized_file_name:
        table_name = 'fixed_deposit'
    elif 'sipmom' in normalized_file_name:
        table_name = 'sip_master'
    elif 'client-dp-holdings' in normalized_file_name:
        table_name = 'bonds_v2'
    elif 'equity' in normalized_file_name:
        table_name = 'equity_master'
    elif 'golden' in normalized_file_name:
        table_name = 'bonds'
    elif 'liqui' in normalized_file_name:
        table_name = 'liquiloans_master'
    elif 'mutual' in normalized_file_name or 'winwizemf' in normalized_file_name:
        table_name = 'mutualfunds_master'
    elif 'strata' in normalized_file_name:
        table_name = 'strata'
    elif 'wawya_daily' in normalized_file_name:
        table_name = 'unify'
    elif 'ask' in normalized_file_name or 'ask_pms' in normalized_file_name:
        table_name = 'ask_pms'
    elif 'pms' in normalized_file_name or 'icici_pms' in normalized_file_name:
        table_name = 'icici_pms'
    elif 'hbits' in normalized_file_name:
        table_name = 'hbits'
    elif 'insurance' in normalized_file_name:
        table_name = 'insurance_v2'
    # elif 'icici' in normalized_file_name:
    #     table_name = 'insurance_icici'
    # elif 'max_bupa' in normalized_file_name:
    #     table_name = 'insurance_max_bupa'
    # elif 'bse' in normalized_file_name:
    #     table_name = 'insurance_bse'
    # elif 'hdfc' in normalized_file_name:
    #     table_name = 'insurance_hdfc'
    elif 'vested' in normalized_file_name or 'funded' in normalized_file_name or 'accounts' in normalized_file_name:
        table_name = 'vested'

    else:
        # table_name = normalized_file_name.split('.')[0]
        print('This function only process all the financial products csv files')
        return None


    print(f'File: {file_name}, Table: {table_name}')
    current_time = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")

    print('current time: ', current_time)
    print('type: ', type(current_time))

    try:
        file_path = f'/tmp/{os.path.dirname(file_name)}'
        os.makedirs(file_path, exist_ok=True)
        file_path = f'/tmp/{file_name}'

        print('file_path:', file_path)
        download_file_from_gcs(bucket, file_name, file_path)

        file_data = pd.read_csv(file_path)
        file_data = file_data.map(lambda x: x.strip().replace('\t', '') if isinstance(x, str) else x)
        # file_data = file_data.applymap(lambda x: x.strip().replace('\t', '') if isinstance(x, str) else x)
        print("CSV file loaded successfully")

        print(f"Processing file: {file_name}")
        print(f"Associated table: {table_name}")

        file_data['created_at'] = datetime.datetime.now()
        file_data['updated_at'] = datetime.datetime.now()

        # print('time from datetime: ', datetime.datetime.now())

        print('created_at: ', current_time)

        table_schema = get_table_schema(table_name)
        print("Schema for table {}: {}".format(table_name, table_schema))

        # print('before data types: ', file_data.dtypes)

        # Strip leading and trailing spaces from column names

        file_data.columns = file_data.columns.str.strip()

        # Checking whether the today's file is already processed or not

        # Perform a query
        # if  table_name == 'equity_master':
        #     is_file_already_processed_query = f"""
        #             select * from `elegant-tendril-399501.winrich_dev_v2.{table_name}`
        #             where Date(created_at) = current_date() AND category = 'E';
        #     """
        # else:

        is_file_already_processed_query = f"""
                select * from `elegant-tendril-399501.winrich_dev_v2.{table_name}`
                where Date(created_at) = current_date()
        """

        # special Handling for direct mutual-funds
        if 'winwizemf' in normalized_file_name:
            is_file_already_processed_query += " AND fund_type = 'direct'"
        elif table_name == 'mutualfunds_master':
            is_file_already_processed_query += " AND fund_type != 'direct'"
        elif table_name == 'equity_master':
            is_file_already_processed_query += " AND type != 'gold bonds'"

        query_job = client.query(is_file_already_processed_query)
        print(f'is already processed query: {is_file_already_processed_query}')
        print(f'query job: {query_job}')

        data_from_corresponding_table = []

        # Process query results
        for row in query_job:

            data_from_corresponding_table.append(row)

        print(f'data_from_corresponding_table: {data_from_corresponding_table}')


        if(data_from_corresponding_table and data_from_corresponding_table is not None and len(data_from_corresponding_table) > 0):
            print('The file: ' + str(file_name) + ' is already processed')

            query = f"""
                DELETE FROM `elegant-tendril-399501.winrich_dev_v2.{table_name}`
                WHERE Date(created_at) = current_date()
            """

            # special Handling for direct mutual-funds  
            if 'winwizemf' in normalized_file_name:
                query += " AND fund_type = 'direct'"
            elif table_name == 'mutualfunds_master':
                query += " AND fund_type != 'direct'"
            elif table_name == 'equity_master':
                query += " AND type != 'gold bonds'"


            print(f'query for deleting already processed data')
            print(query)

            # Execute the Fourth SQL query
            query_job = client.query(query)
            query_job.result()  # Waits for the query to finish
            print("deleted the already processed today's data & we're processing a new file")
        
        print('Need to process the data!')

        if table_name == 'mutualfunds_master':

            print('inside mutual funds')

            column_mappings = {
                'sCode': 's_code',
                'Nature': 'nature',
                'Email': 'email',
                'Mobile': 'mobile',
                'FolioStartDate': 'folio_start_date',
                'AvgCost': 'avg_cost',
                'InvAmt': 'inv_amt',
                'TotalInvAmt': 'total_inv_amt',
                'CurNAV': 'cur_nav',
                'CurValue': 'cur_value',
                'DivAmt': 'div_amt',
                'NotionalGain': 'notional_gain',
                'ActualGain': 'actual_gain',
                'FolioXIRR': 'folio_xirr',
                'NatureXIRR': 'nature_xirr',
                'ClientXIRR': 'client_xirr',
                'NatureAbs': 'nature_abs',
                'ClientAbs': 'client_abs',
                'absReturn': 'abs_return',
                'BalUnit': 'bal_unit',
                'ValueDate': 'value_date',
                'ReportDate': 'report_date'
            }

            # Rename columns using the dictionary
            file_data.rename(columns=column_mappings, inplace=True)


            column_data_types = file_data.dtypes

            print('after data types: ', column_data_types)
            print('file data types')
            print(column_data_types)

            # Define column mappings
            # column_mappings = {'sCode': 's_code'}

            # Rename columns
            # file_data.rename(columns=column_mappings, inplace=True)

            # List of columns to be converted to string data type
            str_columns = ['h_name', 'c_name', 's_code', 's_name', 'foliono', 'nature', 'folio_start_date', 'bal_unit' , 'email', 'mobile', 'value_date', 'report_date']

            # List of columns to be converted to float data type
            float_columns = ['avg_cost', 'inv_amt', 'total_inv_amt', 'cur_nav', 'cur_value', 'div_amt', 'notional_gain', 'actual_gain', 'folio_xirr', 'nature_xirr', 'client_xirr', 'nature_abs', 'client_abs', 'abs_return']

            # Convert columns to string data type
            file_data[str_columns] = file_data[str_columns].astype(str)

            # Convert columns to float data type
            file_data[float_columns] = file_data[float_columns].astype(float)

            if 'winwizemf' in normalized_file_name:
                file_data['fund_type'] = 'direct'

            # print('file data columns: ', file_data)

            # Convert 'FolioStartDate', 'ValueDate', and 'ReportDate' columns to datetime data type
            # date_columns = ['created_at', 'updated_at']
            # file_data[date_columns] = file_data[date_columns].apply(pd.to_datetime)


        elif(table_name == 'liquiloans_master'):

            print('Inside the liquiloans block')

            # # Strip leading and trailing spaces from column names

            # file_data.columns = file_data.columns.str.strip()

            file_data['investori'] = file_data['investori'].astype(int)

            print('file data name: ', file_data['name'])

            file_data.rename(columns={'annualized_return': 'annualized_return_'}, inplace=True)

            file_data['name'] = file_data['name'].astype(str)

            # Convert current_value column to float, coercing errors
            file_data['current_value'] = pd.to_numeric(file_data['current_value'], errors='coerce')
            
            # Optionally, fill NaN values with a default value, e.g., 0.0
            file_data['current_value'].fillna(0.0, inplace=True)

            # Convert current_value column to float, coercing errors
            file_data['annualized_return_'] = pd.to_numeric(file_data['annualized_return_'], errors='coerce')
            
            # Optionally, fill NaN values with a default value, e.g., 0.0
            file_data['annualized_return_'].fillna(0.0, inplace=True)


        elif(table_name == 'sip_master'):

            timezone = pytz.timezone('Asia/Kolkata')
            current_time = timezone.localize(datetime.datetime.now().replace(microsecond=0))

            columns_to_convert = ['c_name', 'Month1', 'Month2', 'Month3', 'Month4', 'Month5', 'Month6', 'Month7', 'Month8', 'Month9', 'Month10', 'Month11', 'Month12']

            # Keep only the specified columns
            file_data = file_data[columns_to_convert]

            for column in columns_to_convert:
                if column == 'c_name':
                    file_data[column] = file_data[column].astype(str)
                else:
                    file_data[column] = file_data[column].astype(float)

            file_data["created_at"] = current_time
            file_data["updated_at"] = current_time


        elif(table_name == 'bonds'):

            print('under bonds')

            # Keep only the relevant columns
            columns_to_keep = ['Name', 'Email', 'Phone', 'Order Date', 'Issuer Name', 'Coupon', 'Maturity Date', 'Units', 'Price', 'Investment Amount']
            file_data = file_data[columns_to_keep]

            file_data.rename(columns={
                'Name': 'Name',
                'Email': 'Email',
                'Phone': 'Phone',
                'Order Date': 'Order_Date',
                'Issuer Name': 'Issuer_Name',
                'Coupon': 'Coupon',
                'Maturity Date': 'Maturity_Date',
                'Units': 'Units',
                'Price': 'Price',
                'Investment Amount': 'Investment_Amount',
            }, inplace=True)

            # Convert columns to appropriate types
            file_data['Phone'] = pd.to_numeric(file_data['Phone'], errors='coerce').fillna(0).astype(int)
            file_data['Units'] = pd.to_numeric(file_data['Units'], errors='coerce').fillna(0).astype(int)
            file_data['Coupon'] = pd.to_numeric(file_data['Coupon'], errors='coerce').fillna(0.0).astype(float)
            file_data['Price'] = pd.to_numeric(file_data['Price'], errors='coerce').fillna(0.0).astype(float)
            file_data['Investment_Amount'] = pd.to_numeric(file_data['Investment_Amount'], errors='coerce').fillna(0.0).astype(float)
            file_data['Order_Date'] = file_data['Order_Date'].astype(str)
            file_data['Issuer_Name'] = file_data['Issuer_Name'].astype(str)
            file_data['Maturity_Date'] = file_data['Maturity_Date'].astype(str)
            file_data['Name'] = file_data['Name'].astype(str)
            file_data['Email'] = file_data['Email'].astype(str)

            file_data['created_at'] = datetime.datetime.now()
            file_data['updated_at'] = datetime.datetime.now()

        elif(table_name == 'bonds_v2'):

            print('under bonds_v2')

            print(f'total records from csv: {file_data.shape[0]} or {len(file_data)}')

            # First, replace the DataFrame’s columns with the first row's values
            # file_data.columns = file_data.iloc[0]

            # # Then drop the first row (which is now redundant, as it's used as headers)
            # file_data = file_data.drop(index=0).reset_index(drop=True)

            # # Clean up column names (important!)
            file_data.columns = file_data.columns.astype(str).str.strip().str.replace('\t', '')

            # # Apply your cleanup
            # file_data = file_data.applymap(lambda x: x.strip().replace('\t', '') if isinstance(x, str) else x)
            file_data = file_data.map(lambda x: x.strip().replace('\t', '') if isinstance(x, str) else x)


            print("Cleaned headers:", list(file_data.columns))

            # print("Fixed DataFrame:")
            # print(file_data.head())

            print("First 5 rows of the DataFrame (with headers):")
            print(file_data.head().to_string())


            # # Keep only the relevant columns
            columns_to_keep = ['Client Code', 'Depository Id', 'Client Name', 'ISIN', 'Scrip Name', 'Holding Quantity', 'Free Quantity', 'Freeze Quantity', 'Pledge Quantity', 'Safe Keeping Quantity', 'Lockin Quantity', 'Value (?)']
            file_data = file_data[columns_to_keep]

            file_data.rename(columns={
                'Client Code': 'client_code',
                'Depository Id': 'depository_id',
                'Client Name': 'client_name',
                'ISIN': 'isin',
                'Scrip Name': 'scrip_name',
                'Holding Quantity': 'holding_qty',
                'Free Quantity': 'free_qty',
                'Freeze Quantity': 'freeze_qty',
                'Pledge Quantity': 'pledge_qty',
                'Safe Keeping Quantity': 'safe_keeping_qty',
                'Lockin Quantity': 'lockin_qty',
                'Value (?)': 'cur_val',
            }, inplace=True)

            # Reset the index
            file_data.reset_index(drop=True, inplace=True)

            # Convert columns to appropriate types
            file_data = file_data.astype({
                        'client_code': str,
                        'depository_id': float,
                        'client_name': str,
                        'isin': str,
                        'scrip_name': str,
                        'holding_qty': float,
                        'free_qty': float,
                        'freeze_qty': int,
                        'pledge_qty': int,
                        'safe_keeping_qty': int,
                        'lockin_qty': int,
                        'cur_val': float
                        })

            # # Filter rows where the 6th character in 'isin' is F/G/H/I/J/K (case-insensitive)
            # file_data = file_data[
            #     file_data['isin'].astype(str).str.match(r'^.{6}[F-GH-IJ-K]', case=False, na=False)
            # ]

            # file_data = file_data[
            #     file_data['isin'].astype(str).apply(lambda isin_code: (
            #         len(isin_code) > 6 and
            #         isin_code[6].lower() in 'fghk' and
            #         not isin_code.lower().startswith('inf')
            #     )) & 
            #     file_data['scrip_name'].astype(str).apply(lambda name: (
            #         'etf' not in name.lower() and
            #         not ('gold' in name.lower() and 'bees' in name.lower()) and
            #         not ('silver' in name.lower() and 'bees' in name.lower())
            #     ))
            # ]

            file_data['created_at'] = datetime.datetime.now()
            file_data['updated_at'] = datetime.datetime.now()

            copied_file_data = file_data.copy(deep=True)

            copied_file_data = copied_file_data.astype({ 'depository_id': str })

            out = (
                copied_file_data.assign(
                    h_name=lambda df: df["client_name"],
                    c_name=lambda df: df["client_name"],
                    clientcode=lambda df: df["client_code"],
                    symbol=lambda df: df["scrip_name"],
                    dpaccountholdings=lambda df: df["holding_qty"],
                    netholdings=lambda df: df["holding_qty"],
                    totalvalue=lambda df: df["cur_val"],
                    scripname=lambda df: df["scrip_name"],
                    category="G",
                    type="gold bonds",
                    created_at=current_time,
                    updated_at=current_time
                )[
                    [
                        "h_name", "c_name", "clientcode", "symbol",
                        "dpaccountholdings", "netholdings", "totalvalue", "scripname",
                        "category", "depository_id", "isin", "scrip_name", "free_qty", "type", "created_at", "updated_at"
                    ]
                ]
            )

            # Convert to list of dicts if required:
            mapped_data = out.to_dict(orient="records")

            process_gold_bonds_from_holdings(mapped_data, client)

            process_unlisted_esops_equity(mapped_data, client)


        elif(table_name == 'icici_pms'):

            print('under icici pms')

            # Find the row index where the actual data starts (assuming it starts after a specific header row)
            start_index = file_data[file_data.iloc[:, 0] == 'CLIENTCODE'].index[0]

            # Remove the unwanted header rows
            file_data = file_data.iloc[start_index:]

            # Reset the column headers
            file_data.columns = file_data.iloc[0]
            file_data = file_data[1:]

            # Keep only the relevant columns
            columns_to_keep = ['CLIENTCODE', 'CLIENTNAME', 'AUM', 'PRODUCTCODE', 'PAN']
            file_data = file_data[columns_to_keep]

            # Reset the index
            file_data.reset_index(drop=True, inplace=True)

            file_data['created_at'] = datetime.datetime.now()
            file_data['updated_at'] = datetime.datetime.now()

            # Print the cleaned DataFrame (for debugging)
            print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
            print(file_data.head())

            file_data.rename(columns={
            'CLIENTCODE': 'clientcode',
            'CLIENTNAME': 'clientname',
            'AUM': 'aum',
            'PRODUCTCODE': 'productcode',
            'PAN': 'pan'
            }, inplace=True)

            file_data = file_data.astype({
            'clientcode': str,
            'clientname': str,
            'aum': float,
            'productcode': str,
            'pan': str
            })

        elif(table_name == 'hbits'):

            # file_data.rename(columns={
            # 'Name of the Investor': 'name_of_the_investor',
            # 'Investment Amount': 'investment_amount',
            # 'SPV': 'spv',
            # 'Investment Amount LMS': 'investment_amount_lms',
            # 'Property Name': 'property_name'
            # }, inplace=True)

            # file_data = file_data.astype({
            #     'name_of_the_investor': str,
            #     'investment_amount': int,
            #     'spv': str,
            #     'investment_amount_lms': int,
            #     'property_name': str
            # })

            # Rename columns
            file_data.rename(columns={
                'Name of the Investor': 'name_of_the_investor',
                'Investment Amount': 'investment_amount',
                'SPV': 'spv',
                'Investment Amount LMS': 'investment_amount_lms',
                'Property Name': 'property_name'
            }, inplace=True)

            # Remove commas and convert to numeric
            file_data['investment_amount'] = file_data['investment_amount'].replace({',': ''}, regex=True).astype(int)
            file_data['investment_amount_lms'] = file_data['investment_amount_lms'].replace({',': ''}, regex=True).astype(int)

            # Convert other columns to appropriate types
            file_data = file_data.astype({
                'name_of_the_investor': str,
                'spv': str,
                'property_name': str
            })

        # elif(table_name == 'insurance_icici'):

        #     file_data.rename(columns={
        #     'Policy No': 'Policy_No',
        #     'Customer Full Name': 'Customer_Full_Name',
        #     'Product Name': 'Product_Name',
        #     'Mobile No': 'Mobile_No',
        #     'Due Date': 'Due_Date',
        #     'Risk Com Date': 'Risk_Com_Date',
        #     'Issuance Date': 'Issuance_Date',
        #     'Premium Paying Term': 'Premium_Paying_Term',
        #     'Premium Amount': 'Premium_Amount',
        #     'Sum Assured': 'Sum_Assured',
        #     'Bill Channel': 'Bill_Channel',
        #     'Suspense Account Balance': 'Suspense_Account_Balance',
        #     'Net Amount Due': 'Net_Amount_Due',
        #     'City': 'City____________',
        #     'Phone1': 'Phone1________',
        #     'Phone2': 'Phone2_______',
        #     'Email': 'Email',
        #     'Payment Frequency': 'Payment_Frequency'
        #     }, inplace=True)

        #     # converting the Due_Date and Issuance_Date to be in date time ( timestamp ) type

        #     file_data['Due_Date'] = pd.to_datetime(file_data['Due_Date'])

        #     file_data['Issuance_Date'] = pd.to_datetime(file_data['Issuance_Date'])

        #     file_data['Risk_Com_Date'] = pd.to_datetime(file_data['Risk_Com_Date'], format='%d-%b-%Y').dt.strftime('%Y-%m-%d')

        #     # List of columns to be converted to string data type
        #     str_columns = ['Policy_No', 'Customer_Full_Name', 'Product_Name', 'Bill_Channel', 'City____________', 'Phone1________', 'Phone2_______', 'Email' , 'Payment_Frequency']

        #     # List of columns to be converted to int data type
        #     int_columns = ['Mobile_No', 'Premium_Paying_Term', 'Premium_Amount', 'Sum_Assured', 'Suspense_Account_Balance', 'Net_Amount_Due']

        #     # Convert columns to string data type
        #     file_data[str_columns] = file_data[str_columns].astype(str)

        #     # Convert columns to int data type
        #     file_data[int_columns] = file_data[int_columns].astype(int)

        # elif table_name == 'insurance_max_bupa':

        #     print('under max_bupa')

        #     # Remove the "First Name" column if it exists in the DataFrame
        #     if 'First Name' in file_data.columns:
        #         file_data.drop(columns=['First Name'], inplace=True)

        #     if 'Last Name' in file_data.columns:
        #         file_data.drop(columns=['Last Name'], inplace=True)

        #     # Define the column mapping
        #     column_mapping = {
        #         'Full Name': 'Full_Name',
        #         'Application Number': 'Application_No',
        #         'Previous Policy Number': 'Old_Policy_Number',
        #         'Policy Number': 'New_Policy_Number',
        #         'Customer ID': 'Customer_ID',
        #         'DOB': 'DOB',
        #         'Plan Type': 'Plan_Type',
        #         'Product ID': 'Product_ID',
        #         'Product Genre': 'Product_Genre',
        #         'Insured Lives': 'Insured_Lives',
        #         'Insured Count': 'Insured_Count',
        #         'Renewal Premium': 'Renewal_Premium',
        #         '2 Years Renewal Premium (With Tax)': '_2_Years_Renewal_Premium__With_Tax_',
        #         'Current Status': 'Current_Status_Workstep',
        #         'Issued Premium': 'Issued_Premium',
        #         'Renewal Premium (Without Taxes)': 'Renewal_Premium__Without_Taxes_',
        #         'Loading Premium': 'Loading_Premium',
        #         'Sum Assured': 'Sum_Assured',
        #         'Individual Sum Assured': 'Individual_Sum_Assured',
        #         'Health Assurance Critical Illness/Criticare Sum Assured': 'Health_Assurance_Critical_Illness_Criticare_Sum_Assured',
        #         'Health Assurance Personal Accident/Accident Care Sum Assured': 'Health_Assurance_Personal_Accident_Accident_Care_Sum_Assured',
        #         'Health Assurance Hospital Cash/Hospicash Sum Assured': 'Health_Assurance_Hospital_Cash_Hospicash_Sum_Assured',
        #         'Login Branch': 'Branch',
        #         'Sales Branch': 'Sales_Branch',
        #         'Zone': 'Zone',
        #         'Renewal Channel': 'Renewal_Channel',
        #         'Renewal Sub Channel': 'Renewal_Sub_Channel',
        #         'Renewal Agent Code': 'Renewal_Agent_Code',
        #         'Renewal Agent Name': 'Renewal_Agent_Name',
        #         'Renewal Agent Type': 'Renewal_Agent_Type',
        #         'Pa Code': 'Pa_Code',
        #         'Conversion Date': 'Conversion_Date',
        #         'Agency Manager ID': 'Agency_Manager_ID',
        #         'Agency Manager Name': 'Agency_Manager_Name',
        #         'Renewal Logged Date': 'Renewal_Logged_Date',
        #         'Renewal Logged Month': 'Renewal_Logged_Month',
        #         'Renewal Issued Date': 'Renewal_Issued_Date',
        #         'Renewal Issued Month': 'Renewal_Issued_Month',
        #         'Maximus Status': 'Maximus_Status',
        #         'Lead Status': 'Lead_Status',
        #         'Sales Status': 'Sales_Status',
        #         'Hums Status': 'Hums_Status',
        #         'Hums Status Update Date': 'Hums_Status_Update_Date',
        #         'Current Team': 'Current_Team',
        #         'Current Status Ageing': 'Current_Status_Ageing',
        #         'Login Ageing': 'Login_Ageing',
        #         'Designation': 'Designation',
        #         'Policy Start Date': 'Policy_Start_Date',
        #         'Policy Expiry Date': 'Policy_Expiry_Date',
        #         'Is Portability': 'Is_Portability',
        #         'Is Split': 'Split_Flag',
        #         'Is Upsell': 'Upsell_Eligibility',
        #         'Upsell Limit': 'Upsell_Limit',
        #         'Plan Name': 'Plan_Name',
        #         'Renew Now': 'Renew_Now',
        #         'Whatsapp Communication for Policy Information': 'Whatsapp_Communication_for_Policy_Information',
        #         'Communication Acknowledgement(Over Ride DND)': 'Communication_Acknowledgement_Over_Ride_DND_',
        #         'Safe Guard': 'Safeguard_Rider_Taken',
        #         'Policy Tenure': 'Policy_Tenure',
        #         'Product Name': 'Product_Name'
        #     }

        #     # Filter the column mapping to include only columns present in the DataFrame
        #     filtered_column_mapping = {k: v for k, v in column_mapping.items() if k in file_data.columns}

        #     # Rename the columns
        #     file_data.rename(columns=filtered_column_mapping, inplace=True)

        #     # List of columns to be converted to int data type
        #     int_columns = [
        #         "Application_No","Old_Policy_Number","Customer_ID","Product_ID","Insured_Lives","Renewal_Premium","_2_Years_Renewal_Premium__With_Tax_","Loading_Premium","Sum_Assured","Individual_Sum_Assured","Health_Assurance_Personal_Accident_Accident_Care_Sum_Assured","Health_Assurance_Hospital_Cash_Hospicash_Sum_Assured","Whatsapp_Communication_for_Policy_Information"
        #     ]

        #     # Filter the list to include only columns present in the DataFrame
        #     existing_int_columns = [col for col in int_columns if col in file_data.columns]

        #     # Replace non-finite values with 0
        #     file_data[existing_int_columns] = file_data[existing_int_columns].replace([np.inf, -np.inf, np.nan], 0)


        #     # List of columns to be converted to string data type
        #     str_columns = [
        #         "Full_Name",
        #         "New_Policy_Number",
        #         "Plan_Type",
        #         "Product_Genre",
        #         "Insured_Count",
        #         "Current_Status_Workstep",
        #         "Issued_Premium",
        #         "Renewal_Premium__Without_Taxes_",
        #         "Branch",
        #         "Sales_Branch",
        #         "Zone",
        #         "Renewal_Channel",
        #         "Renewal_Sub_Channel",
        #         "Renewal_Agent_Code",
        #         "Renewal_Agent_Name",
        #         "Renewal_Agent_Type",
        #         "Pa_Code",
        #         "Conversion_Date",
        #         "Agency_Manager_ID",
        #         "Agency_Manager_Name",
        #         "Renewal_Logged_Date",
        #         "Renewal_Logged_Month",
        #         "Renewal_Issued_Date",
        #         "Renewal_Issued_Month",
        #         "Maximus_Status",
        #         "Lead_Status",
        #         "Sales_Status",
        #         "Hums_Status",
        #         "Hums_Status_Update_Date",
        #         "Current_Team",
        #         "Current_Status_Ageing",
        #         "Login_Ageing",
        #         "Designation",
        #         "Is_Portability",
        #         "Upsell_Eligibility",
        #         "Upsell_Limit",
        #         "Plan_Name",
        #         "Renew_Now",
        #         "Policy_Tenure",
        #         "Product_Name"
        #     ]		

        #     # Filter the list to include only columns present in the DataFrame
        #     existing_str_columns = [col for col in str_columns if col in file_data.columns]				

        #     # List of columns to be converted to bool data type
        #     bool_columns = [
        #         'Communication_Acknowledgement_Over_Ride_DND_', 'Safeguard_Rider_Taken', 'Health_Assurance_Critical_Illness_Criticare_Sum_Assured', 'Split_Flag'
        #     ]

        #     # Filter the list to include only columns present in the DataFrame
        #     existing_bool_columns = [col for col in bool_columns if col in file_data.columns]

        #     # Convert columns to string data type
        #     file_data[existing_str_columns] = file_data[existing_str_columns].astype(str)

        #     # Convert columns to int data type
        #     file_data[existing_int_columns] = file_data[existing_int_columns].astype(int)

        #     # Convert columns to bool data type
        #     file_data[existing_bool_columns] = file_data[existing_bool_columns].astype(bool)

        #     file_data['Policy_Start_Date'] = pd.to_datetime(file_data['Policy_Start_Date'])
        #     file_data['Policy_Expiry_Date'] = pd.to_datetime(file_data['Policy_Expiry_Date'])

        elif table_name == 'equity_master':

            # Filter out rows where 'Client Name' is empty
            file_data = file_data[file_data['Client Name'].notna()]
            
            # Print skipped rows
            skipped_rows = file_data[file_data['Client Name'].isna()]
            for index, row in skipped_rows.iterrows():
                print(f"Skipping row {index} due to missing 'Client Name': {row}")

            file_data = file_data.astype({
                'Client Code': str,
                'Client Name': str,
                'Scrip Code': str,
                'Symbol': str,
                'Pool Holdings': float,
                'Pledge Holdings': float,
                'DP Account Holdings': float,
                'Net Holdings': float,
                'Total Value (?)': float,
                'Bluechip (?)': float,
                'Good (?)': float,
                'Average (?)': float,
                'Poor (?)': float
            })

            # Copy 'Client Name' to 'c_name'
            file_data['c_name'] = file_data['Client Name']

            file_data.rename(columns={
            'Client Code': 'clientcode',
            'Client Name': 'h_name',
            'Scrip Code': 'scripcode',
            'Symbol': 'symbol',
            'Pool Holdings': 'poolholdings',
            'Pledge Holdings': 'pledgeholdings',
            'DP Account Holdings': 'dpaccountholdings',
            'Net Holdings': 'netholdings',
            'Total Value (?)': 'totalvalue',
            'Bluechip (?)': 'bluechip',
            'Good (?)': 'good',
            'Average (?)': 'average',
            'Poor (?)': 'poor'
            }, inplace=True)

            total_records = file_data.shape[0]
            print("Total number of records:", total_records)

            # delete_mismatched_bonds_data()

        elif table_name == 'unify':
            print('under unify block')

            # file_data['Capital Invested'] = file_data['Capital Invested'].str.replace(',', '')
            # file_data['Capital Withdrwal'] = file_data['Capital Withdrwal'].str.replace(',', '')
            # file_data['Net Capital'] = file_data['Net Capital'].str.replace(',', '')
            # file_data['Assets'] = file_data['Assets'].str.replace(',', '')
            # file_data['TWRR'] = file_data['TWRR'].str.rstrip('%')
            # file_data['IRR'] = file_data['IRR'].str.rstrip('%')

            # # print('file_data --------->', file_data)

            # file_data = file_data.astype({
            # 'Name': str,
            # 'Strategy': str,
            # # 'Inception': 'Inception',
            # 'Capital Invested': int,
            # 'Capital Withdrwal': int,
            # 'Net Capital': int,
            # 'Assets': int,
            # 'TWRR': float,
            # 'IRR': float
            # })

            # file_data.rename(columns={
            # 'Name': 'Name',
            # 'Strategy': 'Strategy',
            # 'Inception': 'Inception',
            # 'Capital Invested': 'Capital_Invested',
            # 'Capital Withdrwal': 'Capital_Withdrwal',
            # 'Net Capital': 'Net_Capital',
            # 'Assets': 'Assets',
            # 'TWRR': 'TWRR',
            # 'IRR': 'IRR'
            # }, inplace=True)

            file_data.drop(columns=['CLIENTID'], inplace=True)
            file_data.drop(columns=['ACCOUNTCODE'], inplace=True)
            file_data.drop(columns=['VALUEDATE'], inplace=True)

            file_data = file_data.astype({
            'CLIENTNAME': str,
            'AUM': int
            })

            file_data.rename(columns={
            'CLIENTNAME': 'Name',
            'AUM': 'Assets'
            }, inplace=True)

            file_data['Strategy'] = 'Unify'

        elif table_name == 'fixed_deposit':
            print('under fixed_deposit block')

            file_data.drop(columns=['Sr.No'], inplace=True)

            file_data['Interest Start Date'] = pd.to_datetime(file_data['Interest Start Date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

            # print('file_data --------->', file_data)

            file_data = file_data.astype({
            'Depositt ID': int,
            'Customer ID': int,
            # 'Interest Start Date': int,
            'Application No': int,
            'Customer Name': str,
            'PAN': str,
            'Rate': float,
            'Month': int,
            'Amount': int,
            'Interest Amount': int,
            'Maturity Amount': int
            })

            file_data.rename(columns={
            'Depositt ID': 'Depositt_ID',
            'Customer ID': 'Customer_ID',
            'Interest Start Date': 'Interest_Start_Date',
            'Application No': 'Application_No',
            'Customer Name': 'Customer_Name',
            'PAN': 'PAN',
            'Rate': 'Rate',
            'Month': 'Month',
            'Amount': 'Amount',
            'Interest Amount': 'Interest_Amount',
            'Maturity Amount': 'Maturity_Amount'
            }, inplace=True)


        elif table_name == 'vested':
            print('under vested block')

            file_data.drop(columns=['Country Code'], inplace=True)

            # file_data['Account Created On'] = pd.to_datetime(file_data['Account Created On'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
            # file_data['KYC Approved On'] = pd.to_datetime(file_data['KYC Approved On'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
            # file_data['First Funded On'] = pd.to_datetime(file_data['First Funded On'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
            # file_data['Last Login'] = pd.to_datetime(file_data['Last Login'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')


            # Rename columns
            file_data.rename(columns={
                'Email': 'Email',
                'Name': 'Client_Name',
                'Funded Date': 'First_Funded_On',
                'Phone Number': 'Phone_Number',
                'Equity ($)': 'Equity_Value__USD_',
                'Cash ($)': 'Cash_Value__USD_'
            }, inplace=True)

            # Convert numeric fields to float after removing commas
            numeric_fields = ['Equity_Value__USD_', 'Cash_Value__USD_']
            for field in numeric_fields:
                file_data[field] = file_data[field].astype(str).str.replace(',', '')
                file_data[field] = pd.to_numeric(file_data[field], errors='coerce')

            # Convert dates to 'YYYY-MM-DD' format
            date_fields = ['First_Funded_On']
            for field in date_fields:
                file_data[field] = pd.to_datetime(file_data[field], errors='coerce').dt.strftime('%Y-%m-%d')

            # Apply currency conversion (multiply by 89)
            file_data['Equity_Value__USD_'] *= 89
            file_data['Cash_Value__USD_'] *= 89


        # elif table_name == 'insurance_bse':
        #     print('under insurance bse block')

        #     file_data = file_data.astype({
        #     "Month": str,
        #     "Months": str,
        #     "Ref ID": str,
        #     "Associate Code": str,
        #     "POSP Name": str,
        #     "Region": str,
        #     "Agent PAN": str,
        #     "Channel": str,
        #     "Customer First Name": str,
        #     "Business Type": str,
        #     "Product type": str,
        #     "Coverage Type": str,
        #     "Policy Number": str,
        #     "Policy Number clean": str,
        #     "Policy Issue Date": str,
        #     "Start Date": str,
        #     "Expiry Date": str,
        #     "Payment Date": str,
        #     "Insurer": str,
        #     "Product Name": str,
        #     "Policy Term": str,
        #     "Premium Payment Term": str,
        #     "Premium Payment Frequency": str,
        #     "Sum Insured": str,
        #     "OD Start Date": str,
        #     "OD End Date": str,
        #     "OD Year": str,
        #     "TP Start Date": str,
        #     "TP End Date": str,
        #     "TP Year": str,
        #     "RTO State": str,
        #     "RTO City": str,
        #     "Vehicle Type": str,
        #     "Vehicle Registration No": str,
        #     "Vehicle Registration Date": str,
        #     "Vehicle Manufacture": str,
        #     "Vehicle Make Model": str,
        #     "Vehicle Engine No": str,
        #     "Vehicle Chassis No": str,
        #     "GVW": str,
        #     "Vehicle Cubic Capacity": str,
        #     "Vehicle Seat Capacity": str,
        #     "Fuel": str,
        #     "Vehicle Category": str,
        #     "NCB": str,
        #     "IDV": str,
        #     "OD Premium Amount": str,
        #     "TP Premium Amount": str,
        #     "CPA": str,
        #     "Net Premium": str,
        #     "GST": str,
        #     "Gross Premium Amount": str,
        #     "% On OD Payout": str,
        #     "% On TP Payout": str,
        #     "% Net Payout": str,
        #     "POSP PayOut": str,
        #     "Expected PayOut Amount": str,
        #     "BQP Points less": str,
        #     "POSP/BQP": str,
        #     "Final Status": str
        #     })

        #     file_data.rename(columns={
        #     "Month": "Month",
        #     "Months": "Months",
        #     "Ref ID": "Ref_ID",
        #     "Associate Code": "Associate_Code",
        #     "POSP Name": "POSP_Name",
        #     "Region": "Region",
        #     "Agent PAN": "Agent_PAN",
        #     "Channel": "Channel",
        #     "Customer First Name": "Customer_First_Name",
        #     "Business Type": "Business_Type",
        #     "Product type": "Product_type",
        #     "Coverage Type": "Coverage_Type",
        #     "Policy Number": "Policy_Number",
        #     "Policy Number clean": "Policy_Number_clean",
        #     "Policy Issue Date": "Policy_Issue_Date",
        #     "Start Date": "Start_Date",
        #     "Expiry Date": "Expiry_Date",
        #     "Payment Date": "Payment_Date",
        #     "Insurer": "Insurer",
        #     "Product Name": "Product_Name",
        #     "Policy Term": "Policy_Term",
        #     "Premium Payment Term": "Premium_Payment_Term",
        #     "Premium Payment Frequency": "Premium_Payment_Frequency",
        #     "Sum Insured": "Sum_Insured",
        #     "OD Start Date": "OD_Start_Date",
        #     "OD End Date": "OD_End_Date",
        #     "OD Year": "OD_Year",
        #     "TP Start Date": "TP_Start_Date",
        #     "TP End Date": "TP_End_Date",
        #     "TP Year": "TP_Year",
        #     "RTO State": "RTO_State",
        #     "RTO City": "RTO_City",
        #     "Vehicle Type": "Vehicle_Type",
        #     "Vehicle Registration No": "Vehicle_Registration_No",
        #     "Vehicle Registration Date": "Vehicle_Registration_Date",
        #     "Vehicle Manufacture": "Vehicle_Manufacture",
        #     "Vehicle Make Model": "Vehicle_Make_Model",
        #     "Vehicle Engine No": "Vehicle_Engine_No",
        #     "Vehicle Chassis No": "Vehicle_Chassis_No",
        #     "GVW": "GVW",
        #     "Vehicle Cubic Capacity": "Vehicle_Cubic_Capacity",
        #     "Vehicle Seat Capacity": "Vehicle_Seat_Capacity",
        #     "Fuel": "Fuel",
        #     "Vehicle Category": "Vehicle_Category",
        #     "NCB": "NCB",
        #     "IDV": "IDV",
        #     "OD Premium Amount": "OD_Premium_Amount",
        #     "TP Premium Amount": "TP_Premium_Amount",
        #     "CPA": "CPA",
        #     "Net Premium": "Net_Premium",
        #     "GST": "GST",
        #     "Gross Premium Amount": "Gross_Premium_Amount",
        #     "% On OD Payout": "__On_OD_Payout",
        #     "% On TP Payout": "__On_TP_Payout",
        #     "% Net Payout": "__Net_Payout",
        #     "POSP PayOut": "POSP_PayOut",
        #     "Expected PayOut Amount": "Expected_PayOut_Amount",
        #     "BQP Points less": "BQP_Points_less",
        #     "POSP/BQP": "POSP_BQP",
        #     "Final Status": "Final_Status"
        #     }, inplace=True)
            
        # elif table_name == 'insurance_hdfc':
        #     print('under insurance hdfc block')

        #     file_data = file_data.astype({
        #     "Policy No": str,
        #     "Premium Due Date": str,
        #     "Maturity Date": str,
        #     "Product Name": str,
        #     "Sum Assured": str,
        #     "Policy Status": str,
        #     "Renewal Collection Method": str,
        #     "Client Name": str,
        #     "Life Assured Name": str,
        #     "Birthday": str,
        #     "Policy Proposal Date": str
        #     })

        #     file_data.rename(columns={
        #     "Policy No": "Policy_No",
        #     "Premium Due Date": "Premium_Due_Date",
        #     "Maturity Date": "Maturity_Date",
        #     "Product Name": "Product_Name",
        #     "Sum Assured": "Sum_Assured",
        #     "Policy Status": "Policy_Status",
        #     "Renewal Collection Method": "Renewal_Collection_Method",
        #     "Client Name": "Client_Name",
        #     "Life Assured Name": "Life_Assured_Name",
        #     "Birthday": "Birthday",
        #     "Policy Proposal Date": "Policy_Proposal_Date"
        #     }, inplace=True)

        elif table_name == 'insurance_v2':
            print('under insurance block')

            timezone = pytz.timezone('Asia/Kolkata')
            current_time = timezone.localize(datetime.datetime.now().replace(microsecond=0))

            # Convert columns except "PolicyTerm", "SumAssured", "Mnumber", "TotalPrePaid" to avoid NaN int error
            file_data = file_data.astype({
                "ClientName": str,
                "PolicyNo": str,
                "PolicyIssueDate": "datetime64[ns]",
                "PremiumInstll": float,
                "PaymentFreqncy": str,
                "PremiumDueDate": "datetime64[ns]",
                "MaturityDate": "datetime64[ns]",
                "PName": str,
                "PolicyStatus": str,
                "PDescrption": str,
                "CEmailID": str,
                "NextPreDueDate": "datetime64[ns]",
                "PremiumPayStatus": str,
                "LAssFirstName": str,
                "LAssLastName": str,
                "PaymentMode": str,
                "CompanyName": str
            })

            # Handle int columns separately to fill NaNs
            file_data["PolicyTerm"] = file_data["PolicyTerm"].fillna(0).astype(int)
            file_data["SumAssured"] = file_data["SumAssured"].fillna(0).astype(int)
            file_data["Mnumber"] = file_data["Mnumber"].fillna(0).astype(int)
            file_data["TotalPrePaid"] = file_data["TotalPrePaid"].fillna(0).astype(int)
            file_data["created_at"] = current_time
            file_data["updated_at"] = current_time

            # Keep only the specified columns
            file_data = file_data[[
                "ClientName",
                "PolicyNo",
                "PolicyIssueDate",
                "PremiumInstll",
                "PaymentFreqncy",
                "PremiumDueDate",
                "MaturityDate",
                "PName",
                "PolicyStatus",
                "PDescrption",
                "CEmailID",
                "NextPreDueDate",
                "PremiumPayStatus",
                "LAssFirstName",
                "LAssLastName",
                "PaymentMode",
                "CompanyName",
                "PolicyTerm",
                "SumAssured",
                "Mnumber",
                "TotalPrePaid",
                "created_at",
                "updated_at"
            ]]

            # Drop duplicate rows based on PolicyNo, keeping the latest (or first) entry
            file_data = file_data.drop_duplicates(subset="PolicyNo", keep="last")  # or use keep="first" if preferred

            # Then remove rows where PolicyNo starts with '0'
            file_data = file_data[~file_data['PolicyNo'].astype(str).str.startswith('0')]

            print(file_data)

        elif table_name == 'ask_pms':
            print('under ask_pms block')

            # Normalize column names to uppercase to handle mixed-case headers
            file_data.columns = file_data.columns.str.strip().str.upper()
            print('ask_pms columns:', list(file_data.columns))

            # Clean AUM column — remove commas, convert to float
            file_data['AUM'] = (
                file_data['AUM']
                .astype(str)
                .str.replace(',', '', regex=False)
                .replace('', '0')
                .astype(float)
            )

            # Rename columns to match BigQuery schema
            # New file format: CLIENTID, CLIENTNAME, ACCOUNTCODE, VALUEDATE, AUM
            file_data.rename(columns={
                "CLIENTID":    "CLIENT_CODE",
                "CLIENTNAME":  "CLIENT_NAME",
                "ACCOUNTCODE": "PRODUCT_CODE",
                "AUM":         "AUM_",
            }, inplace=True)

            # Convert datatypes
            file_data = file_data.astype({
                "CLIENT_CODE":  str,
                "CLIENT_NAME":  str,
                "AUM_":         float,
                "PRODUCT_CODE": str,
            })

            # Keep only the required columns (drop VALUEDATE — not in BQ schema)
            file_data = file_data[["CLIENT_CODE", "CLIENT_NAME", "AUM_", "PRODUCT_CODE", "created_at", "updated_at"]]

        elif table_name == 'strata':
            print('under strata block')

            file_data.rename(columns={
            'CP Name': 'CP_Name',
            'IM Name': 'IM_Name',
            'name_on_pan': 'name_on_pan',
            'Amt Deal Value': 'Amt_Deal_Value',
            'Amt Received': 'Amt_Received',
            'status_name': 'status_name',
            'asset_name': 'asset_name'
            }, inplace=True)

            # Convert numeric fields to float if they are represented as strings
            numeric_fields = ['Amt_Deal_Value', 'Amt_Received']
            for field in numeric_fields:
                file_data[field] = pd.to_numeric(file_data[field], errors='coerce')

        # column_data_types = file_data.dtypes
        # print('file data types')
        # print(column_data_types)

        # for index, row in file_data.iterrows():
        #     if index >= 10:  # Check if we've printed 10 rows already
        #         break 
        #     for column in file_data.columns:
        #         cell_value = row[column]
        #         cell_data_type = type(cell_value)
        #         print(f"Row: {index}, Column: {column}, Data Type: {cell_data_type}, Value: {cell_value}")

        # file_data['created_at'] = current_time
        # file_data['updated_at'] = current_time
        # print('roshan')
        # print(file_data)

        print('current time: ', current_time)
        print('type: ', type(current_time))

        # current_date = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")

        # print('Now Current date: ', current_date)

        pandas_gbq.to_gbq(file_data, 'winrich_dev_v2.' + table_name, project_id='elegant-tendril-399501', if_exists='append', location='US')
        # pandas_gbq.to_gbq(file_data, 'temp.' + table_name, project_id='elegant-tendril-399501', if_exists='append', location='asia-south1')
        print(f"Data transfer successful for file: {file_name}")

        print(type(file_data))

        execute_sql_queries(table_name)

        if table_name == 'equity_master':
            insert_from_winwize_into_equity_master(client)
            delete_mismatched_bonds_data()

        if(table_name == 'bonds_v2'):
            sync_and_email_total_investment(client)

        if 'winwizemf' in normalized_file_name:
            send_direct_mf_to_windesk(client)

        # Execute the Fourth SQL query
        # query_job5 = client.query(query5)
        # query_job5.result()  # Waits for the query to finish
        # print("Rows having the unused symbols Deleted From equity_master table")
        print('')

        print('Mapping done successfully')

        metadata = {'Event_ID': event_id, 'Event_type': event_type, 'Bucket_name': bucket, 'File_name': file_name, 'created_at': (current_time), 'updated_at': (current_time), 'status_flag': 1, 'status': 'success', 'failure_reason': None}
        print('meta data: ', metadata)
        # print('meta data data type: ', type(metadata))
        metadata_df = pd.DataFrame.from_records([metadata])

        print("Appending metadata to the metadata table")
        pandas_gbq.to_gbq(metadata_df, 'winrich_dev_v2.gcs_bq_data_transfer_status_tracker', project_id='elegant-tendril-399501', if_exists='append', location='US')
        print("Metadata appended successfully")

    except Exception as e:
        print(f"An error occurred while processing file: {file_name}. Error: {str(e)}")
        print('**************************')
        print(repr(e))

        metadata_failure = {'Event_ID': event_id, 'Event_type': event_type, 'Bucket_name': bucket, 'File_name': file_name, 'created_at': current_time, 'updated_at': current_time, 'status_flag': 0, 'status': 'fail', 'failure_reason': str(e) + '\n' + repr(e)}
        metadata_failure_df = pd.DataFrame.from_records([metadata_failure])

        print("Appending failure metadata to the metadata table")
        pandas_gbq.to_gbq(metadata_failure_df, 'winrich_dev_v2.gcs_bq_data_transfer_status_tracker', project_id='elegant-tendril-399501', if_exists='append', location='US')
        print("Failure metadata appended successfully")

        # Send failure alert email
        try:
            html_message = f"""
            <div style="font-family: Arial, sans-serif; padding: 20px; border: 1px solid #eee; border-radius: 8px;">
                <h2 style="color: #e74c3c;">GCS Upload Failure Alert</h2>
                <p>A file failed to process and load into BigQuery.</p>
                <table style="border-collapse: collapse; width: 100%;">
                    <tr><td style="padding: 8px; font-weight: bold;">File</td><td style="padding: 8px;">{file_name}</td></tr>
                    <tr style="background:#f8f9fa"><td style="padding: 8px; font-weight: bold;">Table</td><td style="padding: 8px;">{table_name}</td></tr>
                    <tr><td style="padding: 8px; font-weight: bold;">Time</td><td style="padding: 8px;">{current_time}</td></tr>
                    <tr style="background:#f8f9fa"><td style="padding: 8px; font-weight: bold;">Error</td><td style="padding: 8px; color: #e74c3c;">{str(e)}</td></tr>
                </table>
                <p style="font-size: 12px; color: #95a5a6; margin-top: 20px;">Check BigQuery table <b>gcs_bq_data_transfer_status_tracker</b> for full details.</p>
            </div>
            """
            for _recipient in ["ashok@winrich.in", "venkatrag@hotmail.com", "niranjan@winrich.in"]:
                requests.post(
                    "https://advisory.winwizeresearch.in/api/method/stock_portfolio_management.api.send_email",
                    json={
                        "recipient": _recipient,
                        "subject": f"[ALERT] GCS Upload Failed: {file_name}",
                        "message": html_message,
                    },
                    timeout=10,
                )
            print("Failure alert email sent.")
        except Exception as email_exc:
            print(f"Could not send failure alert email: {email_exc}")


# *******************************************************************************( Version-5 )****************************************************************************************


# import pandas as pd
# import pandas_gbq
# from google.cloud import bigquery
# from google.cloud import storage
# import functions_framework
# import datetime
# import pytz
# import os
# import numpy as np

# def download_file_from_gcs(bucket_name, source_blob_name, destination_file_name):
#     """Downloads a file from Google Cloud Storage."""
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(source_blob_name)
#     blob.download_to_filename(destination_file_name)
#     print(f'File downloaded from GCS: {source_blob_name}')

# def execute_sql_queries(table_name):
#     client = bigquery.Client()

#     if(table_name == 'liquiloans_master'):

#         column_name = 'name'

#     elif(table_name == 'mutualfunds_master'):

#         column_name = 'c_name'

#     elif(table_name == 'equity_master'):

#         column_name = 'h_name'

#     elif(table_name == 'sip_master'):

#         column_name = 'c_name'

#     elif(table_name == 'vested'):

#         column_name = 'Client_Name'

#     elif(table_name == 'unify'):

#         column_name = 'Name'

#     elif(table_name == 'icici_pms'):

#         column_name = 'clientname'

#     elif(table_name == 'ask_pms'):

#         column_name = 'CLIENT_NAME'

#     elif(table_name == 'hbits'):

#         column_name = 'name_of_the_investor'

#     # elif(table_name == 'insurance_icici'):

#     #     column_name = 'Customer_Full_Name'

#     # elif(table_name == 'insurance_max_bupa'):

#     #     column_name = 'Full_Name'

#     elif(table_name == 'insurance_v2'):

#         column_name = 'ClientName'

#     elif(table_name == 'fixed_deposit'):

#         column_name = 'Customer_Name'

#     elif(table_name == 'strata'):

#         column_name = 'name_on_pan'

#     # elif(table_name == 'insurance_bse'):

#     #     column_name = 'Customer_First_Name'
        
#     # elif(table_name == 'insurance_hdfc'):

#     #     column_name = 'Client_Name'

#     # First SQL query
#     query1 = f"""
#         UPDATE `elegant-tendril-399501.winrich_dev_v2.{table_name}` AS fd
#         SET master_customer_id = (
#             SELECT mcd.master_customer_id
#             FROM `elegant-tendril-399501.winrich_dev_v2.master_customers_data` AS mcd
#             WHERE LOWER(REPLACE(TRIM(fd.{column_name}), ' ', '')) = LOWER(REPLACE(TRIM(mcd.master_username), ' ', '')) 
#         )
#         WHERE fd.master_customer_id is null;
#         """
#     # Second SQL query
#     query2 = f"""
#     UPDATE `elegant-tendril-399501.winrich_dev_v2.{table_name}` AS fd
#     SET master_customer_id = (
#         SELECT ons.master_customer_id
#         FROM `elegant-tendril-399501.winrich_dev_v2.other_names` AS ons
#         WHERE LOWER(REPLACE(TRIM(fd.{column_name}), ' ', '')) = LOWER(REPLACE(TRIM(ons.other_names), ' ', '')) 
#     )
#     WHERE fd.master_customer_id is null;
#     """
    
#     print('Query-2')
#     print(query2)

#     if table_name == 'equity_master':

#         update_stock_symbols()

#         category_update_query_by_condition = f"""
#         UPDATE `elegant-tendril-399501.winrich_dev_v2.equity_master`
#         SET category = CASE
#             WHEN LOWER(symbol) LIKE '%gold%' OR LOWER(symbol) LIKE '%sgb%' THEN 'G'
#             WHEN (REGEXP_CONTAINS(LOWER(symbol), '[a-z]') AND REGEXP_CONTAINS(LOWER(symbol), '[0-9]')) THEN 'B'
#             ELSE 'E'
#         END
#         WHERE TRUE
#         """
#         # Execute the category_update_by_condition SQL query
#         query_job_category1 = client.query(category_update_query_by_condition)
#         query_job_category1.result()  # Waits for the query to finish
#         print(category_update_query_by_condition)
#         print("Category based on the conditions updated successfully.")

#         category_update_by_exception_list = f"""
            
#             UPDATE `elegant-tendril-399501.winrich_dev_v2.{table_name}` AS em
#             SET category = (
#                 SELECT excep_list.category
#                 FROM `elegant-tendril-399501.winrich_dev_v2.equity_master_category_exception_list` AS excep_list
#                 WHERE LOWER(REPLACE(TRIM(em.symbol), ' ', '')) = LOWER(REPLACE(TRIM(excep_list.symbol), ' ', ''))
#                 AND excep_list.category IS NOT NULL AND excep_list.category <> ''
#             )
#             WHERE EXISTS (
#                 SELECT 1
#                 FROM `elegant-tendril-399501.winrich_dev_v2.equity_master_category_exception_list` AS excep_list
#                 WHERE LOWER(REPLACE(TRIM(em.symbol), ' ', '')) = LOWER(REPLACE(TRIM(excep_list.symbol), ' ', ''))
#             );

#         """

#         # Execute the category_update_by_exception_list SQL query
#         query_job_category2 = client.query(category_update_by_exception_list)
#         query_job_category2.result()  # Waits for the query to finish
#         print(category_update_by_exception_list)
#         print("Category based on the exception list updated successfully.")

#         # update stock categories
#         update_stock_categories()

#     # Third SQL Query
#     query3 = """
#     DELETE FROM elegant-tendril-399501.winrich_dev_v2.sip_master
#     WHERE c_name = 'nan' AND
#     month1 = 0.0
#     AND month2 = 0.0
#     AND month3 = 0.0
#     AND month4 = 0.0
#     AND month5 = 0.0
#     AND month6 = 0.0
#     AND month7 = 0.0
#     AND month8 = 0.0
#     AND month9 = 0.0
#     AND month10 = 0.0
#     AND month11 = 0.0
#     AND month12 = 0.0;
#     """

#     # Fourth SQL Query
#     query4 = """
#     DELETE FROM `elegant-tendril-399501.winrich_dev_v2.icici_pms`
#     where clientcode = 'nan' and clientname = 'nan' and productcode = 'nan' and pan = 'nan';
#     """

#     # Fourth SQL Query
#     query5 = """
#     DELETE FROM `elegant-tendril-399501.winrich_dev_v2.equity_master` AS fd
#     WHERE LOWER(REPLACE(TRIM(fd.symbol), ' ', '')) IN (
#     SELECT LOWER(REPLACE(TRIM(mcd.symbol), ' ', ''))
#     FROM `elegant-tendril-399501.winrich_dev_v2.unused_equity_symbols` AS mcd
#     ) AND DATE(created_at) = current_date();
#     """

#     # Execute the first SQL query
#     query_job1 = client.query(query1)
#     query_job1.result()  # Waits for the query to finish
#     print(query1)
#     print("First data mapping successful.")

#     # Execute the second SQL query
#     query_job2 = client.query(query2)
#     query_job2.result()  # Waits for the query to finish
#     print(query2)
#     print("Second data mapping successful.")

#     if(table_name == 'sip_master'):

#         # Execute the Third SQL query
#         query_job3 = client.query(query3)
#         query_job3.result()  # Waits for the query to finish
#         print("Null Rows Deleted From SIP Master")

#     if(table_name == 'icici_pms'):

#         # Execute the Fourth SQL query
#         query_job4 = client.query(query4)
#         query_job4.result()  # Waits for the query to finish
#         print("Null Rows Deleted From icici_pms")


# def get_table_schema(table_name):
#     print('under table_schema')
#     client = bigquery.Client()
#     dataset_ref = client.dataset('winrich_dev_v2')
#     table_ref = dataset_ref.table(table_name)
#     table = client.get_table(table_ref)

#     print('table: ', table)
    
#     schema = [(field.name, field.field_type) for field in table.schema]

#     print('schema: ', schema)
#     return schema

# def is_event_processed(event_id, client):
#     query = f"""
#         SELECT COUNT(*) as count FROM `elegant-tendril-399501.winrich_dev_v2.gcs_bq_data_transfer_status_tracker`
#         WHERE Event_ID = '{event_id}'
#     """
#     result = client.query(query).result()
#     for row in result:
#         return row["count"] > 0
#     return False

# def update_stock_symbols():

#     client = bigquery.Client()

#     query = f"""
#                 UPDATE `elegant-tendril-399501.winrich_dev_v2.equity_master` AS em
#                     SET symbol = ss.symbol
#                     FROM (
#                     SELECT *, ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(isin_no))) AS rn
#                     FROM `elegant-tendril-399501.winrich_dev_v2.stock_symbols`
#                     ) AS ss
#                     WHERE
#                     DATE(em.created_at) = CURRENT_DATE()
#                     AND LOWER(TRIM(em.isin)) = LOWER(TRIM(ss.isin_no))
#                     AND ss.rn = 1;
#         """

#     print(query)

#     # Execute the first SQL query
#     query_job = client.query(query)
#     query_job.result()  # Waits for the query to finish

#     print('**********************************')
#     print('stock symbols have been updated successfully')


# def update_stock_categories():

#     client = bigquery.Client()

#     query = f"""
#                 UPDATE `elegant-tendril-399501.winrich_dev_v2.equity_master` AS em
#                     SET category = ss.category
#                     FROM `elegant-tendril-399501.winrich_dev_v2.stock_symbols` AS ss
#                     WHERE
#                     DATE(em.created_at) = CURRENT_DATE()
#                     AND LOWER(TRIM(em.symbol)) = LOWER(TRIM(ss.symbol))
#                     AND ss.category IS NOT NULL
#                     AND ss.category != '';
#         """

#     print(query)

#     # Execute the first SQL query
#     query_job = client.query(query)
#     query_job.result()  # Waits for the query to finish

#     print('**********************************')
#     print('stock categories have been updated successfully')



# @functions_framework.cloud_event
# def hello_gcs(cloud_event):

#     data = cloud_event.data

#     client = bigquery.Client()

#     # Extract event metadata
#     event_id = cloud_event["id"]
#     event_type = cloud_event["type"]

#     # Extract data fields
#     bucket = data["bucket"]
#     file_name = data["name"]
#     metageneration = data.get("metageneration")
#     timeCreated = data.get("timeCreated")
#     updated = data.get("updated")

#     if is_event_processed(event_id, client):
#         print(f"Event {event_id} already processed. Skipping.")
#         return jsonify({'status': 200, 'msg': 'Duplicate event skipped'}), 200

#     print(f"Event ID: {event_id}")
#     print(f"Event type: {event_type}")
#     print(f"Bucket: {bucket}")
#     print(f"File: {file_name}")
#     print(f"Metageneration: {metageneration}")
#     print(f"Created: {timeCreated}")
#     print(f"Updated: {updated}")

#     # file_name = event['name'].lower()
#     file_name = data["name"]
#     print('file_name: ', file_name)

#     dataset_ref = client.dataset('winrich_dev_v2')
#     tables = client.list_tables(dataset_ref)
#     table_names = [table.table_id for table in tables]

#     print('%%%%%%%%%%%%%%%%%%%%%%%%%')

#     normalized_file_name = file_name.lower()

#     if 'deposit' in normalized_file_name:
#         table_name = 'fixed_deposit'
#     elif 'sipmom' in normalized_file_name:
#         table_name = 'sip_master'
#     elif 'client-dp-holdings' in normalized_file_name:
#         table_name = 'equity_master'
#     elif 'liqui' in normalized_file_name:
#         table_name = 'liquiloans_master'
#     elif 'mutual' in normalized_file_name:
#         table_name = 'mutualfunds_master'
#     elif 'strata' in normalized_file_name:
#         table_name = 'strata'
#     elif 'wawya_daily' in normalized_file_name:
#         table_name = 'unify'
#     elif 'ask' in normalized_file_name or 'ask_pms' in normalized_file_name:
#         table_name = 'ask_pms'
#     elif 'pms' in normalized_file_name or 'icici_pms' in normalized_file_name:
#         table_name = 'icici_pms'
#     elif 'hbits' in normalized_file_name:
#         table_name = 'hbits'
#     # elif 'icici' in normalized_file_name:
#     #     table_name = 'insurance_icici'
#     # elif 'max_bupa' in normalized_file_name:
#     #     table_name = 'insurance_max_bupa'
#     # elif 'bse' in normalized_file_name:
#     #     table_name = 'insurance_bse'
#     # elif 'hdfc' in normalized_file_name:
#     #     table_name = 'insurance_hdfc'
#     elif 'insurance' in normalized_file_name:
#         table_name = 'insurance_v2'
#     elif 'vested' in normalized_file_name or 'funded' in normalized_file_name or 'accounts' in normalized_file_name:
#         table_name = 'vested'
#     else:
#         # table_name = normalized_file_name.split('.')[0]
#         print('This function only process all the financial products csv files')
#         return None


#     print(f'File: {file_name}, Table: {table_name}')
#     current_time = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")

#     print('current time: ', current_time)
#     print('type: ', type(current_time))

#     try:
#         file_path = f'/tmp/{os.path.dirname(file_name)}'
#         os.makedirs(file_path, exist_ok=True)
#         file_path = f'/tmp/{file_name}'

#         print('file_path:', file_path)
#         download_file_from_gcs(bucket, file_name, file_path)

#         file_data = pd.read_csv(file_path)
#         file_data = file_data.applymap(lambda x: x.strip().replace('\t', '') if isinstance(x, str) else x)
#         print("CSV file loaded successfully")

#         print(f"Processing file: {file_name}")
#         print(f"Associated table: {table_name}")

#         file_data['created_at'] = datetime.datetime.now()
#         file_data['updated_at'] = datetime.datetime.now()

#         print('created_at: ', current_time)

#         table_schema = get_table_schema(table_name)
#         print("Schema for table {}: {}".format(table_name, table_schema))

#         file_data.columns = file_data.columns.str.strip()

#         # Checking whether the today's file is already processed or not

#         # Perform a query
#         is_file_already_processed_query = f"""
#                 select * from `elegant-tendril-399501.winrich_dev_v2.{table_name}`
#                 where Date(created_at) = current_date();
#         """
#         query_job = client.query(is_file_already_processed_query)
#         print(f'is already processed query: {is_file_already_processed_query}')
#         print(f'query job: {query_job}')

#         data_from_corresponding_table = []

#         # Process query results
#         for row in query_job:

#             data_from_corresponding_table.append(row)

#         print(f'data_from_corresponding_table: {data_from_corresponding_table}')


#         if(data_from_corresponding_table and data_from_corresponding_table is not None and len(data_from_corresponding_table) > 0):
#             print('The file: ' + str(file_name) + ' is already processed')
#             # return None

#             query = f"""
#             DELETE FROM `elegant-tendril-399501.winrich_dev_v2.{table_name}`
#             WHERE Date(created_at) = current_date();
#             """

#             print(f'query for deleting already processed data')
#             print(query)

#             # Execute the Fourth SQL query
#             query_job = client.query(query)
#             query_job.result()  # Waits for the query to finish
#             print("deleted the already processed today's data & we're processing a new file")
        
#         print('Need to process the data!')

#         if table_name == 'mutualfunds_master':

#             print('inside mutual funds')

#             column_mappings = {
#                 'sCode': 's_code',
#                 'Nature': 'nature',
#                 'Email': 'email',
#                 'Mobile': 'mobile',
#                 'FolioStartDate': 'folio_start_date',
#                 'AvgCost': 'avg_cost',
#                 'InvAmt': 'inv_amt',
#                 'TotalInvAmt': 'total_inv_amt',
#                 'CurNAV': 'cur_nav',
#                 'CurValue': 'cur_value',
#                 'DivAmt': 'div_amt',
#                 'NotionalGain': 'notional_gain',
#                 'ActualGain': 'actual_gain',
#                 'FolioXIRR': 'folio_xirr',
#                 'NatureXIRR': 'nature_xirr',
#                 'ClientXIRR': 'client_xirr',
#                 'NatureAbs': 'nature_abs',
#                 'ClientAbs': 'client_abs',
#                 'absReturn': 'abs_return',
#                 'BalUnit': 'bal_unit',
#                 'ValueDate': 'value_date',
#                 'ReportDate': 'report_date'
#             }

#             # Rename columns using the dictionary
#             file_data.rename(columns=column_mappings, inplace=True)


#             column_data_types = file_data.dtypes

#             print('after data types: ', column_data_types)
#             print('file data types')
#             print(column_data_types)

#             # List of columns to be converted to string data type
#             str_columns = ['h_name', 'c_name', 's_code', 's_name', 'foliono', 'nature', 'folio_start_date', 'bal_unit' , 'email', 'mobile', 'value_date', 'report_date']

#             # List of columns to be converted to float data type
#             float_columns = ['avg_cost', 'inv_amt', 'total_inv_amt', 'cur_nav', 'cur_value', 'div_amt', 'notional_gain', 'actual_gain', 'folio_xirr', 'nature_xirr', 'client_xirr', 'nature_abs', 'client_abs', 'abs_return']

#             # Convert columns to string data type
#             file_data[str_columns] = file_data[str_columns].astype(str)

#             # Convert columns to float data type
#             file_data[float_columns] = file_data[float_columns].astype(float)

#             # print('file data columns: ', file_data)


#         elif(table_name == 'liquiloans_master'):

#             print('Inside the liquiloans block')

#             # # Strip leading and trailing spaces from column names

#             # file_data.columns = file_data.columns.str.strip()

#             file_data['investori'] = file_data['investori'].astype(int)

#             print('file data name: ', file_data['name'])

#             file_data.rename(columns={'annualized_return': 'annualized_return_'}, inplace=True)

#             file_data['name'] = file_data['name'].astype(str)

#             # Convert current_value column to float, coercing errors
#             file_data['current_value'] = pd.to_numeric(file_data['current_value'], errors='coerce')
            
#             # Optionally, fill NaN values with a default value, e.g., 0.0
#             file_data['current_value'].fillna(0.0, inplace=True)

#             # Convert current_value column to float, coercing errors
#             file_data['annualized_return_'] = pd.to_numeric(file_data['annualized_return_'], errors='coerce')
            
#             # Optionally, fill NaN values with a default value, e.g., 0.0
#             file_data['annualized_return_'].fillna(0.0, inplace=True)


#         elif(table_name == 'sip_master'):

#             timezone = pytz.timezone('Asia/Kolkata')
#             current_time = timezone.localize(datetime.datetime.now().replace(microsecond=0))

#             columns_to_convert = ['c_name', 'Month1', 'Month2', 'Month3', 'Month4', 'Month5', 'Month6', 'Month7', 'Month8', 'Month9', 'Month10', 'Month11', 'Month12']

#             # Keep only the specified columns
#             file_data = file_data[columns_to_convert]

#             for column in columns_to_convert:
#                 if column == 'c_name':
#                     file_data[column] = file_data[column].astype(str)
#                 else:
#                     file_data[column] = file_data[column].astype(float)

#             file_data["created_at"] = current_time
#             file_data["updated_at"] = current_time

#         elif(table_name == 'equity_master'):

#             print('under equity_master')

#             print(f'total records from csv: {file_data.shape[0]} or {len(file_data)}')

#             # First, replace the DataFrame’s columns with the first row's values
#             file_data.columns = file_data.iloc[0]

#             # Then drop the first row (which is now redundant, as it's used as headers)
#             file_data = file_data.drop(index=0).reset_index(drop=True)

#             # Apply your cleanup
#             file_data = file_data.applymap(lambda x: x.strip().replace('\t', '') if isinstance(x, str) else x)

#             print("Fixed DataFrame:")
#             print(file_data.head())

#             # # Keep only the relevant columns
#             columns_to_keep = ['Client Code', 'Depository Id', 'Client Name', 'ISIN', 'Scrip Name', 'Holding Quantity', 'Free Quantity', 'Value (₹)']
#             file_data = file_data[columns_to_keep]

#             file_data.rename(columns={
#                 'Client Code': 'clientcode',
#                 'Depository Id': 'depository_id',
#                 'Client Name': 'c_name',
#                 'ISIN': 'isin',
#                 'Scrip Name': 'scrip_name',
#                 'Holding Quantity': 'dpaccountholdings',
#                 'Free Quantity': 'free_qty',
#                 'Value (₹)': 'totalvalue',
#             }, inplace=True)

#             # Assign c_name to h_name
#             file_data['h_name'] = file_data['c_name']

#             # Convert necessary columns to float BEFORE doing math operations
#             file_data['dpaccountholdings'] = pd.to_numeric(file_data['dpaccountholdings'], errors='coerce')
#             file_data['free_qty'] = pd.to_numeric(file_data['free_qty'], errors='coerce')

#             file_data['netholdings'] = file_data['dpaccountholdings']

#             # Now safe to compute pledgeholdings
#             file_data['pledgeholdings'] = file_data['dpaccountholdings'] - file_data['free_qty']

#             # Reset the index
#             file_data.reset_index(drop=True, inplace=True)

#             # Convert columns to appropriate types
#             file_data = file_data.astype({
#                         'clientcode': str,
#                         'depository_id': str,
#                         'c_name': str,
#                         'h_name': str,
#                         'isin': str,
#                         'scrip_name': str,
#                         'dpaccountholdings': float,
#                         'pledgeholdings': float,
#                         'free_qty': float,
#                         'netholdings': float,
#                         'totalvalue': float
#                         })

#             file_data['created_at'] = datetime.datetime.now()
#             file_data['updated_at'] = datetime.datetime.now()

#             print('*************************************')
#             print('filtered data')

#             print(file_data.iloc[0])

#             print(file_data.head(1))

#             print(file_data.iloc[0].to_dict())

#             print(f'total filtered records from csv: {file_data.shape[0]} or {len(file_data)}')


#         elif(table_name == 'icici_pms'):

#             print('under icici pms')

#             # Find the row index where the actual data starts (assuming it starts after a specific header row)
#             start_index = file_data[file_data.iloc[:, 0] == 'CLIENTCODE'].index[0]

#             # Remove the unwanted header rows
#             file_data = file_data.iloc[start_index:]

#             # Reset the column headers
#             file_data.columns = file_data.iloc[0]
#             file_data = file_data[1:]

#             # Keep only the relevant columns
#             columns_to_keep = ['CLIENTCODE', 'CLIENTNAME', 'AUM', 'PRODUCTCODE', 'PAN']
#             file_data = file_data[columns_to_keep]

#             # Reset the index
#             file_data.reset_index(drop=True, inplace=True)

#             file_data['created_at'] = datetime.datetime.now()
#             file_data['updated_at'] = datetime.datetime.now()

#             # Print the cleaned DataFrame (for debugging)
#             print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
#             print(file_data.head())

#             file_data.rename(columns={
#             'CLIENTCODE': 'clientcode',
#             'CLIENTNAME': 'clientname',
#             'AUM': 'aum',
#             'PRODUCTCODE': 'productcode',
#             'PAN': 'pan'
#             }, inplace=True)

#             file_data = file_data.astype({
#             'clientcode': str,
#             'clientname': str,
#             'aum': float,
#             'productcode': str,
#             'pan': str
#             })

#         elif(table_name == 'hbits'):

#             # Rename columns
#             file_data.rename(columns={
#                 'Name of the Investor': 'name_of_the_investor',
#                 'Investment Amount': 'investment_amount',
#                 'SPV': 'spv',
#                 'Investment Amount LMS': 'investment_amount_lms',
#                 'Property Name': 'property_name'
#             }, inplace=True)

#             # Remove commas and convert to numeric
#             file_data['investment_amount'] = file_data['investment_amount'].replace({',': ''}, regex=True).astype(int)
#             file_data['investment_amount_lms'] = file_data['investment_amount_lms'].replace({',': ''}, regex=True).astype(int)

#             # Convert other columns to appropriate types
#             file_data = file_data.astype({
#                 'name_of_the_investor': str,
#                 'spv': str,
#                 'property_name': str
#             })

#         # elif(table_name == 'insurance_icici'):

#         #     file_data.rename(columns={
#         #     'Policy No': 'Policy_No',
#         #     'Customer Full Name': 'Customer_Full_Name',
#         #     'Product Name': 'Product_Name',
#         #     'Mobile No': 'Mobile_No',
#         #     'Due Date': 'Due_Date',
#         #     'Risk Com Date': 'Risk_Com_Date',
#         #     'Issuance Date': 'Issuance_Date',
#         #     'Premium Paying Term': 'Premium_Paying_Term',
#         #     'Premium Amount': 'Premium_Amount',
#         #     'Sum Assured': 'Sum_Assured',
#         #     'Bill Channel': 'Bill_Channel',
#         #     'Suspense Account Balance': 'Suspense_Account_Balance',
#         #     'Net Amount Due': 'Net_Amount_Due',
#         #     'City': 'City____________',
#         #     'Phone1': 'Phone1________',
#         #     'Phone2': 'Phone2_______',
#         #     'Email': 'Email',
#         #     'Payment Frequency': 'Payment_Frequency'
#         #     }, inplace=True)

#         #     # converting the Due_Date and Issuance_Date to be in date time ( timestamp ) type

#         #     file_data['Due_Date'] = pd.to_datetime(file_data['Due_Date'])

#         #     file_data['Issuance_Date'] = pd.to_datetime(file_data['Issuance_Date'])

#         #     file_data['Risk_Com_Date'] = pd.to_datetime(file_data['Risk_Com_Date'], format='%d-%b-%Y').dt.strftime('%Y-%m-%d')

#         #     # List of columns to be converted to string data type
#         #     str_columns = ['Policy_No', 'Customer_Full_Name', 'Product_Name', 'Bill_Channel', 'City____________', 'Phone1________', 'Phone2_______', 'Email' , 'Payment_Frequency']

#         #     # List of columns to be converted to int data type
#         #     int_columns = ['Mobile_No', 'Premium_Paying_Term', 'Premium_Amount', 'Sum_Assured', 'Suspense_Account_Balance', 'Net_Amount_Due']

#         #     # Convert columns to string data type
#         #     file_data[str_columns] = file_data[str_columns].astype(str)

#         #     # Convert columns to int data type
#         #     file_data[int_columns] = file_data[int_columns].astype(int)

#         # elif table_name == 'insurance_max_bupa':

#         #     print('under max_bupa')

#         #     # Remove the "First Name" column if it exists in the DataFrame
#         #     if 'First Name' in file_data.columns:
#         #         file_data.drop(columns=['First Name'], inplace=True)

#         #     if 'Last Name' in file_data.columns:
#         #         file_data.drop(columns=['Last Name'], inplace=True)

#         #     # Define the column mapping
#         #     column_mapping = {
#         #         'Full Name': 'Full_Name',
#         #         'Application Number': 'Application_No',
#         #         'Previous Policy Number': 'Old_Policy_Number',
#         #         'Policy Number': 'New_Policy_Number',
#         #         'Customer ID': 'Customer_ID',
#         #         'DOB': 'DOB',
#         #         'Plan Type': 'Plan_Type',
#         #         'Product ID': 'Product_ID',
#         #         'Product Genre': 'Product_Genre',
#         #         'Insured Lives': 'Insured_Lives',
#         #         'Insured Count': 'Insured_Count',
#         #         'Renewal Premium': 'Renewal_Premium',
#         #         '2 Years Renewal Premium (With Tax)': '_2_Years_Renewal_Premium__With_Tax_',
#         #         'Current Status': 'Current_Status_Workstep',
#         #         'Issued Premium': 'Issued_Premium',
#         #         'Renewal Premium (Without Taxes)': 'Renewal_Premium__Without_Taxes_',
#         #         'Loading Premium': 'Loading_Premium',
#         #         'Sum Assured': 'Sum_Assured',
#         #         'Individual Sum Assured': 'Individual_Sum_Assured',
#         #         'Health Assurance Critical Illness/Criticare Sum Assured': 'Health_Assurance_Critical_Illness_Criticare_Sum_Assured',
#         #         'Health Assurance Personal Accident/Accident Care Sum Assured': 'Health_Assurance_Personal_Accident_Accident_Care_Sum_Assured',
#         #         'Health Assurance Hospital Cash/Hospicash Sum Assured': 'Health_Assurance_Hospital_Cash_Hospicash_Sum_Assured',
#         #         'Login Branch': 'Branch',
#         #         'Sales Branch': 'Sales_Branch',
#         #         'Zone': 'Zone',
#         #         'Renewal Channel': 'Renewal_Channel',
#         #         'Renewal Sub Channel': 'Renewal_Sub_Channel',
#         #         'Renewal Agent Code': 'Renewal_Agent_Code',
#         #         'Renewal Agent Name': 'Renewal_Agent_Name',
#         #         'Renewal Agent Type': 'Renewal_Agent_Type',
#         #         'Pa Code': 'Pa_Code',
#         #         'Conversion Date': 'Conversion_Date',
#         #         'Agency Manager ID': 'Agency_Manager_ID',
#         #         'Agency Manager Name': 'Agency_Manager_Name',
#         #         'Renewal Logged Date': 'Renewal_Logged_Date',
#         #         'Renewal Logged Month': 'Renewal_Logged_Month',
#         #         'Renewal Issued Date': 'Renewal_Issued_Date',
#         #         'Renewal Issued Month': 'Renewal_Issued_Month',
#         #         'Maximus Status': 'Maximus_Status',
#         #         'Lead Status': 'Lead_Status',
#         #         'Sales Status': 'Sales_Status',
#         #         'Hums Status': 'Hums_Status',
#         #         'Hums Status Update Date': 'Hums_Status_Update_Date',
#         #         'Current Team': 'Current_Team',
#         #         'Current Status Ageing': 'Current_Status_Ageing',
#         #         'Login Ageing': 'Login_Ageing',
#         #         'Designation': 'Designation',
#         #         'Policy Start Date': 'Policy_Start_Date',
#         #         'Policy Expiry Date': 'Policy_Expiry_Date',
#         #         'Is Portability': 'Is_Portability',
#         #         'Is Split': 'Split_Flag',
#         #         'Is Upsell': 'Upsell_Eligibility',
#         #         'Upsell Limit': 'Upsell_Limit',
#         #         'Plan Name': 'Plan_Name',
#         #         'Renew Now': 'Renew_Now',
#         #         'Whatsapp Communication for Policy Information': 'Whatsapp_Communication_for_Policy_Information',
#         #         'Communication Acknowledgement(Over Ride DND)': 'Communication_Acknowledgement_Over_Ride_DND_',
#         #         'Safe Guard': 'Safeguard_Rider_Taken',
#         #         'Policy Tenure': 'Policy_Tenure',
#         #         'Product Name': 'Product_Name'
#         #     }

#         #     # Filter the column mapping to include only columns present in the DataFrame
#         #     filtered_column_mapping = {k: v for k, v in column_mapping.items() if k in file_data.columns}

#         #     # Rename the columns
#         #     file_data.rename(columns=filtered_column_mapping, inplace=True)

#         #     # List of columns to be converted to int data type
#         #     int_columns = [
#         #         "Application_No","Old_Policy_Number","Customer_ID","Product_ID","Insured_Lives","Renewal_Premium","_2_Years_Renewal_Premium__With_Tax_","Loading_Premium","Sum_Assured","Individual_Sum_Assured","Health_Assurance_Personal_Accident_Accident_Care_Sum_Assured","Health_Assurance_Hospital_Cash_Hospicash_Sum_Assured","Whatsapp_Communication_for_Policy_Information"
#         #     ]

#         #     # Filter the list to include only columns present in the DataFrame
#         #     existing_int_columns = [col for col in int_columns if col in file_data.columns]

#         #     # Replace non-finite values with 0
#         #     file_data[existing_int_columns] = file_data[existing_int_columns].replace([np.inf, -np.inf, np.nan], 0)


#         #     # List of columns to be converted to string data type
#         #     str_columns = [
#         #         "Full_Name",
#         #         "New_Policy_Number",
#         #         "Plan_Type",
#         #         "Product_Genre",
#         #         "Insured_Count",
#         #         "Current_Status_Workstep",
#         #         "Issued_Premium",
#         #         "Renewal_Premium__Without_Taxes_",
#         #         "Branch",
#         #         "Sales_Branch",
#         #         "Zone",
#         #         "Renewal_Channel",
#         #         "Renewal_Sub_Channel",
#         #         "Renewal_Agent_Code",
#         #         "Renewal_Agent_Name",
#         #         "Renewal_Agent_Type",
#         #         "Pa_Code",
#         #         "Conversion_Date",
#         #         "Agency_Manager_ID",
#         #         "Agency_Manager_Name",
#         #         "Renewal_Logged_Date",
#         #         "Renewal_Logged_Month",
#         #         "Renewal_Issued_Date",
#         #         "Renewal_Issued_Month",
#         #         "Maximus_Status",
#         #         "Lead_Status",
#         #         "Sales_Status",
#         #         "Hums_Status",
#         #         "Hums_Status_Update_Date",
#         #         "Current_Team",
#         #         "Current_Status_Ageing",
#         #         "Login_Ageing",
#         #         "Designation",
#         #         "Is_Portability",
#         #         "Upsell_Eligibility",
#         #         "Upsell_Limit",
#         #         "Plan_Name",
#         #         "Renew_Now",
#         #         "Policy_Tenure",
#         #         "Product_Name"
#         #     ]		

#         #     # Filter the list to include only columns present in the DataFrame
#         #     existing_str_columns = [col for col in str_columns if col in file_data.columns]				

#         #     # List of columns to be converted to bool data type
#         #     bool_columns = [
#         #         'Communication_Acknowledgement_Over_Ride_DND_', 'Safeguard_Rider_Taken', 'Health_Assurance_Critical_Illness_Criticare_Sum_Assured', 'Split_Flag'
#         #     ]

#         #     # Filter the list to include only columns present in the DataFrame
#         #     existing_bool_columns = [col for col in bool_columns if col in file_data.columns]

#         #     # Convert columns to string data type
#         #     file_data[existing_str_columns] = file_data[existing_str_columns].astype(str)

#         #     # Convert columns to int data type
#         #     file_data[existing_int_columns] = file_data[existing_int_columns].astype(int)

#         #     # Convert columns to bool data type
#         #     file_data[existing_bool_columns] = file_data[existing_bool_columns].astype(bool)

#         #     file_data['Policy_Start_Date'] = pd.to_datetime(file_data['Policy_Start_Date'])
#         #     file_data['Policy_Expiry_Date'] = pd.to_datetime(file_data['Policy_Expiry_Date'])


#         elif table_name == 'unify':
#             print('under unify block')

#             # file_data['Capital Invested'] = file_data['Capital Invested'].str.replace(',', '')
#             # file_data['Capital Withdrwal'] = file_data['Capital Withdrwal'].str.replace(',', '')
#             # file_data['Net Capital'] = file_data['Net Capital'].str.replace(',', '')
#             # file_data['Assets'] = file_data['Assets'].str.replace(',', '')
#             # file_data['TWRR'] = file_data['TWRR'].str.rstrip('%')
#             # file_data['IRR'] = file_data['IRR'].str.rstrip('%')

#             # # print('file_data --------->', file_data)

#             # file_data = file_data.astype({
#             # 'Name': str,
#             # 'Strategy': str,
#             # # 'Inception': 'Inception',
#             # 'Capital Invested': int,
#             # 'Capital Withdrwal': int,
#             # 'Net Capital': int,
#             # 'Assets': int,
#             # 'TWRR': float,
#             # 'IRR': float
#             # })

#             # file_data.rename(columns={
#             # 'Name': 'Name',
#             # 'Strategy': 'Strategy',
#             # 'Inception': 'Inception',
#             # 'Capital Invested': 'Capital_Invested',
#             # 'Capital Withdrwal': 'Capital_Withdrwal',
#             # 'Net Capital': 'Net_Capital',
#             # 'Assets': 'Assets',
#             # 'TWRR': 'TWRR',
#             # 'IRR': 'IRR'
#             # }, inplace=True)

#             file_data.drop(columns=['CLIENTID'], inplace=True)
#             file_data.drop(columns=['ACCOUNTCODE'], inplace=True)
#             file_data.drop(columns=['VALUEDATE'], inplace=True)

#             file_data = file_data.astype({
#             'CLIENTNAME': str,
#             'AUM': int
#             })

#             file_data.rename(columns={
#             'CLIENTNAME': 'Name',
#             'AUM': 'Assets'
#             }, inplace=True) 

#             file_data['Strategy'] = 'Unify'

#         elif table_name == 'fixed_deposit':
#             print('under fixed_deposit block')

#             file_data.drop(columns=['Sr.No'], inplace=True)

#             file_data['Interest Start Date'] = pd.to_datetime(file_data['Interest Start Date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

#             # print('file_data --------->', file_data)

#             file_data = file_data.astype({
#             'Depositt ID': int,
#             'Customer ID': int,
#             # 'Interest Start Date': int,
#             'Application No': int,
#             'Customer Name': str,
#             'PAN': str,
#             'Rate': float,
#             'Month': int,
#             'Amount': int,
#             'Interest Amount': int,
#             'Maturity Amount': int
#             })

#             file_data.rename(columns={
#             'Depositt ID': 'Depositt_ID',
#             'Customer ID': 'Customer_ID',
#             'Interest Start Date': 'Interest_Start_Date',
#             'Application No': 'Application_No',
#             'Customer Name': 'Customer_Name',
#             'PAN': 'PAN',
#             'Rate': 'Rate',
#             'Month': 'Month',
#             'Amount': 'Amount',
#             'Interest Amount': 'Interest_Amount',
#             'Maturity Amount': 'Maturity_Amount'
#             }, inplace=True)


#         elif table_name == 'vested':
#             print('under vested block')

#             file_data.drop(columns=['Country Code'], inplace=True)

#             # file_data['Account Created On'] = pd.to_datetime(file_data['Account Created On'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
#             # file_data['KYC Approved On'] = pd.to_datetime(file_data['KYC Approved On'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
#             # file_data['First Funded On'] = pd.to_datetime(file_data['First Funded On'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
#             # file_data['Last Login'] = pd.to_datetime(file_data['Last Login'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')


#             # Rename columns
#             file_data.rename(columns={
#                 'Email': 'Email',
#                 'Name': 'Client_Name',
#                 'Funded Date': 'First_Funded_On',
#                 'Phone Number': 'Phone_Number',
#                 'Equity ($)': 'Equity_Value__USD_',
#                 'Cash ($)': 'Cash_Value__USD_'
#             }, inplace=True)

#             # Convert numeric fields to float after removing commas
#             numeric_fields = ['Equity_Value__USD_', 'Cash_Value__USD_']
#             for field in numeric_fields:
#                 file_data[field] = file_data[field].astype(str).str.replace(',', '')
#                 file_data[field] = pd.to_numeric(file_data[field], errors='coerce')

#             # Convert dates to 'YYYY-MM-DD' format
#             date_fields = ['First_Funded_On']
#             for field in date_fields:
#                 file_data[field] = pd.to_datetime(file_data[field], errors='coerce').dt.strftime('%Y-%m-%d')

#             # Apply currency conversion (multiply by 89)
#             file_data['Equity_Value__USD_'] *= 89
#             file_data['Cash_Value__USD_'] *= 89


#         # elif table_name == 'insurance_bse':
#         #     print('under insurance bse block')

#         #     file_data = file_data.astype({
#         #     "Month": str,
#         #     "Months": str,
#         #     "Ref ID": str,
#         #     "Associate Code": str,
#         #     "POSP Name": str,
#         #     "Region": str,
#         #     "Agent PAN": str,
#         #     "Channel": str,
#         #     "Customer First Name": str,
#         #     "Business Type": str,
#         #     "Product type": str,
#         #     "Coverage Type": str,
#         #     "Policy Number": str,
#         #     "Policy Number clean": str,
#         #     "Policy Issue Date": str,
#         #     "Start Date": str,
#         #     "Expiry Date": str,
#         #     "Payment Date": str,
#         #     "Insurer": str,
#         #     "Product Name": str,
#         #     "Policy Term": str,
#         #     "Premium Payment Term": str,
#         #     "Premium Payment Frequency": str,
#         #     "Sum Insured": str,
#         #     "OD Start Date": str,
#         #     "OD End Date": str,
#         #     "OD Year": str,
#         #     "TP Start Date": str,
#         #     "TP End Date": str,
#         #     "TP Year": str,
#         #     "RTO State": str,
#         #     "RTO City": str,
#         #     "Vehicle Type": str,
#         #     "Vehicle Registration No": str,
#         #     "Vehicle Registration Date": str,
#         #     "Vehicle Manufacture": str,
#         #     "Vehicle Make Model": str,
#         #     "Vehicle Engine No": str,
#         #     "Vehicle Chassis No": str,
#         #     "GVW": str,
#         #     "Vehicle Cubic Capacity": str,
#         #     "Vehicle Seat Capacity": str,
#         #     "Fuel": str,
#         #     "Vehicle Category": str,
#         #     "NCB": str,
#         #     "IDV": str,
#         #     "OD Premium Amount": str,
#         #     "TP Premium Amount": str,
#         #     "CPA": str,
#         #     "Net Premium": str,
#         #     "GST": str,
#         #     "Gross Premium Amount": str,
#         #     "% On OD Payout": str,
#         #     "% On TP Payout": str,
#         #     "% Net Payout": str,
#         #     "POSP PayOut": str,
#         #     "Expected PayOut Amount": str,
#         #     "BQP Points less": str,
#         #     "POSP/BQP": str,
#         #     "Final Status": str
#         #     })

#         #     file_data.rename(columns={
#         #     "Month": "Month",
#         #     "Months": "Months",
#         #     "Ref ID": "Ref_ID",
#         #     "Associate Code": "Associate_Code",
#         #     "POSP Name": "POSP_Name",
#         #     "Region": "Region",
#         #     "Agent PAN": "Agent_PAN",
#         #     "Channel": "Channel",
#         #     "Customer First Name": "Customer_First_Name",
#         #     "Business Type": "Business_Type",
#         #     "Product type": "Product_type",
#         #     "Coverage Type": "Coverage_Type",
#         #     "Policy Number": "Policy_Number",
#         #     "Policy Number clean": "Policy_Number_clean",
#         #     "Policy Issue Date": "Policy_Issue_Date",
#         #     "Start Date": "Start_Date",
#         #     "Expiry Date": "Expiry_Date",
#         #     "Payment Date": "Payment_Date",
#         #     "Insurer": "Insurer",
#         #     "Product Name": "Product_Name",
#         #     "Policy Term": "Policy_Term",
#         #     "Premium Payment Term": "Premium_Payment_Term",
#         #     "Premium Payment Frequency": "Premium_Payment_Frequency",
#         #     "Sum Insured": "Sum_Insured",
#         #     "OD Start Date": "OD_Start_Date",
#         #     "OD End Date": "OD_End_Date",
#         #     "OD Year": "OD_Year",
#         #     "TP Start Date": "TP_Start_Date",
#         #     "TP End Date": "TP_End_Date",
#         #     "TP Year": "TP_Year",
#         #     "RTO State": "RTO_State",
#         #     "RTO City": "RTO_City",
#         #     "Vehicle Type": "Vehicle_Type",
#         #     "Vehicle Registration No": "Vehicle_Registration_No",
#         #     "Vehicle Registration Date": "Vehicle_Registration_Date",
#         #     "Vehicle Manufacture": "Vehicle_Manufacture",
#         #     "Vehicle Make Model": "Vehicle_Make_Model",
#         #     "Vehicle Engine No": "Vehicle_Engine_No",
#         #     "Vehicle Chassis No": "Vehicle_Chassis_No",
#         #     "GVW": "GVW",
#         #     "Vehicle Cubic Capacity": "Vehicle_Cubic_Capacity",
#         #     "Vehicle Seat Capacity": "Vehicle_Seat_Capacity",
#         #     "Fuel": "Fuel",
#         #     "Vehicle Category": "Vehicle_Category",
#         #     "NCB": "NCB",
#         #     "IDV": "IDV",
#         #     "OD Premium Amount": "OD_Premium_Amount",
#         #     "TP Premium Amount": "TP_Premium_Amount",
#         #     "CPA": "CPA",
#         #     "Net Premium": "Net_Premium",
#         #     "GST": "GST",
#         #     "Gross Premium Amount": "Gross_Premium_Amount",
#         #     "% On OD Payout": "__On_OD_Payout",
#         #     "% On TP Payout": "__On_TP_Payout",
#         #     "% Net Payout": "__Net_Payout",
#         #     "POSP PayOut": "POSP_PayOut",
#         #     "Expected PayOut Amount": "Expected_PayOut_Amount",
#         #     "BQP Points less": "BQP_Points_less",
#         #     "POSP/BQP": "POSP_BQP",
#         #     "Final Status": "Final_Status"
#         #     }, inplace=True)
            
#         # elif table_name == 'insurance_hdfc':
#         #     print('under insurance hdfc block')

#         #     file_data = file_data.astype({
#         #     "Policy No": str,
#         #     "Premium Due Date": str,
#         #     "Maturity Date": str,
#         #     "Product Name": str,
#         #     "Sum Assured": str,
#         #     "Policy Status": str,
#         #     "Renewal Collection Method": str,
#         #     "Client Name": str,
#         #     "Life Assured Name": str,
#         #     "Birthday": str,
#         #     "Policy Proposal Date": str
#         #     })

#         #     file_data.rename(columns={
#         #     "Policy No": "Policy_No",
#         #     "Premium Due Date": "Premium_Due_Date",
#         #     "Maturity Date": "Maturity_Date",
#         #     "Product Name": "Product_Name",
#         #     "Sum Assured": "Sum_Assured",
#         #     "Policy Status": "Policy_Status",
#         #     "Renewal Collection Method": "Renewal_Collection_Method",
#         #     "Client Name": "Client_Name",
#         #     "Life Assured Name": "Life_Assured_Name",
#         #     "Birthday": "Birthday",
#         #     "Policy Proposal Date": "Policy_Proposal_Date"
#         #     }, inplace=True)

#         elif table_name == 'insurance_v2':
#             print('under insurance block')

#             timezone = pytz.timezone('Asia/Kolkata')
#             current_time = timezone.localize(datetime.datetime.now().replace(microsecond=0))

#             # Convert columns except "PolicyTerm", "SumAssured", "Mnumber", "TotalPrePaid" to avoid NaN int error
#             file_data = file_data.astype({
#                 "ClientName": str,
#                 "PolicyNo": str,
#                 "PolicyIssueDate": "datetime64[ns]",
#                 "PremiumInstll": float,
#                 "PaymentFreqncy": str,
#                 "PremiumDueDate": "datetime64[ns]",
#                 "MaturityDate": "datetime64[ns]",
#                 "PName": str,
#                 "PolicyStatus": str,
#                 "PDescrption": str,
#                 "CEmailID": str,
#                 "NextPreDueDate": "datetime64[ns]",
#                 "PremiumPayStatus": str,
#                 "LAssFirstName": str,
#                 "LAssLastName": str,
#                 "PaymentMode": str,
#                 "CompanyName": str
#             })

#             # Handle int columns separately to fill NaNs
#             file_data["PolicyTerm"] = file_data["PolicyTerm"].fillna(0).astype(int)
#             file_data["SumAssured"] = file_data["SumAssured"].fillna(0).astype(int)
#             file_data["Mnumber"] = file_data["Mnumber"].fillna(0).astype(int)
#             file_data["TotalPrePaid"] = file_data["TotalPrePaid"].fillna(0).astype(int)
#             file_data["created_at"] = current_time
#             file_data["updated_at"] = current_time

#             # Keep only the specified columns
#             file_data = file_data[[
#                 "ClientName",
#                 "PolicyNo",
#                 "PolicyIssueDate",
#                 "PremiumInstll",
#                 "PaymentFreqncy",
#                 "PremiumDueDate",
#                 "MaturityDate",
#                 "PName",
#                 "PolicyStatus",
#                 "PDescrption",
#                 "CEmailID",
#                 "NextPreDueDate",
#                 "PremiumPayStatus",
#                 "LAssFirstName",
#                 "LAssLastName",
#                 "PaymentMode",
#                 "CompanyName",
#                 "PolicyTerm",
#                 "SumAssured",
#                 "Mnumber",
#                 "TotalPrePaid",
#                 "created_at",
#                 "updated_at"
#             ]]

#             print(file_data)

#         elif table_name == 'ask_pms':
#             print('under ask_pms block')

#             print(type(file_data['AUM']))

#             # Clean the 'aum' column by removing commas and converting to float
#             # file_data['AUM'] = file_data['AUM'].str.replace(',', '').astype(float)

#             file_data['AUM'] = (
#                 file_data['AUM']
#                 .astype(str)
#                 .str.replace(',', '', regex=False)
#                 .replace('', '0')  # optional: replace empty strings with 0
#                 .astype(float)
#             )


#             # file_data['AUM'] = pd.to_numeric(
#             #     file_data['AUM'].astype(str).str.replace(',', '').replace(['nan', 'NaN', 'None', ''], '0'),
#             #     errors='coerce'
#             # ).fillna(0)

#             file_data = file_data.astype({
#             "CLIENT CODE": int,
#             "CLIENT NAME": str,
#             "AUM": float,
#             "PRODUCT CODE": str,
#             "PAN": str
#             })

#             file_data.rename(columns={
#             "CLIENT CODE": "CLIENT_CODE",
#             "CLIENT NAME": "CLIENT_NAME",
#             "AUM": "AUM_",
#             "PRODUCT CODE": "PRODUCT_CODE",
#             "PAN": "PAN"
#             }, inplace=True)
        

#         elif table_name == 'strata':
#             print('under strata block')

#             file_data.rename(columns={
#             'CP Name': 'CP_Name',
#             'IM Name': 'IM_Name',
#             'name_on_pan': 'name_on_pan',
#             'Amt Deal Value': 'Amt_Deal_Value',
#             'Amt Received': 'Amt_Received',
#             'status_name': 'status_name',
#             'asset_name': 'asset_name'
#             }, inplace=True)

#             # Convert numeric fields to float if they are represented as strings
#             numeric_fields = ['Amt_Deal_Value', 'Amt_Received']
#             for field in numeric_fields:
#                 file_data[field] = pd.to_numeric(file_data[field], errors='coerce')

#         # column_data_types = file_data.dtypes
#         # print('file data types')
#         # print(column_data_types)

#         # for index, row in file_data.iterrows():
#         #     if index >= 10:  # Check if we've printed 10 rows already
#         #         break 
#         #     for column in file_data.columns:
#         #         cell_value = row[column]
#         #         cell_data_type = type(cell_value)
#         #         print(f"Row: {index}, Column: {column}, Data Type: {cell_data_type}, Value: {cell_value}")

#         # file_data['created_at'] = current_time
#         # file_data['updated_at'] = current_time
#         # print('roshan')
#         # print(file_data)

#         print('current time: ', current_time)
#         print('type: ', type(current_time))

#         # current_date = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")

#         # print('Now Current date: ', current_date)

#         pandas_gbq.to_gbq(file_data, 'winrich_dev_v2.' + table_name, project_id='elegant-tendril-399501', if_exists='append', location='US')
#         # pandas_gbq.to_gbq(file_data, 'temp.' + table_name, project_id='elegant-tendril-399501', if_exists='append', location='asia-south1')
#         print(f"Data transfer successful for file: {file_name}")

#         execute_sql_queries(table_name)

#         print('Mapping done successfully')

#         metadata = {'Event_ID': event_id, 'Event_type': event_type, 'Bucket_name': bucket, 'File_name': file_name, 'created_at': (current_time), 'updated_at': (current_time), 'status_flag': 1, 'status': 'success', 'failure_reason': None}
#         print('meta data: ', metadata)
#         # print('meta data data type: ', type(metadata))
#         metadata_df = pd.DataFrame.from_records([metadata])

#         print("Appending metadata to the metadata table")
#         pandas_gbq.to_gbq(metadata_df, 'winrich_dev_v2.gcs_bq_data_transfer_status_tracker', project_id='elegant-tendril-399501', if_exists='append', location='US')
#         print("Metadata appended successfully")

#     except Exception as e:
#         print(f"An error occurred while processing file: {file_name}. Error: {str(e)}")
#         print('**************************')
#         print(repr(e))

#         metadata_failure = {'Event_ID': event_id, 'Event_type': event_type, 'Bucket_name': bucket, 'File_name': file_name, 'created_at': current_time, 'updated_at': current_time, 'status_flag': 0, 'status': 'fail', 'failure_reason': str(e) + '\n' + repr(e)}
#         metadata_failure_df = pd.DataFrame.from_records([metadata_failure])

#         print("Appending failure metadata to the metadata table")
#         pandas_gbq.to_gbq(metadata_failure_df, 'winrich_dev_v2.gcs_bq_data_transfer_status_tracker', project_id='elegant-tendril-399501', if_exists='append', location='US')
#         print("Failure metadata appended successfully")