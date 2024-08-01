import pandas as pd
import pyodbc
from datetime import datetime

# Importing the responses
file_path = "C:\\Users\\avarsh1\\OneDrive - MORNINGSTAR INC\\Documents\\Handover automation Project\\Handover Info Final Form(Sheet1).csv"
df = pd.read_csv(file_path)
# Drop unnecessary columns
df = df.drop(["Start time", "Completion time","Name (Person doing the Handover)","Id"], axis='columns')
print(df.columns)


# Connecting to the sql server
server = 'idrschprddb6003'
database = 'MSTAR_INDEX'
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

def get_portfolio_name(portfolio_id, cursor):
    query = "SELECT Name FROM IndexIdentifier WHERE PortfolioId = ?"
    cursor.execute(query, (portfolio_id,))
    result = cursor.fetchone()
    return result[0] if result else None

research_counter = 1 
new_rows = []

print("Original DataFrame shape:", df.shape)
print("Original DataFrame columns:", df.columns)
print("First few rows of the original DataFrame:")
print(df.head())

for index, row in df.iterrows():
    print(f"\nProcessing row {index}")
    print(f"Portfolio ID: {row.get('Portfolio ID', 'N/A')}")
    print(f"Index Status: {row.get('Index Status', 'N/A')}")
    print("Add all the Portfolio IDs: {}".format(row.get("Add all the Portfolio IDs [separated by \",\"(commas)]", 'N/A')))

    all_portfolio_ids = row.get("Add all the Portfolio IDs [separated by \",\"(commas)]", "")
    
    if pd.notna(all_portfolio_ids) and isinstance(all_portfolio_ids, str) and all_portfolio_ids.strip():
        portfolio_id_list = [id.strip() for id in all_portfolio_ids.split(',') if id.strip()]
        print(f"Found {len(portfolio_id_list)} Portfolio IDs: {portfolio_id_list}")
        
        if portfolio_id_list:
            for pid in portfolio_id_list:
                portfolio_name = get_portfolio_name(pid, cursor)
                if portfolio_name:
                    new_row = row.copy()
                    new_row['Portfolio ID'] = pid
                    new_row['Portfolio Name'] = portfolio_name
                    new_rows.append(new_row)
                    print(f"Added new row for Portfolio ID: {pid}, Portfolio Name: {portfolio_name}")
                else:
                    print(f"Warning: Portfolio ID '{pid}' not found in IndexIdentifier")
        else:
            print("Warning: No valid Portfolio IDs found after splitting")
            new_rows.append(row)
            print("Added original row due to no valid Portfolio IDs")
    else:
        print("No multiple Portfolio IDs found, processing single Portfolio ID")
        single_portfolio_id = str(row.get('Portfolio ID', '')).strip()
        if single_portfolio_id:
            if row.get('Index Status') == 'Under Research':
                row['Portfolio ID'] = f'Research_{research_counter}'
                row['Portfolio Name'] = row['Project Name']
                research_counter += 1
                new_rows.append(row)
                print(f"Added Under Research row: {row['Portfolio ID']}")
            else:
                portfolio_name = get_portfolio_name(single_portfolio_id, cursor)
                if portfolio_name:
                    row['Portfolio Name'] = portfolio_name
                new_rows.append(row)
                print(f"Added row for single Portfolio ID: {single_portfolio_id}, Portfolio Name: {portfolio_name}")
        else:
            print(f"Warning: Empty Portfolio ID at index {index}")
            new_rows.append(row)
            print("Added original row due to empty Portfolio ID")

    print(f"Current number of rows in new_rows: {len(new_rows)}")

new_df = pd.DataFrame(new_rows)
print("\nNew DataFrame shape:", new_df.shape)
print("New DataFrame columns:", new_df.columns)
print("First few rows of the new DataFrame:")
print(new_df.head())
print("\nLast few rows of the new DataFrame:")
print(new_df.tail())

# Close the database connection
cursor.close()
conn.close()

df=new_df

email_dict = dict(zip(df['Handing Over to'], df['Email (Person receiving the handover)']))

new_rows = []
latest_entries = {}

# Iterate over the DataFrame to create new rows and update end dates
for index, row in df.iterrows():
    portfolio_id = row['Portfolio ID']
    
    if portfolio_id in latest_entries:
        latest_entries[portfolio_id]['End Date'] = row['Start Date']
    
    latest_entries[portfolio_id] = row

for portfolio_id, row in latest_entries.items():
    if pd.notnull(row['Handing Over to']) and pd.notnull(row['End Date']):
        new_row = row.copy()
        new_row['Name'] = row['Handing Over to']
        new_row['Start Date'] = row['End Date']
        new_row['End Date'] = pd.Timestamp.max
        new_row['Email'] = email_dict.get(new_row['Name'], '')       
        new_row['Handing Over to'] = 'Currently working'
        new_row['Email (Person receiving the handover)'] = ''
        new_row['Zoom recording link'] = ''
        new_row['Zoom recording key'] = ''
        
        new_rows.append(new_row)

new_rows_df = pd.DataFrame(new_rows)
df = pd.concat([df, new_rows_df], ignore_index=True)

# Convert End Date to string and replace max timestamp with '12/31/9999'
df['End Date'] = df['End Date'].astype(str).replace('2262-04-11 23:47:16.854775807', '12/31/9999')

# Set display options
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# Reset index and create RowIndex column
df = df.reset_index(drop=True)
df.index = df.index + 1
df = df.reset_index()
df = df.rename(columns={'index': 'RowIndex'})


column_to_drop = "Add all the Portfolio IDs [separated by \",\"(commas)]"
df = df.drop([column_to_drop], axis=1)

#To remove the Last Two words like GR USD words from Portfolio Name
def remove_suffixes(name):
    if pd.isna(name):
        return name
    suffixes = ['GR', 'TR', 'PR', 'NR']
    for suffix in suffixes:
        index = name.find(suffix)
        if index != -1:
            return name[:index].strip()
    return name

df['Portfolio Name'] = df['Portfolio Name'].apply(remove_suffixes)
 

# Establish the connection to SQL Server
server = 'idrschprddb6003'
database = 'Playground'
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Define the upsert function
def upsert_data(cursor, row):
    try:
        sql = """
        MERGE dbo.HandoversFinal AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source 
        (RowIndex, Email, Name, PortfolioName, PortfolioId, StartDate, EndDate, IPRPLink, 
        HandingOverTo, SharepointLink, IndexStatus, IndexFamily, EmailReceiver, ZoomLink, ZoomKey)
        ON target.RowIndex = source.RowIndex 
        WHEN MATCHED THEN
            UPDATE SET 
                Email = source.Email,
                Name = source.Name,
                PortfolioName = source.PortfolioName,
                PortfolioId = source.PortfolioId,
                StartDate = source.StartDate,
                EndDate = source.EndDate,
                IPRPLink = source.IPRPLink,
                HandingOverTo = source.HandingOverTo,
                SharepointLink = source.SharepointLink,
                IndexStatus = source.IndexStatus,
                IndexFamily = source.IndexFamily,
                EmailReceiver = source.EmailReceiver,
                ZoomLink = source.ZoomLink,
                ZoomKey = source.ZoomKey
        WHEN NOT MATCHED THEN
            INSERT (RowIndex, Email, Name, PortfolioName, PortfolioId, StartDate, EndDate, 
                    IPRPLink, HandingOverTo, SharepointLink, IndexStatus, IndexFamily, 
                    EmailReceiver, ZoomLink, ZoomKey)
            VALUES (source.RowIndex, source.Email, source.Name, source.PortfolioName, 
                    source.PortfolioId, source.StartDate, source.EndDate, source.IPRPLink, 
                    source.HandingOverTo, source.SharepointLink, source.IndexStatus, 
                    source.IndexFamily, source.EmailReceiver, source.ZoomLink, source.ZoomKey);
        """
        
        values = [
            int(row.get('RowIndex', 0)),  
            str(row.get('Email', ''))[:225],
            str(row.get('Name', ''))[:225],
            str(row.get('Portfolio Name', ''))[:225],  
            str(row.get('Portfolio ID', ''))[:50],  
            row.get('Start Date', None),  
            row.get('End Date', None),  
            str(row.get('IPRP Link', '')),  
            str(row.get('Handing Over to', ''))[:50],  
            str(row.get('Handover files Sharepoint link', '')),  
            str(row.get('Index Status', '')),  
            str(row.get('Is it a part of Index Family', '')),  
            str(row.get('Email (Person receiving the handover)', ''))[:225], 
            str(row.get('Zoom recording link', ''))[:225],  
            str(row.get('Zoom recording key', ''))[:225]  
        ]
        
        print(f"Executing SQL: {sql}")
        print(f"With values: {values}")
        cursor.execute(sql, values)
        return True
    except Exception as e:
        print(f"Error in upsert_data: {e}")
        print(f"Row data: {row}")
        return False

# Counter for successful and failed upserts
successful_upserts = 0
failed_upserts = 0

# Assuming you're calling this function in a loop:
for index, row in df.iterrows():
    print(f"Processing row {index}")
    try:
        if upsert_data(cursor, row):
            successful_upserts += 1
            print(f"Row {index} upserted successfully")
        else:
            failed_upserts += 1
            print(f"Row {index} failed to upsert")
    except Exception as e:
        print(f"Error upserting row {index}: {e}")
        failed_upserts += 1

# Commit the changes and close the connection
conn.commit()
conn.close()

print(f"Successful upserts: {successful_upserts}")
print(f"Failed upserts: {failed_upserts}")



df = df.drop(["RowIndex"], axis='columns')

print(df)

new_column_order = ['Name','Portfolio ID','Portfolio Name', 'Handing Over to','Start Date',
                    'End Date','Email','Email (Person receiving the handover)', 
       'Handover files Sharepoint link', 'Zoom recording link', 'Zoom recording key', 'IPRP Link', 'Index Status',
        'Is it a part of Index Family' ]


df = df[new_column_order]

# Export the DataFrame to Excel
timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
output_path = f"C:\\Users\\pnandik\\OneDrive - MORNINGSTAR INC\\Documents\\Handover automation Project\\Updated_Handover_Info_{timestamp}.xlsx"
df.to_excel(output_path, index=False, engine='openpyxl')
print(f"Excel file has been generated: {output_path}")