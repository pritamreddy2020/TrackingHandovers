import pandas as pd
import pyodbc
from datetime import datetime
import logging
import sys
import DatabaseConnection as dc

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#Function for Connecting to the SQL Server
def connect_to_database(server, database):
    conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
    return pyodbc.connect(conn_str)


#Function to Get the Portfolio Name From the MSTAR Index
def get_portfolio_name(cursor, portfolio_id):
    query = "SELECT Name FROM IndexIdentifier WHERE PortfolioId = ?"
    cursor.execute(query, (portfolio_id,))
    result = cursor.fetchone()
    return result[0] if result else None

wrong_portfolio_ids = []

def process_portfolio_ids(row, cursor, portfolio_id_list):
    new_rows = []
    for pid in portfolio_id_list:
        portfolio_name = get_portfolio_name(cursor, pid)
        new_row = row.copy()
        if portfolio_name:
            new_row['Portfolio ID'] = pid
            new_row['Portfolio Name'] = portfolio_name
            print(f"Added new row for Portfolio ID: {pid}, Portfolio Name: {portfolio_name}")
        else:
            wrong_portfolio_ids.append(pid)
            logging.warning(f"Portfolio ID '{pid}' not found in IndexIdentifier")
        new_rows.append(new_row)
    return new_rows

def read_excel_from_user_input():
    file_path = r"C:\Users\avarsh1\OneDrive - MORNINGSTAR INC\Documents\Handover automation Project\Handover Form Final(Sheet1).csv"
    try:
        df = pd.read_csv(file_path,encoding='latin1')
        print("File read successfully!")
        return df
    except FileNotFoundError:
        print("The file was not found. Please check the path and try again.")
    except Exception as e:
        print(f"An error occurred: {e}")


def process_single_portfolio_id(row, cursor):
    new_rows = []
    single_portfolio_id = str(row.get('Portfolio ID', '')).strip()
    if single_portfolio_id:
        if row.get('Index Status') == 'Under Research':
            row['Portfolio ID'] = f'Research_{checking_portfolio_id.research_counter}'
            row['Portfolio Name'] = row['Portfolio/Project Name']
            checking_portfolio_id.research_counter += 1
            print(f"Added Under Research row: {row['Portfolio ID']}")
        else:
            portfolio_name = get_portfolio_name(cursor, single_portfolio_id)
            if portfolio_name:
                row['Portfolio Name'] = portfolio_name
            else:
                wrong_portfolio_ids.append(single_portfolio_id)
                print(f"Wrong Portfolio ID entered: {single_portfolio_id}")
        print(f"Added row for single Portfolio ID: {row['Portfolio ID']}")
    else:
        logging.warning(f"Empty Portfolio ID at index {row.name}")
        print("Added original row due to empty Portfolio ID")
    new_rows.append(row)
    return new_rows

def checking_portfolio_id(row, cursor):
    all_portfolio_ids = row.get("Add all the Portfolio IDs [separated by \",\"(commas)]", "")
    print(f"Portfolio ID: {row.get('Portfolio ID', 'N/A')}")
    print(f"Index Status: {row.get('Index Status', 'N/A')}")
    print("Add all the Portfolio IDs: {}".format(row.get("Add all the Portfolio IDs [separated by \",\"(commas)]", 'N/A')))

    if pd.notna(all_portfolio_ids) and isinstance(all_portfolio_ids, str) and all_portfolio_ids.strip():
        portfolio_id_list = [id.strip() for id in all_portfolio_ids.split(',') if id.strip()]
        return process_portfolio_ids(row, cursor, portfolio_id_list)
    else:
        print("No multiple Portfolio IDs found, processing single Portfolio ID")
        return process_single_portfolio_id(row, cursor)
    
checking_portfolio_id.research_counter = 1
checking_portfolio_id.wrong_portfolio_id_counter = 1


def remove_suffixes(name):
    if pd.isna(name):
        return name
    suffixes = ['GR', 'TR', 'PR', 'NR']
    for suffix in suffixes:
        index = name.find(suffix)
        if index != -1:
            return name[:index].strip()
    return name


def upsert_data(cursor, df):
    sql = """
    MERGE dbo.HandoverAutomationFinal AS target
    USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source 
    (PortfolioId, Name, StartDate, Email, PortfolioName, EndDate, IPRPNumber, 
    HandingOverTo, SharepointLink, IndexStatus, IndexFamily, EmailReceiver, 
    ZoomLink, ZoomKey)
    ON target.PortfolioId = source.PortfolioId 
    AND target.Name = source.Name 
    AND target.StartDate = source.StartDate 
    WHEN MATCHED THEN
        UPDATE SET 
            Email = source.Email,
            PortfolioName = source.PortfolioName,
            EndDate = source.EndDate,
            IPRPNumber = source.IPRPNumber,
            HandingOverTo = source.HandingOverTo,
            SharepointLink = source.SharepointLink,
            IndexStatus = source.IndexStatus,
            IndexFamily = source.IndexFamily,
            EmailReceiver = source.EmailReceiver,
            ZoomLink = source.ZoomLink,
            ZoomKey = source.ZoomKey
    WHEN NOT MATCHED THEN
        INSERT (PortfolioId, Name, StartDate, Email, PortfolioName, EndDate, 
                IPRPNumber, HandingOverTo, SharepointLink, IndexStatus, 
                IndexFamily, EmailReceiver, ZoomLink, ZoomKey)
        VALUES (source.PortfolioId, source.Name, source.StartDate, source.Email, 
                source.PortfolioName, source.EndDate, source.IPRPNumber, 
                source.HandingOverTo, source.SharepointLink, source.IndexStatus, 
                source.IndexFamily, source.EmailReceiver, source.ZoomLink, source.ZoomKey);
    """
    
    successful_upserts = 0
    failed_upserts = 0

    for index, row in df.iterrows():
        try:
            values = [
                str(row.get('Portfolio ID', ''))[:50],
                str(row.get('Name', ''))[:225],
                row.get('Start Date', None),
                str(row.get('Email', ''))[:225],
                str(row.get('Portfolio Name', ''))[:225],
                row.get('End Date', None),
                str(row.get('IPRP Number', '')),
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
            successful_upserts += 1
        except Exception as e:
            logging.error(f"Error upserting row {index}: {e}")
            print(f"Row data: {row}")
            failed_upserts += 1

    return successful_upserts, failed_upserts




def add_currently_working_entries(new_df, email_dict):
    latest_entries = {}
    for _, row in new_df.iterrows():
        portfolio_id = row['Portfolio Name']
        if portfolio_id not in latest_entries or pd.to_datetime(row['Start Date']) > pd.to_datetime(latest_entries[portfolio_id]['Start Date']):
            latest_entries[portfolio_id] = row.to_dict()

    currently_working_rows = []
    for _, row in latest_entries.items():
        if pd.notnull(row['Handing Over to']) and pd.notnull(row['End Date']):
            new_row = row.copy()
            new_row['Name'] = row['Handing Over to']
            new_row['Start Date'] = (pd.to_datetime(row['End Date'], format='%m/%d/%Y') + pd.Timedelta(days=1)).strftime('%m/%d/%Y')
            new_row['End Date'] = "9999/99/9"
            new_row['Email'] = email_dict.get(new_row['Name'], '')
            new_row['Handing Over to'] = 'Currently working'
            new_row['Email (Person receiving the handover)'] = ''
            new_row['Zoom recording link'] = ''
            new_row['Zoom recording key'] = ''
            currently_working_rows.append(new_row)
    
    return pd.DataFrame(currently_working_rows)

def save_dataframe_to_excel(new_df, output_path):
    new_column_order = ['IPRP Number', 'Portfolio ID', 'Portfolio Name','Name', 'Handing Over to', 'Start Date',
                        'End Date', 'Email', 'Email (Person receiving the handover)',
                        'Handover files Sharepoint link',  'Index Status',
                        'Is it a part of Index Family']
    new_df = new_df[new_column_order]
    
    new_df.to_excel(output_path, index=False, engine='openpyxl')
    logging.info(f"Excel file has been generated: {output_path}")
    
    
def portfolioid_checks(df):
     live_indices = df[df["Index Status"] == "Live Index"]
 
     if live_indices.empty:
         print("No Live Indices found in the data.")
     else:
         portfolio_id_filled = ~(live_indices["Portfolio ID"].isna() | (live_indices["Portfolio ID"] == ""))
         add_all_portfolios_filled = ~(live_indices["Add all the Portfolio IDs [separated by \",\"(commas)]"].isna() | (live_indices["Add all the Portfolio IDs [separated by \",\"(commas)]"] == ""))
     
         both_empty = (~portfolio_id_filled & ~add_all_portfolios_filled).any()
         both_filled = (portfolio_id_filled & add_all_portfolios_filled).any()
     
         if both_empty:
             print("Error: For Live Indices, either 'Portfolio ID' or 'Add all the Portfolio IDs' must be filled.")
             sys.exit("Exiting script as Portfolio ID information is missing for Live Indices.")
         elif both_filled:
             print("Error: For Live Indices, 'Portfolio ID' and 'Add all the Portfolio IDs' cannot both be filled.")
             sys.exit("Exiting script as both Portfolio ID fields are filled for Live Indices.")
         else:
             print("Portfolio ID information for Live Indices is correctly formatted.")   

def main():
    wrong_portfolio_ids=[]
    
    df = read_excel_from_user_input()
    
    #Format the dates to 'MM/DD/YYYY'
    df['Start Date'] = pd.to_datetime(df['Start Date'], errors='coerce')
    df['End Date'] = pd.to_datetime(df['End Date'], errors='coerce')
    
    df['Start Date'] = df['Start Date'].dt.strftime('%m/%d/%Y')
    df['End Date'] = df['End Date'].dt.strftime('%m/%d/%Y')
    
    df = df.drop(["Start time", "Completion time", "Name (Person doing the Handover)", "Id"], axis='columns') #Droping unnecessary columns
    print(df.columns)
    print(df.shape)
    
    portfolioid_checks(df)
            
        
    with connect_to_database('idrschprddb6003', 'MSTAR_INDEX') as conn:
        cursor = conn.cursor()
        new_rows = []
        for _, row in df.iterrows():
            new_rows.extend(checking_portfolio_id(row, cursor))
    new_df = pd.DataFrame(new_rows)
   
    if wrong_portfolio_ids:
        print("The following portfolio IDs were not found and caused errors:")
        for pid in wrong_portfolio_ids:
            print(pid)
        sys.exit("Exiting script due to wrong portfolio IDs.")
        
    new_df['End Date'] = new_df['End Date'].astype(str).replace('2262-04-11 23:47:16.854775807', '12/31/9999')
    new_df = new_df.drop(["Add all the Portfolio IDs [separated by \",\"(commas)]"], axis=1)
    new_df['Portfolio Name'] = new_df['Portfolio Name'].apply(remove_suffixes)

    with connect_to_database('idrschprddb6003', 'Playground') as conn:
        cursor = conn.cursor()
        successful_upserts, failed_upserts = upsert_data(cursor, new_df)
        conn.commit()

    logging.info(f"Successful upserts: {successful_upserts}")
    logging.info(f"Failed upserts: {failed_upserts}")
    
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

    save_dataframe_to_excel(new_df, f"C:\\Users\\avarsh1\\OneDrive - MORNINGSTAR INC\\Documents\\Handover automation Project\\Updated_Handover_Info_{timestamp}.xlsx")
    
    # Create a separate dataframe for currently working entries
    
    sqlDb_df = dc.sqlread("""select * from Playground..HandoverAutomationFinal""","idrschprddb6003")
        
    
    email_dict = dict(zip(new_df['Handing Over to'], new_df['Email (Person receiving the handover)']))
    currently_working_df = add_currently_working_entries(sqlDb_df, email_dict)

    # Save currently_working_df to a different Excel file
    save_dataframe_to_excel(currently_working_df, f"C:\\Users\\avarsh1\\OneDrive - MORNINGSTAR INC\\Documents\\Handover automation Project\\Currently_working_Info_{timestamp}.xlsx")

    
    
    

if __name__ == "__main__":
    main()