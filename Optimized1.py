import pandas as pd
import pyodbc
from datetime import datetime
import logging

# Set up logging
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


def adding_currently_working_rows(row, cursor):
    new_rows = []
    all_portfolio_ids = row.get("Add all the Portfolio IDs [separated by \",\"(commas)]", "")
    print(f"Portfolio ID: {row.get('Portfolio ID', 'N/A')}")
    print(f"Index Status: {row.get('Index Status', 'N/A')}")
    print("Add all the Portfolio IDs: {}".format(row.get("Add all the Portfolio IDs [separated by \",\"(commas)]", 'N/A')))
    
    if pd.notna(all_portfolio_ids) and isinstance(all_portfolio_ids, str) and all_portfolio_ids.strip():
        portfolio_id_list = [id.strip() for id in all_portfolio_ids.split(',') if id.strip()]
        for pid in portfolio_id_list:
            portfolio_name = get_portfolio_name(cursor, pid)
            new_row = row.copy()
            if portfolio_name:
                new_row['Portfolio ID'] = pid
                new_row['Portfolio Name'] = portfolio_name
                print(f"Added new row for Portfolio ID: {pid}, Portfolio Name: {portfolio_name}")
            else:
                new_row['Portfolio ID'] = f"Wrong Portfolio ID_{adding_currently_working_rows.wrong_portfolio_id_counter}"
                new_row['Portfolio Name'] = row.get('Portfolio Name', 'N/A')
                adding_currently_working_rows.wrong_portfolio_id_counter +=1
                print(f"Wrong Portfolio ID entered: {pid}, Using original Portfolio Name: {new_row['Portfolio Name']}")
                logging.warning(f"Portfolio ID '{pid}' not found in IndexIdentifier")
            new_rows.append(new_row)
    else:
        print("No multiple Portfolio IDs found, processing single Portfolio ID")
        single_portfolio_id = str(row.get('Portfolio ID', '')).strip()
        if single_portfolio_id:
            if row.get('Index Status') == 'Under Research':
                row['Portfolio ID'] = f'Research_{adding_currently_working_rows.research_counter}'
                row['Portfolio Name'] = row['Portfolio/Project Name']
                adding_currently_working_rows.research_counter += 1
                print(f"Added Under Research row: {row['Portfolio ID']}")
                new_rows.append(row)
            else:
                portfolio_name = get_portfolio_name(cursor, single_portfolio_id)
                if portfolio_name:
                    row['Portfolio Name'] = portfolio_name
                else:
                    row['Portfolio ID'] = f"Wrong Portfolio ID_{adding_currently_working_rows.wrong_portfolio_id_counter}"
                    row['Portfolio Name'] = row.get('Portfolio/Project Name', 'N/A')
                    adding_currently_working_rows.wrong_portfolio_id_counter +=1
                    print(f"Wrong Portfolio ID entered: {single_portfolio_id}, Using original Portfolio Name: {row['Portfolio Name']}")
                new_rows.append(row)
                print(f"Added row for single Portfolio ID: {row['Portfolio ID']}, Portfolio Name: {row['Portfolio Name']}")
        else:
            logging.warning(f"Empty Portfolio ID at index {row.name}")
            new_rows.append(row)
            print("Added original row due to empty Portfolio ID")
            
    return new_rows

adding_currently_working_rows.research_counter = 1
adding_currently_working_rows.wrong_portfolio_id_counter = 1

def remove_suffixes(name):
    if pd.isna(name):
        return name
    suffixes = ['GR', 'TR', 'PR', 'NR']
    for suffix in suffixes:
        index = name.find(suffix)
        if index != -1:
            return name[:index].strip()
    return name
#it updates the data in the sql db 
def upsert_data(cursor, df):
    sql = """
    MERGE dbo.HandoverAutomationFinal AS target
    USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source 
    (RowIndex, Email, Name, PortfolioName, PortfolioId, StartDate, EndDate, IPRPNumber, 
    HandingOverTo, SharepointLink, IndexStatus, IndexFamily, EmailReceiver, ZoomLink, ZoomKey)
    ON target.RowIndex = source.RowIndex 
    WHEN MATCHED THEN
        UPDATE SET 
            IPRPNumber = source.IPRPNumber,
            PortfolioId = source.PortfolioId,
            PortfolioName = source.PortfolioName,      
            Name = source.Name,
            HandingOverTo = source.HandingOverTo,
            StartDate = source.StartDate,
            EndDate = source.EndDate,
            Email = source.Email,
            EmailReceiver = source.EmailReceiver,
            SharepointLink = source.SharepointLink,
            ZoomLink = source.ZoomLink,
            ZoomKey = source.ZoomKey,
            IndexStatus = source.IndexStatus,
            IndexFamily = source.IndexFamily
            
            
    WHEN NOT MATCHED THEN
        INSERT (RowIndex, Email, Name, PortfolioName, PortfolioId, StartDate, EndDate, 
                IPRPNumber, HandingOverTo, SharepointLink, IndexStatus, IndexFamily, 
                EmailReceiver, ZoomLink, ZoomKey)
        VALUES (source.RowIndex, source.Email, source.Name, source.PortfolioName, 
                source.PortfolioId, source.StartDate, source.EndDate, source.IPRPNumber, 
                source.HandingOverTo, source.SharepointLink, source.IndexStatus, 
                source.IndexFamily, source.EmailReceiver, source.ZoomLink, source.ZoomKey);
    """
    
    successful_upserts = 0
    failed_upserts = 0

    for index, row in df.iterrows():
        try:
            values = [
                int(row.get('RowIndex', 0)),
                str(row.get('Email', ''))[:225],
                str(row.get('Name', ''))[:225],
                str(row.get('Portfolio Name', ''))[:225],
                str(row.get('Portfolio ID', ''))[:50],
                row.get('Start Date', None),
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

def main():
    file_path = r"C:\Users\avarsh1\OneDrive - MORNINGSTAR INC\Documents\Handover automation Project\Handover Info Final Form(Sheet1).csv"
    df = pd.read_csv(file_path,encoding='latin1')
    # df = pd.read_excel(r"C:\Users\pnandik\OneDrive - MORNINGSTAR INC\responseForm\Handover Form Final.xlsx")
    df['Start Date'] = pd.to_datetime(df['Start Date'], errors='coerce')
    df['End Date'] = pd.to_datetime(df['End Date'], errors='coerce')
    
    # Format the dates to 'MM/DD/YYYY'
    df['Start Date'] = df['Start Date'].dt.strftime('%m/%d/%Y')
    df['End Date'] = df['End Date'].dt.strftime('%m/%d/%Y')
    df = df.drop(["Start time", "Completion time", "Name (Person doing the Handover)", "Id"], axis='columns') #Droping unnecessary columns
    print(df.columns)
    with connect_to_database('idrschprddb6003', 'MSTAR_INDEX') as conn:
        cursor = conn.cursor()
        new_rows = []
        for _, row in df.iterrows():
            new_rows.extend(adding_currently_working_rows(row, cursor))

    new_df = pd.DataFrame(new_rows)
    email_dict = dict(zip(new_df['Handing Over to'], new_df['Email (Person receiving the handover)']))

    # To add new currently working entries
    latest_entries = {}
    for _, row in new_df.iterrows():
        portfolio_id = row['Portfolio ID']
        if portfolio_id in latest_entries:
            latest_entries[portfolio_id]['End Date'] = row['Start Date']
        latest_entries[portfolio_id] = row

    additional_rows = []
    for _, row in latest_entries.items():
        if pd.notnull(row['Handing Over to']) and pd.notnull(row['End Date']):
            new_row = row.copy()
            new_row['Name'] = row['Handing Over to']
            new_row['Start Date'] = (pd.to_datetime(row['End Date'], format='%m/%d/%Y') + pd.Timedelta(days=1)).strftime('%m/%d/%Y')
            new_row['End Date'] = pd.Timestamp.max
            new_row['Email'] = email_dict.get(new_row['Name'], '')
            new_row['Handing Over to'] = 'Currently working'
            new_row['Email (Person receiving the handover)'] = ''
            new_row['Zoom recording link'] = ''
            new_row['Zoom recording key'] = ''
            new_row['IPRP Number'] = row['IPRP Number']
            additional_rows.append(new_row)

    new_df = pd.concat([new_df, pd.DataFrame(additional_rows)], ignore_index=True)
    new_df['End Date'] = new_df['End Date'].astype(str).replace('2262-04-11 23:47:16.854775807', '12/31/9999')

    new_df = new_df.reset_index(drop=True)
    new_df.index = new_df.index + 1
    new_df = new_df.reset_index()
    new_df = new_df.rename(columns={'index': 'RowIndex'})

    new_df = new_df.drop(["Add all the Portfolio IDs [separated by \",\"(commas)]"], axis=1)
    new_df['Portfolio Name'] = new_df['Portfolio Name'].apply(remove_suffixes)

    with connect_to_database('idrschprddb6003', 'Playground') as conn:
        cursor = conn.cursor()
        successful_upserts, failed_upserts = upsert_data(cursor, new_df)
        conn.commit()

    logging.info(f"Successful upserts: {successful_upserts}")
    logging.info(f"Failed upserts: {failed_upserts}")

    new_column_order = ['IPRP Number', 'Portfolio ID', 'Portfolio Name','Name', 'Handing Over to', 'Start Date',
                        'End Date', 'Email', 'Email (Person receiving the handover)',
                        'Handover files Sharepoint link',  'Index Status',
                        'Is it a part of Index Family']
    new_df = new_df[new_column_order]

    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    output_path = f"C:\\Users\\avarsh1\\OneDrive - MORNINGSTAR INC\\Documents\\Handover automation Project\\Updated_Handover_Info_{timestamp}.xlsx"
    new_df.to_excel(output_path, index=False, engine='openpyxl')
    logging.info(f"Excel file has been generated: {output_path}")

if __name__ == "__main__":
    main()