import os
import pandas as pd
import pyodbc
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import configparser
from dotenv import load_dotenv
import argparse
import sys
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed

# Load environment variables
load_dotenv()

# Set up argument parser
parser = argparse.ArgumentParser(description='Handover Automation Script')
parser.add_argument('--config', default='config.ini', help='Path to configuration file')
args = parser.parse_args()

# Load configuration
config = configparser.ConfigParser()
config.read(args.config)

# Set up logging
log_file = config['Logging']['LogFile']
os.makedirs(os.path.dirname(log_file), exist_ok=True)
handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[handler, logging.StreamHandler(sys.stdout)]
)

def connect_to_database(server, database):
    """Establish a connection to the database."""
    conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};'
    conn_str += f'UID={os.getenv("DB_USERNAME")};PWD={os.getenv("DB_PASSWORD")}'
    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        logging.error(f"Failed to connect to database: {e}")
        raise

def get_portfolio_name(cursor, portfolio_id):
    """Retrieve portfolio name from database."""
    query = "SELECT Name FROM IndexIdentifier WHERE PortfolioId = ?"
    try:
        cursor.execute(query, (portfolio_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    except pyodbc.Error as e:
        logging.error(f"Error retrieving portfolio name for ID {portfolio_id}: {e}")
        return None

def process_portfolio_ids(row, cursor):
    """Process portfolio IDs for a given row."""
    new_rows = []
    all_portfolio_ids = row.get("Add all the Portfolio IDs [separated by \",\"(commas)]", "")
    
    if pd.notna(all_portfolio_ids) and isinstance(all_portfolio_ids, str) and all_portfolio_ids.strip():
        portfolio_id_list = [id.strip() for id in all_portfolio_ids.split(',') if id.strip()]
        for pid in portfolio_id_list:
            portfolio_name = get_portfolio_name(cursor, pid)
            if portfolio_name:
                new_row = row.copy()
                new_row['Portfolio ID'] = pid
                new_row['Portfolio Name'] = portfolio_name
                new_rows.append(new_row)
            else:
                logging.warning(f"Portfolio ID '{pid}' not found in IndexIdentifier")
    else:
        single_portfolio_id = str(row.get('Portfolio ID', '')).strip()
        if single_portfolio_id:
            if row.get('Index Status') == 'Under Research':
                row['Portfolio ID'] = f'Research_{process_portfolio_ids.research_counter}'
                process_portfolio_ids.research_counter += 1
            else:
                portfolio_name = get_portfolio_name(cursor, single_portfolio_id)
                if portfolio_name:
                    row['Portfolio Name'] = portfolio_name
            new_rows.append(row)
        else:
            logging.warning(f"Empty Portfolio ID at index {row.name}")
            new_rows.append(row)
    
    return new_rows

process_portfolio_ids.research_counter = 1

def remove_suffixes(name):
    """Remove specific suffixes from portfolio names."""
    if pd.isna(name):
        return name
    suffixes = ['GR', 'TR', 'PR', 'NR']
    for suffix in suffixes:
        index = name.find(suffix)
        if index != -1:
            return name[:index].strip()
    return name

def upsert_data(cursor, df):
    """Upsert data into the database."""
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
                str(row.get('IPRP Link', '')),
                str(row.get('Handing Over to', ''))[:50],
                str(row.get('Handover files Sharepoint link', '')),
                str(row.get('Index Status', '')),
                str(row.get('Is it a part of Index Family', '')),
                str(row.get('Email (Person receiving the handover)', ''))[:225],
                str(row.get('Zoom recording link', ''))[:225],
                str(row.get('Zoom recording key', ''))[:225]
            ]
            cursor.execute(sql, values)
            successful_upserts += 1
        except Exception as e:
            logging.error(f"Error upserting row {index}: {e}")
            failed_upserts += 1

    return successful_upserts, failed_upserts

def process_chunk(chunk, server, database):
    """Process a chunk of data."""
    with connect_to_database(server, database) as conn:
        cursor = conn.cursor()
        new_rows = []
        for _, row in chunk.iterrows():
            new_rows.extend(process_portfolio_ids(row, cursor))
    return new_rows

def main():
    """Main function to run the handover automation script."""
    try:
        file_path = config['Paths']['InputFile']
        df = pd.read_csv(file_path)
        df = df.drop(["Start time", "Completion time", "Name (Person doing the Handover)", "Id"], axis='columns')

        # Parallel processing of portfolio IDs
        chunks = np.array_split(df, os.cpu_count())
        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(process_chunk, chunk, 
                                       config['Database']['Server'], 
                                       config['Database']['Database']) 
                       for chunk in chunks]
            new_rows = []
            for future in as_completed(futures):
                new_rows.extend(future.result())

        new_df = pd.DataFrame(new_rows)
        email_dict = dict(zip(new_df['Handing Over to'], new_df['Email (Person receiving the handover)']))

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
                new_row['Start Date'] = row['End Date']
                new_row['End Date'] = pd.Timestamp.max
                new_row['Email'] = email_dict.get(new_row['Name'], '')
                new_row['Handing Over to'] = 'Currently working'
                new_row['Email (Person receiving the handover)'] = ''
                new_row['Zoom recording link'] = ''
                new_row['Zoom recording key'] = ''
                additional_rows.append(new_row)

        new_df = pd.concat([new_df, pd.DataFrame(additional_rows)], ignore_index=True)
        new_df['End Date'] = new_df['End Date'].astype(str).replace('2262-04-11 23:47:16.854775807', '12/31/9999')

        new_df = new_df.reset_index(drop=True)
        new_df.index = new_df.index + 1
        new_df = new_df.reset_index()
        new_df = new_df.rename(columns={'index': 'RowIndex'})

        new_df = new_df.drop(["Add all the Portfolio IDs [separated by \",\"(commas)]"], axis=1)
        new_df['Portfolio Name'] = new_df['Portfolio Name'].apply(remove_suffixes)

        with connect_to_database(config['Database']['Server'], config['Database']['Database']) as conn:
            cursor = conn.cursor()
            successful_upserts, failed_upserts = upsert_data(cursor, new_df)
            conn.commit()

        logging.info(f"Successful upserts: {successful_upserts}")
        logging.info(f"Failed upserts: {failed_upserts}")

        new_column_order = ['Name', 'Portfolio ID', 'Portfolio Name', 'Handing Over to', 'Start Date',
                            'End Date', 'Email', 'Email (Person receiving the handover)',
                            'Handover files Sharepoint link', 'IPRP Link', 'Index Status',
                            'Is it a part of Index Family']
        new_df = new_df[new_column_order]

        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        output_path = os.path.join(config['Paths']['OutputDirectory'], f"Updated_Handover_Info_{timestamp}.xlsx")
        new_df.to_excel(output_path, index=False, engine='openpyxl')
        logging.info(f"Excel file has been generated: {output_path}")

    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")
        raise

if __name__ == "__main__":
    main()