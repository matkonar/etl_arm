import pandas as pd
import numpy as np
import datetime
import re

# 1. Extract

def get_date_from_filename(file_name):
    """
    Extracts the date from a given filename if it follows the format 'ARM_YYYY-MM-DD.xlsx'.

    Parameters:
        file_name (str): The filename from which the date needs to be extracted.

    Returns:
        str: The extracted date in the format 'YYYY-MM-DD'.

    Raises:
        ValueError: If the file name does not match the required format 'ARM_YYYY-MM-DD.xlsx'.
    """
    pattern = r'^ARM_\d{4}-\d{2}-\d{2}\.xlsx$'
    if re.match(pattern, file_name):
        date_extracted = file_name.split('_')[1].split('.')[0]
        return date_extracted
    else:
        raise ValueError(f"File name '{file_name}' does not match format ARM_YYYY-MM-DD.xlsx")
    
def date_str_datetime(file_name):
    """
    Converts a date string from a given filename to a datetime object.

    Parameters:
        file_name (str): The filename from which the date string needs to be extracted and converted.

    Returns:
        tuple: A tuple containing the original date string and its corresponding datetime object.

    Raises:
        ValueError: If the extracted date string cannot be converted to a datetime object.
        Exception: If an error occurs while extracting date from the file name.
    """
    date_str = get_date_from_filename(file_name)
    date_format = '%Y-%m-%d'
    try:
        date_datetime = datetime.datetime.strptime(date_str, date_format)
        return date_str, date_datetime
    except ValueError as ve:
        raise ValueError(f"{date_str} cannot be converted. Verify file name. \nPython message: {ve}")
    except Exception as e:
        raise Exception(f"Error occurred while extracting date from a file name: {e}")
    
def get_file_path(folder_path, file_name):
    """
    Concatenates the folder path and file name to form the complete file path.

    Parameters:
        folder_path (str): The path of the folder where the file is located.
        file_name (str): The name of the file.

    Returns:
        str: The complete file path by joining folder_path and file_name.

    Example:
        get_file_path('/path/to/folder', 'data.csv')
    """
    file_path = folder_path + '/' + file_name
    return file_path

def top_rows_to_skip():
    """
    Generates a list of row indices to skip from the top of a dataset.

    Returns:
        list: A list containing the row indices to skip from the top of the dataset.
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17]
    """
    # Row 16 contains headers, hence it's not skipped
    rows_to_skip = [i for i in range(16)]
    rows_to_skip.append(17)
    return rows_to_skip

def get_data_from_xlsx(folder_path, file_name):
    """
    Reads data from an Excel file and returns a pandas DataFrame.

    Parameters:
        folder_path (str): The path of the folder where the Excel file is located.
        file_name (str): The name of the Excel file.

    Returns:
        pandas.DataFrame: A DataFrame containing the data from the Excel file.

    Raises:
        Exception: If an error occurs while reading data from xlsx.
    """
    try:
        file_path = get_file_path(folder_path=folder_path, file_name=file_name)
        top_skip = top_rows_to_skip()
        df = pd.read_excel(file_path, skiprows=top_skip, skipfooter=15)
        return df
    except Exception as e:
        raise Exception(f'Error while importing xlsx file {file_name}: \nMessage:{e}')
    
def trim_string(folder_path, file_name):
    """
    Trims leading and trailing whitespaces from folder_path and file_name.

    Parameters:
        folder_path (str): The path of the folder where the file is located.
        file_name (str): The name of the file.

    Returns:
        tuple: A tuple containing the trimmed folder_path and file_name.
    """
    folder_path_trimmed, file_name_trimmed = folder_path.strip(), file_name.strip()
    return folder_path_trimmed, file_name_trimmed

def extract_data(folder_path, file_name):
    """
    Extracts data from an Excel file and returns it along with the date extracted from the file name.

    Parameters:
        folder_path (str): The path of the folder where the Excel file is located.
        file_name (str): The name of the Excel file.

    Returns:
        tuple: A tuple containing:
            pandas.DataFrame: The DataFrame containing the data from the Excel file.
            str: The date in string format ('YYYY-MM-DD') extracted from the file name.
            datetime.datetime: The date as a datetime object extracted from the file name.
    """
    folder_path_adj, file_name_adj = trim_string(folder_path=folder_path, file_name=file_name)
    date_str, date_datetime = date_str_datetime(file_name=file_name_adj)
    df = get_data_from_xlsx(folder_path=folder_path_adj, file_name=file_name_adj)
    return df, date_str, date_datetime

# 1.2.1 Pre-transform

def adjust_col_headers(df):
    """
    Adjusts column headers of a pandas DataFrame by removing leading/trailing whitespaces, 
    replacing special characters (':', '.', and spaces) with empty strings, and converting 
    headers to lowercase.

    Parameters:
        df (pandas.DataFrame): The DataFrame whose column headers need to be adjusted.

    Returns:
        pandas.DataFrame: The DataFrame with adjusted column headers.

    Raises:
        Exception: If an error occurs while adjusting the headers' names.
    """
    try:
        df.columns = df.columns.str.strip()
        df.columns = df.columns.str.replace(':', '').str.replace('.', '').str.replace(' ', '')
        df.columns = df.columns.str.lower()
        return df
    except Exception as e:
        raise Exception(f"An error occurred while adjusting header names: {e}")
    
def adjust_select_columns(df):
    """
    Adjusts column headers of a DataFrame, selects specific columns, and returns the resulting DataFrame.

    Parameters:
        df (pandas.DataFrame): The DataFrame whose column headers need adjustment.

    Returns:
        pandas.DataFrame: The DataFrame with adjusted column headers and selected columns.
        
    Raises:
        KeyError: If one or more of the specified select_cols are not present in the DataFrame.
    """
    df_adj = adjust_col_headers(df)
    
    # Checks if headers are available in the Dataframe
    select_cols = ['unnamed0', 'unnamed1', 'unnamed4', 'balance', 'due1-30d', 'due31-60d', 'due61-90d', 'due91-120d', 'due>120d', 'citotal', 'sall12m', 'secbank', 'secother']
    missing_cols = set(select_cols) - set(df_adj.columns)
    if missing_cols:
        raise KeyError(f"The following columns are missing in the DataFrame: {', '.join(missing_cols)}")
    df_adj = df_adj[select_cols]
    return df_adj

def convert_float_to_int(df):
    """
    Converts columns of float type to integer type in a DataFrame.

    Parameters:
        df (pandas.DataFrame): The DataFrame containing float columns to convert.

    Returns:
        pandas.DataFrame: The DataFrame with converted float columns to integers.

    Raises:
        ValueError: If any of the float columns cannot be converted to integers.
    """
    # Save the current mode for chained assignment
    original_chained_assignment_mode = pd.options.mode.chained_assignment
    # Temporarily disable the warning for chained assignment
    pd.options.mode.chained_assignment = None

    # Convert float columns to integer type
    for col in df.select_dtypes(include='float').columns:
        try:
            df[col] = df[col].astype('int')
        except ValueError as ve:
            raise ValueError(f"Error while converting column '{col}' to integer: {ve}")

    # Restore the original mode for chained assignment
    pd.options.mode.chained_assignment = original_chained_assignment_mode
    return df

def rename_headers(df):
    """
    Rename columns in a DataFrame using a predefined translation dictionary.

    Args:
        df (pandas.DataFrame): The DataFrame whose columns need to be renamed.

    Returns:
        pandas.DataFrame: A new DataFrame with renamed columns.
    """
    headers_translate = {'unnamed0': 'long_credit_account', 'unnamed1': 'debtor_name', 'unnamed4': 'country_code'}
    df = df.rename(columns=headers_translate)
    return df

def filter_positive_balance(df):
    """
    Filter rows from a DataFrame where the 'balance' column has a positive value.

    Args:
        df (pandas.DataFrame): The DataFrame to filter.

    Returns:
        pandas.DataFrame: A new DataFrame containing only rows with positive 'balance'.
    """
    df = df.loc[df['balance'] > 0]
    return df

def filter_col_debtor_name(df):
    """
    Filters rows from a DataFrame based on specific debtor names and performs case normalization.

    Parameters:
        df (pandas.DataFrame): The DataFrame to be filtered and normalized.

    Returns:
        pandas.DataFrame: The filtered and normalized DataFrame.
    
    Raises:
        Exception: If error occurs when filtering column debtor_name
    """
    try:
        df.loc[:,'debtor_name'] = df.loc[:,'debtor_name'].str.lower()
        debtors_to_drop = ['aaa','bbb','ccc','ddd','eee']
        # Create a '|'-separated string from debtors_to_drop
        debtors_to_drop = '|'.join(debtors_to_drop)
        # Filter rows where debtor_name contains any of the specified names
        df = df.loc[~df['debtor_name'].str.contains(debtors_to_drop, na=False),:]
        df.loc[:,'debtor_name'] = df.loc[:,'debtor_name'].str.capitalize()
        return df
    except Exception as e:
        raise Exception(f"Error while filtering debtor names: {e}")
    
def split_col_long_credit_account(df):
    """
    Split the 'long_credit_account' column in a DataFrame by slash ('/') into multiple columns.

    Args:
        df (pandas.DataFrame): The DataFrame to split.

    Returns:
        pandas.DataFrame: A new DataFrame with the 'long_credit_account' column split.
    """
    df[['col_to_del', 'entity_code', 'credit_account']] = df['long_credit_account'].str.split('/', expand=True)
    df = df.drop(columns=['long_credit_account', 'col_to_del'])
    return df

def filter_col_entity_code(df):
    """
    Filter rows from a DataFrame based on specific entity codes.

    Args:
        df (pandas.DataFrame): The DataFrame to filter.

    Returns:
        pandas.DataFrame: A new DataFrame with rows removed for specific entity codes.

    Raises:
        Exception: If an error occurs while dropping selected entity codes
    """
    try:
        entity_code_to_drop = ['1', '2', '3', '4']
        entity_code_to_drop = '|'.join(entity_code_to_drop)
        df = df[~df['entity_code'].str.contains(entity_code_to_drop)]
        return df
    except Exception as e:
        raise Exception(f"Error while dropping entities: {e}")
    
def add_col_security(df):
    """
    Add a 'security' column to a DataFrame by combining 'secbank' and 'secother' columns.

    Args:
        df (pandas.DataFrame): The DataFrame to modify.

    Returns:
        pandas.DataFrame: A new DataFrame with the 'security' column added.
    """
    df['security'] = df['secbank'] + df['secother']
    df = df.drop(columns=['secbank', 'secother'])
    return df

def pre_transform_data(df):
    """
    Perform a series of data preprocessing steps on a pandas DataFrame.

    Parameters:
        df (pandas.DataFrame): The DataFrame to be preprocessed.

    Returns:
        pandas.DataFrame: A new DataFrame after completing all preprocessing steps.
    """
    df = adjust_select_columns(df)
    df = convert_float_to_int(df)
    df = rename_headers(df)
    df = filter_positive_balance(df)
    df = filter_col_debtor_name(df)
    df = split_col_long_credit_account(df)
    df = filter_col_entity_code(df)
    df = add_col_security(df)
    return df

# 1.2.2 Transform

def get_regions_df(filepath):
    """
    Read a CSV file containing region data and return a DataFrame with selected columns.

    Parameters:
        filepath (str): The path to the CSV file containing region data.

    Returns:
        pandas.DataFrame: A DataFrame containing 'entity_code' and 'tax_rate' columns.

    Raises:
        FileNotFoundError: If the file specified by 'filepath' is not found.
        Exception: If an error occurs while reading the CSV file or processing the data.
    """
    col_type_region = {'entity_code': str, 'tax_rate': float}
    try:
        d_region = pd.read_csv(filepath, sep=';', decimal=',', dtype=col_type_region)
        d_region = d_region.drop(columns=['entity_name', 'vat_insured'])
        return d_region
    except FileNotFoundError as fnfe:
        raise FileNotFoundError(f"File not found at '{filepath}': {fnfe}")
    except Exception as e:
        raise Exception(f"Error reading the CSV file: {e}")

def join_main_and_dregion(df_main, d_region):
    """
    Perform a left join between two DataFrames using the 'entity_code' column.

    Parameters:
        df_main (pandas.DataFrame): The main DataFrame to be joined.
        d_region (pandas.DataFrame): The DataFrame containing region data to be joined.

    Returns:
        pandas.DataFrame: A new DataFrame resulting from the left join operation.

    Raises:
        KeyError: If the 'entity_code' column is not found in both DataFrames.
        Exception: If an error occurs during the join operation.
    """
    try:
        df_joined = pd.merge(df_main, d_region, how='left', on='entity_code')
        return df_joined
    except KeyError as ke:
        raise KeyError(f"Column 'entity_code' not found in both DataFrames: {ke}")
    except Exception as e:
        raise Exception(f"Error occurred while joining DataFrames: {e}")

def calculate_uninsured_balance(df):
    """
    Calculate the uninsured balance for each row in the DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame containing necessary columns.

    Returns:
        pandas.DataFrame: A new DataFrame with the 'uninsured_balance' column added.

    Raises:
        Exception: If an error occurs during calculation of uninsured balance.
    """
    try:
        balance_minus_colateral_gross = df['balance'] - df['security']
        tax_rate_plus1 = 1 + df['tax_rate']
        uninsured_balance = (balance_minus_colateral_gross / tax_rate_plus1) - df['citotal']
        uninsured_balance_adj = np.maximum(uninsured_balance, 0)
        df['uninsured_balance'] = uninsured_balance_adj.astype('int')
        df = df.drop(columns="tax_rate")
        return df
    except Exception as e:
        raise Exception(f"Error while calculating uninsured balance: {e}")

def get_top40_by_region(df, reporting_date):
    """
    Get the top 40 records by 'balance' for each region in the DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame to group and filter.
        reporting_date (str): The reporting date to be added as a new column.

    Returns:
        pandas.DataFrame: A new DataFrame with the top 40 records by 'balance' for each region.

    Raises:
        Exception: If an error occurs during grouping top40 debtors by region.
    """
    try:
        df_top40_by_region = df.groupby('region').apply(lambda x: x.nlargest(40, 'balance')).reset_index(drop=True)
        df_top40_by_region = df_top40_by_region.drop(columns="region")
        df_top40_by_region = df_top40_by_region.reindex(columns=['entity_code', 'credit_account', 'debtor_name', 'country_code', 'balance', 'uninsured_balance', 'citotal', 'security', 'due1-30d', 'due31-60d', 'due61-90d', 'due91-120d', 'due>120d', 'sall12m'])
        df_top40_by_region.insert(0, 'date', reporting_date)
        return df_top40_by_region
    except Exception as e:
        raise Exception(f"Error occurred while getting top40 debtors by region: {e}")
    
def get_agg_by_entity_country(df, reporting_date):
    """
    Get aggregated data grouped by 'entity_code' and 'country_code' for the given DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame to group and aggregate.
        reporting_date (str): The reporting date to be added as a new column.

    Returns:
        pandas.DataFrame: A new DataFrame with aggregated data by 'entity_code' and 'country_code'.

    Raises:
        Exception: If an error occurs during grouping by entity code and country.
    """
    agg_entity_country = {
            'balance': 'sum',
            'uninsured_balance': 'sum',
            'due1-30d': 'sum',
            'due31-60d': 'sum',
            'due61-90d': 'sum',
            'due91-120d': 'sum',
            'due>120d': 'sum',
            'sall12m': 'sum'
        }
    try:
        df_entity_country = df.groupby(['entity_code', 'country_code'], as_index=False).agg(agg_entity_country)
        df_entity_country.insert(0, 'date', reporting_date)
        return df_entity_country
    except Exception as e:
        raise ValueError(f"Error occurred while aggregating by entity code and country: {e}")

def transform_data(df_main, reporting_date, filepath_dregion):
    """
    Perform a full series of data preprocessing steps on a pandas DataFrame.

    Parameters:
        df_main (pandas.DataFrame): The DataFrame to be preprocessed.
        reporting_date (datetime.datetime): The reporting date which data file refers to.
        filepath_dregion (str): The path to the CSV file containing region data.

    Returns:
        tuple: A tuple containing:
            pandas.DataFrame: A new DataFrame with the top 40 records by 'balance' for each region.
            pandas.DataFrame: A new DataFrame with aggregated data by 'entity_code' and 'country_code'.
    """
    df_main = pre_transform_data(df_main)
    df_region = get_regions_df(filepath=filepath_dregion)
    df_joined = join_main_and_dregion(df_main=df_main, d_region=df_region)
    df_joined = calculate_uninsured_balance(df=df_joined)
    df_top40_by_region = get_top40_by_region(df=df_joined, reporting_date=reporting_date)
    df_entity_country = get_agg_by_entity_country(df=df_joined, reporting_date=reporting_date)
    return df_top40_by_region, df_entity_country

# 1.3 Export

def load_to_csv(destination_path, df, df_export_name, reporting_date):
    """
    Export the DataFrame to a CSV file in the specified destination path.

    Args:
        destination_path (str): The path where the CSV file will be saved.
        df (pandas.DataFrame): The DataFrame to be exported.
        df_export_name (str): A descriptive name for the exported DataFrame.
        reporting_date (str): The reporting date used in the file name.

    Returns:
        None

    Raises:
        Exception: If a file cannot be exported to a CSV file
    """
    try:
        export_name = f'arm_{df_export_name}_{reporting_date}.csv'
        destination_path_file = destination_path + '/' + export_name
        df.to_csv(destination_path_file, index=None, sep=';')
    except Exception as e:
        raise Exception(f"Error occurred while exporting the DataFrame {df_export_name} to CSV: {e}")
    
def load_data(destination_path, reporting_date, df_top40, df_agg):
    """
    Load DataFrames to CSV files with appropriate export names and reporting date.

    Args:
        destination_path (str): The path where the CSV files will be saved.
        reporting_date (str): The reporting date used in the file names.
        df_top40 (pandas.DataFrame): The DataFrame containing top 40 records by region.
        df_agg (pandas.DataFrame): The DataFrame containing aggregated data by entity and country.

    Returns:
        None
    """
    load_to_csv(destination_path=destination_path, df=df_top40, df_export_name='top40_by_region', reporting_date=reporting_date)
    load_to_csv(destination_path=destination_path, df=df_agg, df_export_name='agg_by_region_country', reporting_date=reporting_date)

# 1.4 File paths

def etl_parameter_path(file_path):
    """
    Extracts ETL (Extract, Transform, Load) parameters from an Excel file.

    Args:
        file_path (str): The path to the Excel file containing the ETL parameters.

    Returns:
        tuple: A tuple containing the extracted folder paths and file paths.
            - folder_path_raw_data (str): The folder path for raw data.
            - file_path_dregion (str): The file path for DRegion.
            - folder_path_export (str): The folder path for exporting data.
            
    Raises:
        ValueError: If the Excel file does not contain the required sheet 'etl_parameters'.
        KeyError: If the 'Description' or 'Path' columns are not present in the 'etl_parameters' sheet.
        FileNotFoundError: If the provided file_path does not exist or cannot be found.
        pd.ExcelFileError: If there is an issue reading the Excel file.
        pd.DataFrameError: If there is an issue reading or processing data from the 'etl_parameters' sheet.
    """
    try:
        parameters = pd.read_excel(file_path, sheet_name='etl_parameters')
    except (FileNotFoundError, pd.ExcelFileError) as e:
        raise e

    try:
        parameter_paths = parameters.set_index('Description')['Path'].to_dict()
    except KeyError as e:
        raise KeyError("The 'etl_parameters' sheet must have 'Description' and 'Path' columns.") from e

    try:
        folder_path_raw_data, file_path_dregion, folder_path_export = tuple(parameter_paths.values())
    except ValueError as e:
        raise ValueError("The 'etl_parameters' sheet must contain exactly three rows.") from e
    
    return folder_path_raw_data, file_path_dregion, folder_path_export
