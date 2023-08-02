import os
from tqdm import tqdm
import logging
import src.processing as pr

def main():

    logging.basicConfig(filename = './logs/logfile.log',
                        level = logging.ERROR,
                        format = '%(asctime)s %(levelname)s %(message)s')
    
    folder_path_raw_data, file_path_dregion, folder_path_export = pr.etl_parameter_path(file_path='./parameter_paths.xlsx')
    files_in_folder = os.listdir(folder_path_raw_data)

    with tqdm(total=len(files_in_folder), desc='Progress of ETL') as pbar:
        for item in files_in_folder:
            pbar.set_postfix_str(item)
            try:
                df, date_str, date_datetime = pr.extract_data(folder_path=folder_path_raw_data, file_name=item)
                logging.info(f'File {item} extracted successfully')
                df_top40_by_region, df_agg_entity_country = pr.transform_data(df_main=df, reporting_date=date_datetime, filepath_dregion=file_path_dregion)
                logging.info(f'File {item} transformed successfully')
                pr.load_data(destination_path=folder_path_export, reporting_date=date_str, df_top40=df_top40_by_region, df_agg=df_agg_entity_country)
                logging.info(f'File {item} loaded successfully')
            except (ValueError, Exception, KeyError, FileNotFoundError) as e:
                logging.error(f'Failed to execute ETL on file {item}: {e}')
                continue
            pbar.update()

if __name__ == "__main__":
    main()