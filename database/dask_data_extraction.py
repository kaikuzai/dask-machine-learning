import dask.dataframe as dd 

def load_hotel_data(file_path: str): 

    df = dd.read_csv(file_path)
    
    return df 