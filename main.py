import os 
import pandas as pd 

from database.mongo_storage import MongoDatabaseClient 
from database.data_formatter import DataFormatter

def main():

    cleaned_data_path = "data/Hotel_Reviews_Clean.csv"

    if not os.path.exists(cleaned_data_path):
        processor = DataFormatter()
        processor.clean_and_save_data()

    dataframe = pd.read_csv(cleaned_data_path)

    mongo = MongoDatabaseClient()
    mongo.connect()
    mongo.insert_data(dataframe=dataframe)
    mongo.create_indexes()
    mongo.client.close()

if __name__ == '__main__':
    main()