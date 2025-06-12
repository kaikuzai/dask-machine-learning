import pandas as pd 

raw_data_location = "data/Hotel_Reviews_Raw.csv"
cleaned_data_location = "data/Hotel_Reviews_Clean.csv"
delete_columns = ["Hotel_Address", "Tags", "days_since_review", "lat", "lng"]

class DataPreprocessor:
    def __init__(self, 
                 raw_data_location=raw_data_location,
                 cleaned_data_location=cleaned_data_location,
                 delete_columns=delete_columns):
        
        self.raw_data_location = raw_data_location
        self.cleaned_data_location = cleaned_data_location
        self.delete_columns = delete_columns

    def clean_and_save_data(self):
        df = pd.read_csv(raw_data_location)
        df.drop(columns=self.delete_columns, inplace=True)
        df.head()
        df.to_csv(cleaned_data_location)