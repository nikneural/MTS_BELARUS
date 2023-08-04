import pandas as pd
from abc import ABC


class DataLoader(ABC):
    def __init__(self, **paths):
        self.paths = paths

    @staticmethod
    def _read_data(file_path: str):
        pass

    @staticmethod
    def _clean_data(df: pd.DataFrame) -> pd.DataFrame:
        pass


class AlfaLoader(DataLoader):
    @staticmethod
    def _clean_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Alfa Data Function

        This function takes a pandas DataFrame containing Alfa data and performs the following steps:
        1. Renames the columns 'Phone', 'Date', and 'Identification number' to 'phone_number', 'date', and 'identify',
           respectively.
        2. Selects only the 'date', 'phone_number', and 'identify' columns from the DataFrame.

        Parameters:
            df (pd.DataFrame): The input DataFrame containing Alfa data.

        Returns:
            pd.DataFrame: A cleaned DataFrame with columns 'date', 'phone_number', and 'identify'.
        """

        # Step 1: Rename the columns
        df_cleaned = df.rename(
            columns={
                "Phone": "phone_number",
                "Date": "date",
                "Identification number": "identify",
            }
        )

        # Step 2: Select the desired columns
        df_cleaned = df_cleaned.loc[:, ["date", "phone_number", "identify"]]

        return df_cleaned

    def load_alfa_data(self):
        """
        Loads and cleans the Alfa data from an Excel file.

        This method reads the Alfa data from the specified Excel file and processes it
        to clean any inconsistencies or missing values. The cleaned data is returned as a pandas DataFrame.

        Returns:
            pd.DataFrame or None: The cleaned Alfa data as a pandas DataFrame if successful,
                                  otherwise returns None if an error occurs during loading or cleaning.
        """
        try:
            # Load Alfa data from the Excel file
            print("Loading Alfa data...")
            alfa_data = pd.read_excel(self.paths["alfa"], sheet_name="Sheet1", header=0)
            print("Alfa data loaded successfully.")

            # Clean Alfa data
            print("Cleaning Alfa data...")
            cleaned_alfa_data = self._clean_data(alfa_data)
            print("Alfa data cleaned successfully.")

            return cleaned_alfa_data
        except Exception as e:
            # If any exception occurs during loading or cleaning, catch it and print an error message
            print(f"Error while loading or cleaning Alfa data: {e}")
            return None


class BpsLoader(DataLoader):
    @staticmethod
    def _read_data(csv_file_path: str) -> pd.DataFrame:
        """
        Read data from a CSV file and return a pandas DataFrame with specific data types and parsed dates.

        Parameters:
            csv_file_path (str): The path to the CSV file.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the read data.

        Note:
            The CSV file should have columns 'TEL', 'APLCTN_DT', and 'PD'.
            The 'TEL' column should contain integer values.
            The 'APLCTN_DT' column should contain date values in string format.
            The 'PD' column should contain integer values, and it won't be included in the DataFrame.
        """
        # Define the columns to be used in the DataFrame
        columns_to_use = ['TEL', 'APLCTN_DT']

        # Specify the data types for each column
        data_types = {
            "TEL": 'int64',
            "APLCTN_DT": 'string',
        }

        # Specify the 'APLCTN_DT' column as a date to be parsed during reading
        parse_dates = ['APLCTN_DT']

        # Read the CSV file into a DataFrame with the specified parameters
        df = pd.read_csv(csv_file_path, sep=';', usecols=columns_to_use, dtype=data_types, parse_dates=parse_dates,
                         low_memory=False)

        return df

    @staticmethod
    def _clean_data(input_df: pd.DataFrame) -> pd.DataFrame:
        # Create a copy of the input DataFrame to avoid modifying the original data
        df = input_df.copy()

        # Rename columns using more descriptive names
        renamed_columns = {
            "TEL": "phone_number",
            "APLCTN_DT": "date",
            # Add more column renames if needed
        }
        df.rename(columns=renamed_columns, inplace=True)

        # Select the desired columns in the desired order
        selected_columns = ["date", "phone_number"]
        df = df[selected_columns]

        return df

    def load_bps_data(self):
        """
        Method to load and clean BPS data from a CSV file.

        Returns:
            pandas.DataFrame or None: A DataFrame containing the cleaned BPS data,
            or None if an error occurred during data processing.
        """
        try:
            print("Loading BPS data...")
            # Read data from CSV file
            data_df = self._read_data(csv_file_path=self.paths['bps'])
            print("Data loaded successfully.")

            print("Cleaning BPS data...")
            # Clean the data
            cleaned_df = self._clean_data(input_df=data_df)
            print("Data cleaned successfully.")

            # Make a copy of the cleaned data to avoid modifying the original DataFrame
            bps_train_df = cleaned_df.copy()

            return bps_train_df

        except Exception as e:
            print("An error occurred during data processing:", e)
            return None

class InstallmentLoader(DataLoader):
    @staticmethod
    def _clean_data(input_df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and select relevant columns from the input DataFrame.

        Parameters:
            input_df (pd.DataFrame): The input DataFrame with the following columns:
                - "Действующий абонентский номер" (str): The phone number of the subscriber.
                - "Дата продажи" (str or datetime): The date of the sale.

        Returns:
            pd.DataFrame: A new DataFrame with the cleaned data containing the following columns:
                - "date" (str or datetime): The date of the sale.
                - "phone_number" (str): The phone number of the subscriber.
        """
        cleaned_df = input_df.rename(columns={
            "Действующий абонентский номер": 'phone_number',
            "Дата продажи": 'date',
        })

        cleaned_df = cleaned_df.loc[:, ['date', 'phone_number']]
        return cleaned_df

    def load_installment_data(self):
        """
        Loads installment data from an Excel file and performs data cleaning.

        Returns:
            pd.DataFrame or None: A cleaned DataFrame containing 'Действующий абонентский номер'
                                  and 'Дата продажи' columns, or None if an error occurs during processing.
        """
        try:
            print("Loading Installment data...")
            # Load the installment data from the Excel file into a DataFrame
            new_data = pd.read_excel(self.paths['installment'], sheet_name="Лист1", header=0)
            # Keep only the 'Действующий абонентский номер' and 'Дата продажи' columns
            new_data = new_data[['Действующий абонентский номер', 'Дата продажи']]
            print("Data loaded successfully.")

            print("Cleaning Lizing data...")
            # Clean the data using the internal _clean_data method
            train_df_installment = self._clean_data(new_data)
            print("Data cleaned successfully.")

            return train_df_installment
        except Exception as e:
            # If an error occurs during data processing, print the error message and return None
            print("An error occurred during data processing:", e)
            return None

class LizingLoader(DataLoader):
    @staticmethod
    def _clean_data(df: pd.DataFrame) -> pd.DataFrame:
        train_df = df.rename(columns={
            "Мобильный телефон": 'phone_number',
            "Дата заключения": 'date',
        })

        train_df = train_df.loc[:, ['date', 'phone_number']]
        return train_df

    def load_lizing_data(self):
        try:
            print("Loading Lizing data...")
            # Load the installment data from the Excel file into a DataFrame
            new_data = pd.read_excel(self.paths['lizing'], sheet_name="Sheet1", header=0)
            print("Data loaded successfully.")

            print("Cleaning Lizing data...")
            # Clean the data using the internal _clean_data method
            train_df_lizing = self._clean_data(new_data)
            print("Data cleaned successfully.")

            return train_df_lizing
        except Exception as e:
            # If an error occurs during data processing, print the error message and return None
            print("An error occurred during data processing:", e)
            return None
