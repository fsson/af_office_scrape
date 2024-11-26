import os
import requests
import pandas as pd
from datetime import datetime

def fetch_data(page_number):
    """
    Fetches all JSON data from a specific page and converts it to a fully flattened DataFrame.
    """
    # Construct the URL with the page number
    url = f"https://arbetsformedlingen.se/rest/rusta-och-matcha-2/sokleverantor/leverantorer?sida={page_number}&tjanstekoder=A015"

    # Make the HTTP GET request
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Check if there are any rows in the data
        if not data:  # Empty data
            return None

        # Flatten the main JSON fields
        data_df = pd.json_normalize(data)

        # Flatten the 'adresser' field if it exists
        if 'adresser' in data_df.columns:
            # Explode the nested 'adresser' field
            data_df = data_df.explode('adresser')

            # Convert the dictionaries in 'adresser' to separate columns
            adresser_details = data_df['adresser'].dropna().apply(pd.json_normalize)
            adresser_details_df = pd.concat(adresser_details.tolist(), axis=0)

            # Reset index and join the expanded address details back to the main DataFrame
            data_df = data_df.reset_index(drop=True).drop(columns=['adresser'])
            data_df = pd.concat([data_df, adresser_details_df.reset_index(drop=True)], axis=1)

        return data_df
    else:
        print(f"Error: Unable to fetch data from page {page_number}")
        return None

def save_combined_data():
    """
    Fetches all data from the API, combines it, maps the columns to match the R script output, 
    and saves it to a CSV file.
    """
    # Initialize an empty list to store data from each page
    all_data = []

    # Start fetching data from page 1
    page_number = 1

    # Iterate through pages and fetch data until no more results
    while True:
        print(f"Fetching data from page {page_number}...")
        data = fetch_data(page_number)
        if data is None:  # Stop if no more data
            break
        all_data.append(data)
        page_number += 1

    # Combine all dataframes into one
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)

        # Add a date column
        today_date = datetime.now().strftime("%Y-%m-%d")
        combined_data['datum'] = today_date

        # Rename and map columns to match R script output
        column_mapping = {
            'id': 'id',
            'namn': 'namn',
            'nyval_tillatet': 'nyval_tillatet',
            'adressid': 'adresser.adressid',
            'adressrad': 'adresser.adressrad',
            'postnummer': 'adresser.postnummer',
            'postort': 'adresser.postort',
            'koordinater.latitud': 'adresser.koordinater.latitud',
            'koordinater.longitud': 'adresser.koordinater.longitud',
            'koordinater.north': 'adresser.koordinater.north',
            'koordinater.east': 'adresser.koordinater.east',
            'datum': 'datum'
        }
        combined_data = combined_data.rename(columns=column_mapping)

        # Filter to include only the columns we need
        required_columns = list(column_mapping.values())
        combined_data = combined_data[required_columns]

        # Construct the file name with today's date
        data_folder = os.path.join(os.path.dirname(__file__), "data")
        os.makedirs(data_folder, exist_ok=True)
        file_name = os.path.join(data_folder, f"office_location_{today_date}.csv")

        # Save the combined data to a CSV file
        combined_data.to_csv(file_name, index=False)
        print(f"Data successfully written to {file_name}")
        return True
    else:
        print("No data fetched.")
        return False

def combine_all_csvs():
    """
    Combines all CSV files in the 'data' folder into one and saves it in the main directory.
    """
    data_folder = os.path.join(os.path.dirname(__file__), "data")
    output_file = os.path.join(os.path.dirname(__file__), "combined_data.csv")

    # Get a list of all CSV files in the data folder
    csv_files = [os.path.join(data_folder, file) for file in os.listdir(data_folder) if file.endswith(".csv")]

    if not csv_files:
        print("No CSV files found in the 'data' folder.")
        return

    # Read and combine all CSV files
    combined_data = pd.concat([pd.read_csv(file) for file in csv_files], ignore_index=True)

    # Save the combined data to a CSV file in the main directory
    combined_data.to_csv(output_file, index=False)
    print(f"All CSV files combined and saved as {output_file}")

if __name__ == "__main__":
    # Step 1: Fetch, process, and save the data
    if save_combined_data():
        # Step 2: Combine all CSV files in the 'data' folder
        combine_all_csvs()

requests.get("https://hc-ping.com/8b6a6df5-ab9e-4ec5-be7d-4fd343495204")