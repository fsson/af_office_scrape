import os
import requests
import pandas as pd
from datetime import datetime
import subprocess

def fetch_data(page_number):
    """
    Fetches all JSON data from a specific page and converts it to a fully flattened DataFrame.
    """
    url = f"https://arbetsformedlingen.se/rest/rusta-och-matcha-2/sokleverantor/leverantorer?sida={page_number}&tjanstekoder=A015"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if not data:
            return None
        data_df = pd.json_normalize(data)
        if 'adresser' in data_df.columns:
            data_df = data_df.explode('adresser')
            adresser_details = data_df['adresser'].dropna().apply(pd.json_normalize)
            adresser_details_df = pd.concat(adresser_details.tolist(), axis=0)
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
    all_data = []
    page_number = 1
    while True:
        print(f"Fetching data from page {page_number}...")
        data = fetch_data(page_number)
        if data is None:
            break
        all_data.append(data)
        page_number += 1

    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        today_date = datetime.now().strftime("%Y-%m-%d")
        combined_data['datum'] = today_date
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
        required_columns = list(column_mapping.values())
        combined_data = combined_data[required_columns]
        data_folder = os.path.join(os.path.dirname(__file__), "data")
        os.makedirs(data_folder, exist_ok=True)
        file_name = os.path.join(data_folder, f"office_location_{today_date}.csv")
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
    csv_files = [os.path.join(data_folder, file) for file in os.listdir(data_folder) if file.endswith(".csv")]

    if not csv_files:
        print("No CSV files found in the 'data' folder.")
        return

    combined_data = pd.concat([pd.read_csv(file) for file in csv_files], ignore_index=True)
    combined_data.to_csv(output_file, index=False)
    print(f"All CSV files combined and saved as {output_file}")

def push_to_github():
    """
    Pushes the latest changes to the GitHub repository.
    """
    try:
        repo_path = os.path.dirname(__file__)
        os.chdir(repo_path)

        subprocess.run(["git", "pull"], check=True)

        subprocess.run(["git", "add", "."], check=True)

        commit_message = f"Automated update on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        subprocess.run(["git", "commit", "-m", commit_message], check=True)

        subprocess.run(["git", "push"], check=True)
        print("Changes pushed to GitHub successfully!")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while pushing to GitHub: {e}")

if __name__ == "__main__":
    if save_combined_data():
        combine_all_csvs()
        push_to_github()

    requests.get("https://hc-ping.com/8b6a6df5-ab9e-4ec5-be7d-4fd343495204")
