from dotenv import load_dotenv
from datetime import datetime
import json
import time
import logging
import random
from session_manager import SessionManager
from site_logic import SiteLogic
from data_handler import save_json_as_csv, process_csv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

urls = [
    "https://mercado.carrefour.com.br/",
    "https://superveneza.instabuy.com.br/",
    "https://www.paodeacucar.com/",
    "https://vitaliaparksul.instabuy.com.br/",
    "https://105sudoeste.bigboxdelivery.com.br/",
]
shopping_list = ["Stella", "Becks", "Corona", "Heineken"]
data_folder = "D:\\ProjectsAI\\WebBuyer\\data\\"

current_date_time = datetime.now().strftime("%d.%m.%Y_%H.%M")
file_name = f"{data_folder}price_verification_{current_date_time}.csv"

logging.basicConfig(level=logging.DEBUG, filename=f"{data_folder}log_{current_date_time}.log", filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def handle_url(url):
    session_manager = SessionManager(url)
    site_logic = SiteLogic(session_manager.session, url)
    site_logic.set_postal_code()
    max_retries = 3
    local_shopping_list = shopping_list[:]
    random.shuffle(local_shopping_list)
    for item in local_shopping_list:
        retries = 0
        while retries < max_retries:
            try:
                search_results = site_logic.search_item(item)
                logging.info(f"Successfully processed {item} results at {url}")
                save_json_as_csv(search_results, file_name, url, item)
                break  # Break the retry loop if successful
            except ValueError as ve:
                logging.error(f"ValueError processing {item} at {url}: {str(ve)}")
                break  # No need to retry on ValueError
            except Exception as e:
                logging.error(f"Unexpected error processing {item} at {url}: {str(e)} - Retry {retries + 1}")
                retries += 1
                if retries < max_retries:
                    time.sleep(2 ** retries)  # Exponential backoff
                else:
                    logging.error(f"Failed to process {item} at {url} after {max_retries} retries.")
        time.sleep(2)
    session_manager.stop()

def main():
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(handle_url, url) for url in urls]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error in thread execution: {str(e)}")

    process_csv(file_name)

if __name__ == "__main__":
    main()
