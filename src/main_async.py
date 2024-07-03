import asyncio
import logging
import json
from datetime import datetime
from site_logic_async import SiteLogicAsync
from data_handler import save_json_as_csv, process_csv
from dotenv import load_dotenv
import agentql

load_dotenv()

urls = [
    "https://mercado.carrefour.com.br/",
    "https://superveneza.instabuy.com.br/",
    "https://www.paodeacucar.com/",
    "https://vitaliaparksul.instabuy.com.br/",
    "https://105sudoeste.bigboxdelivery.com.br/",
]
shopping_list = ["Corona", "Stella", "Becks", "Heineken"]
data_folder = "D:\\ProjectsAI\\WebBuyer\\data\\"

current_date_time = datetime.now().strftime("%d.%m.%Y_%H.%M")
file_name = f"{data_folder}price_verification_{current_date_time}.csv"

list_of_cookies = []
with open(r"./research/carrefour.txt", "r") as file:
    list_of_cookies = json.load(file)

logging.basicConfig(level=logging.DEBUG, filename=f"{data_folder}log_{current_date_time}.log", filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

async def handle_url(url):
    session = await agentql.start_async_session(url)
    site_logic = SiteLogicAsync(session, url)
    await site_logic.set_postal_code()
    for item in shopping_list:
        try:
            search_results = await site_logic.search_item(item)
            logging.info(f"Successfully processed {item} results at {url}")
            save_json_as_csv(search_results, file_name, url, item)
        except ValueError as ve:
            logging.error(f"ValueError processing {item} at {url}: {str(ve)}")
        except Exception as e:
            logging.error(f"Unexpected error processing {item} at {url}: {str(e)}")
        await asyncio.sleep(1)
    await session.stop()

async def main():
    tasks = [handle_url(url) for url in urls]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())

process_csv(file_name)
