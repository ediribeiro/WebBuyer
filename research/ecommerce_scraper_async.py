import asyncio
import csv
import time
from datetime import datetime
from dotenv import load_dotenv
import agentql

load_dotenv()

urls = [
    "https://superveneza.instabuy.com.br/",
    "https://www.paodeacucar.com/",
    "https://vitaliaparksul.instabuy.com.br/",
    "https://www.bigboxdelivery.com.br/",
]
shopping_list = ["Corona", "Stella", "Becks", "Heineken"]
data_folder = "D:\\ProjectsAI\\WebBuyer\\data\\"

HOME_QUERY = """
{
    header {
        search_box
    }
}
"""

SEARCH_QUERY = """
{
    results {
        products[] {
            product_description
            product_price
            product_promotion_price
        }
    }
}
"""

async def save_json_as_csv_async(json_data, file_name, url, item):
    csv_header = [
        "url",
        "item",
        "product_description",
        "product_price",
        "product_promotion_price",
    ]
    products = json_data.get("results", {}).get("products", [])
    async with aiofiles.open(file_name, "a", newline="") as file:
        writer = csv.writer(file)
        if file.tell() == 0:
            await writer.writerow(csv_header)
        for product in products:
            product_description = product.get("product_description", "").lower()
            if product_description.startswith("cerveja"):
                product_price = product.get("product_price", "")
                product_promotion_price = product.get("product_promotion_price", "")
                await writer.writerow(
                    [url, item, product_description, product_price, product_promotion_price]
                )
        await asyncio.sleep(1)

async def fetch_prices(url, item, file_name):
    session = await agentql.start_async_session(url)
    try:
        home_page = await session.query(HOME_QUERY)
        home_page.header.search_box.fill(item)
        home_page.header.search_box.press("Enter")
        await asyncio.sleep(1)
        search_results = await session.query(SEARCH_QUERY)
        await save_json_as_csv_async(search_results.to_data(), file_name, url, item)
    finally:
        await session.close()



async def main():
    tasks = []
    current_date_time = datetime.now().strftime("%d.%m.%Y_%H.%M")
    file_name = f"{data_folder}price_verification_{current_date_time}.csv"
    for url in urls:
        for item in shopping_list:
            tasks.append(fetch_prices(url, item, file_name))
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
