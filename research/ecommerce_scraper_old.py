import agentql
from agentql.ext.playwright import PlaywrightWebDriverSync
from dotenv import load_dotenv
import csv
import json
import time
from datetime import datetime

load_dotenv()

urls = [
    #"https://mercado.carrefour.com.br/",
    "https://superveneza.instabuy.com.br/",
    "https://www.paodeacucar.com/",
    "https://vitaliaparksul.instabuy.com.br/",
    "https://105sudoeste.bigboxdelivery.com.br/",
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

# Helper function to save JSON data as CSV
def save_json_as_csv(json_data, file_name, url, item):
    csv_header = [
        "url",
        "item",
        "product_description",
        "product_price",
        "product_promotion_price",
    ]
    products = json_data.get("results", {}).get("products", [])
    with open(file_name, "a", newline="") as file:
        # Open file in append mode
        writer = csv.writer(file)
        # Check if the file is empty and write header if necessary
        if file.tell() == 0:
            writer.writerow(csv_header)
        # Write data for each product
        for product in products:
            product_description = product.get("product_description", "").lower()
            if product_description.startswith('cerveja'):
                product_price = product.get("product_price", "")
                product_promotion_price = product.get("product_promotion_price", "")
                writer.writerow(
                    [url, item, product_description, product_price, product_promotion_price]
                )
    time.sleep(1)

# Set up the Playwright web driver and start a new agentql session
driver = PlaywrightWebDriverSync(headless=False)

# Get the current date and time
current_date_time = datetime.now().strftime("%d.%m.%Y_%H.%M")

# Create a new file name with the date patter
file_name = f"{data_folder}price_verification_{current_date_time}.csv"

# Run query on home page & go to search result
for url in urls:
    # Start session
    session = agentql.start_session(url, web_driver=driver)

    for item in shopping_list:
        # Run query on home page & go to search result
        home_page = session.query(HOME_QUERY)
        home_page.header.search_box.fill(item)
        home_page.header.search_box.press("Enter")
        # Wait for the page to load completely
        time.sleep(1)
        # Run search query on search page
        search_results = session.query(SEARCH_QUERY)
        print(search_results)
        save_json_as_csv(search_results.to_data(), file_name, url, item)
        time.sleep(1)
    session.stop()