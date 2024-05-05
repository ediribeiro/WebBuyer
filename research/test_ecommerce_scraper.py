import agentql
from agentql.ext.playwright import PlaywrightWebDriverSync
from dotenv import load_dotenv
import csv
import json
from datetime import datetime
import time

load_dotenv()
urls = [
    "https://superveneza.instabuy.com.br/",
    #"https://www.paodeacucar.com/",
    #"https://vitaliaparksul.instabuy.com.br/",
    #"https://www.bigboxdelivery.com.br/",
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

def save_json_as_csv(json_data, file_name, url):
    products = json_data.get("results", {}).get("products", [])
    with open(file_name, "a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        for product in products:
            product_description = product.get("product_description", "").lower()
            if product_description.startswith('cerveja'):
                product_price = product.get("product_price", "")
                product_promotion_price = product.get("product_promotion_price", "")
                writer.writerow(
                    [url, product_description, product_price, product_promotion_price]
                )
                print(f"Saved data for {product_description} from {url}")
                time.sleep(1)

# Set up the Playwright web driver and start a new agentql session
driver = PlaywrightWebDriverSync(headless=False)

# Get the current date and time
current_date_time = datetime.now().strftime("%d.%m.%Y_%H:%M")
print(current_date_time)

# Create a new file name with the date pattern
file_name = f"{data_folder}catch_beer_{current_date_time}.csv"

# Write the header to the CSV file
with open(file_name, "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow([
        "url",
        "product_description",
        "product_price",
        "product_promotion_price",
    ])

# Run query on home page & go to search result
for url in urls:
    session = agentql.start_session(url, web_driver=driver)
    for item in shopping_list:
        home_page = session.query(HOME_QUERY)
        home_page.header.search_box.fill(item)
        home_page.header.search_box.press("Enter")
        time.sleep(1)
        search_results = session.query(SEARCH_QUERY)
        print(f"Search results for {item} on {url}:")
        print(search_results.to_data())  # Debugging
        save_json_as_csv(search_results.to_data(), file_name, url)
        time.sleep(1)
    session.stop()
