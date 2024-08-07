import agentql
from agentql.ext.playwright import PlaywrightWebDriverSync
from dotenv import load_dotenv
from datetime import datetime
import csv
import json
import time
from data_handler import process_csv


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

LOCATE_QUERY = """
{
    cep_btn
    cep_box
}
"""

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
            product_discount_price
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
        "product_discount_price",
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
            if product_description.startswith("cerveja"):
                product_price = product.get("product_price", "")
                product_discount_price = product.get("product_discount_price", "")
                writer.writerow(
                    [url, item, product_description, product_price, product_discount_price]
                )
    time.sleep(1)

# Get the current date and time
current_date_time = datetime.now().strftime("%d.%m.%Y_%H.%M")

# Create a new file name with the date pattern
file_name = f"{data_folder}price_verification_{current_date_time}.csv"

# Load cookies from text file
list_of_cookies = []
with open(r"./research/carrefour.txt", "r") as file:
    list_of_cookies = json.load(file)

# Set up the Playwright web driver
driver = PlaywrightWebDriverSync(headless=False)

# Run query on home page & go to search result
for url in urls:
    # Start session
    session = agentql.start_session(url, web_driver=driver)
    # Set zip to this site
    if url == "https://mercado.carrefour.com.br/":
        cep_set = session.query(LOCATE_QUERY)
        cep_set.cep_btn.click(force=True)
        cep_data = session.query(LOCATE_QUERY)
        cep_data.cep_box.fill("71218-010")
        cep_data.cep_btn.click(force=True)
    
    for item in shopping_list:
        # Run query on home page & go to search result
        home_page = session.query(HOME_QUERY)
        home_page.header.search_box.fill(item)
        home_page.header.search_box.press("Enter")
        # Wait for the page to load completely
        time.sleep(1)
        # Run search query on search page
        search_results = session.query(SEARCH_QUERY)
        if not search_results.results.products:
        # Retry the search query if the first result is empty
            search_results = session.query(SEARCH_QUERY)
            if not search_results.results.products:
                print("No products found for", item)
                continue
            else:
                print(search_results)
                # Save JSON data as CSV
                save_json_as_csv(search_results, file_name, url, item)


        save_json_as_csv(search_results.to_data(), file_name, url, item)
        time.sleep(1)
    
    # Stop session
    session.stop()

process_csv(file_name)