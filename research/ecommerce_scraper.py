import agentql
from agentql.ext.playwright import PlaywrightWebDriverSync
from dotenv import load_dotenv
import csv
import time

load_dotenv()


urls = [
    "https://superveneza.instabuy.com.br/",
    "https://www.mercadolivre.com.br/",
    "https://www.paodeacucar.com/",
    "https://www.extra.com.br/",
    "https://vitaliaparksul.instabuy.com.br/",
    "https://www.bigboxdelivery.com.br/",
    "https://www.amazon.com.br/"
]

"""urls = [
    "https://superveneza.instabuy.com.br/",
    "https://www.paodeacucar.com/",
]"""

#url = "https://superveneza.instabuy.com.br/"
product = "cerveja"
file_name = "D:\ProjectsAI\WebBuyer\data\catch_beer.csv"


# Helper function to save JSON data as CSV
def save_json_as_csv(json_data, file_name, url):
    csv_header = [
        "url",
        
        "product_description",
        "product_price",
        "product_promotion_price",
    ]
    products = json_data.get("results", {}).get("products", [])
    with open(file_name, "a", newline="") as file:  # Open file in append mode
        writer = csv.writer(file)

        # Check if the file is empty and write header if necessary
        if file.tell() == 0:
            writer.writerow(csv_header)

        # Write data for each product
        for product in products:

            product_description = product.get("product_description", "")
            product_price = product.get("product_price", "")
            product_promotion_price = product.get("product_promotion_price", "")

            writer.writerow(
                [url, product_description, product_price, product_promotion_price]
            )


HOME_QUERY = """
{
        search_box
        search_btn
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

# Set up the Playwright web driver and start a new agentql session
driver = PlaywrightWebDriverSync(headless=False)



# Run query on home page & go to search result
for url in urls:
    # Start session for each URL
    session = agentql.start_session(url, web_driver=driver)

    # Run query on home page & go to search result
    home_page = session.query(HOME_QUERY)
    home_page.search_box.fill(product)
    home_page.search_btn.click(force=True)

    time.sleep(1)

    search_results = session.query(SEARCH_QUERY)
    print(search_results)

    save_json_as_csv(search_results.to_data(), file_name, url)

    session.stop()
