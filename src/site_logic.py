import time
import logging
import os
from dotenv import load_dotenv

load_dotenv()

LOCATE_QUERY = """
{
    cep_btn
    cep_text_box
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
            product_link
            product_description
            product_price
            product_discount_price
        }
    }
}
"""

class SiteLogic:
    def __init__(self, session, url):
        self.session = session
        self.url = url

    def set_postal_code(self):
        postal_code = os.getenv("POSTAL_CODE")
        if self.url == "https://mercado.carrefour.com.br/":
            cep_data = self.session.query(LOCATE_QUERY)
            cep_data.cep_btn.click(force=True)
            cep_set = self.session.query(LOCATE_QUERY)
            cep_set.cep_text_box.fill(postal_code)
            cep_set.cep_btn.click(force=True)
    
    def querying(self, query_name):
        logging.debug(f"querying - {query_name}")

    def search_item(self, item, max_retries=3):
        home_page = self.session.query(HOME_QUERY)
        home_page.header.search_box.fill(item)
        home_page.header.search_box.press("Enter")
        time.sleep(2)
        
        retries = 0
        while retries < max_retries:
            search_results = self.session.query(SEARCH_QUERY)
            if search_results.results.products:
                return search_results.to_data()
            retries += 1
            logging.warning(f"No products found for {item} at {self.url}. Retrying {retries}/{max_retries}.")
            time.sleep(1)
        
        raise ValueError(f"No products found for {item} at {self.url} after {max_retries} retries.")

