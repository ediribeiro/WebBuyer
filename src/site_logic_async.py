import asyncio
import logging

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

class SiteLogicAsync:
    def __init__(self, session, url):
        self.session = session
        self.url = url

    async def set_postal_code(self):
        self.log_query("LOCATE_QUERY")
        if self.url == "https://mercado.carrefour.com.br/":
            cep_set = await self.session.query(LOCATE_QUERY)
            await cep_set.cep_btn.click(force=True)
            cep_data = await self.session.query(LOCATE_QUERY)
            await cep_data.cep_box.fill("71218-010")
            await cep_data.cep_btn.click(force=True)

    def log_query(self, query_name):
        logging.debug(f"querying - {query_name}")

    async def search_item(self, item, max_retries=3):
        self.log_query("HOME_QUERY")
        home_page = await self.session.query(HOME_QUERY)
        await home_page.header.search_box.fill(item)
        await home_page.header.search_box.press("Enter")
        await asyncio.sleep(2)  # Increase wait time to ensure page load
        
        retries = 0
        while retries < max_retries:
            self.log_query("SEARCH_QUERY")
            search_results = await self.session.query(SEARCH_QUERY)
            if search_results.results.products:
                filtered_products = [
                    product for product in search_results.results.products 
                    if item.lower() in product.get("product_description", "").lower()
                ]
                if filtered_products:
                    return {"results": {"products": filtered_products}}
            retries += 1
            logging.warning(f"No products found for {item} at {self.url}. Retrying {retries}/{max_retries}.")
            await asyncio.sleep(2)  # Wait before retrying

        raise ValueError(f"No products found for {item} at {self.url} after {max_retries} retries.")
