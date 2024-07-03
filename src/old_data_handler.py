import csv
import re
import logging
from collections import defaultdict
import time
from concurrent.futures import ThreadPoolExecutor

# Define dictionaries
volume_beer_unit = {
    "5l": 5000,
    "600ml": 600,
    "350ml": 350,
    "330ml": 330,
    "275ml": 275,
    "269ml": 269,
    "250ml": 250,
    "210ml": 210
}

package_type = {
    "lata": "lata",
    "garrafa": "garrafa",
    "longneck": "garrafa",
    "ln": "garrafa",
    "barril": "barril",
    "keg": "barril"
}

def save_json_as_csv(json_data, file_name, url, item):
    """
    Saves the product data in JSON format to a CSV file.
    """
    csv_header = [
        "url", "item", "product_description", "product_price", "product_discount_price"
    ]
    products = json_data.get("results", {}).get("products", [])
    with open(file_name, "a", newline="") as file:
        writer = csv.writer(file)
        if file.tell() == 0:
            writer.writerow(csv_header)
        for product in products:
            product_description = product.get("product_description", "").lower()
            if product_description.startswith("cerveja"):
                product_price = product.get("product_price", "")
                product_discount_price = product.get("product_discount_price", "")
                writer.writerow(
                    [url, item, product_description, product_price, product_discount_price]
                )
    logging.info(f"Saved {len(products)} products from {url} to {file_name}")
    time.sleep(1)

def extract_quantity(description):
    """
    Extracts the quantity of units from the product description.
    """
    match = re.search(r'\b(\d+)\s*(?:unidades?|und|pack|pacote|caixa|cx|un|pç)\b', description, re.IGNORECASE)
    if match:
        return int(match.group(1))
    else:
        return 1

def extract_volume_and_package(description):
    """
    Extracts the volume and package type from the product description.
    """
    volume, volume_value, package = None, None, None
    description = description.replace(" ", "")
    for key, value in volume_beer_unit.items():
        if key in description:
            volume, volume_value = key, value
            break
    for key, value in package_type.items():
        if key in description:
            package = value
            break
    return volume, volume_value, package

def extract_price(price_str):
    """
    Extracts and converts the price from string to float.
    """
    price_str = price_str.replace('R$', '').strip().replace(',', '.')
    try:
        return float(price_str)
    except ValueError:
        logging.warning(f"Invalid price encountered: {price_str}")
        return float('inf')  # Assign infinity as the sentinel value for invalid prices

def calculate_unit_price(min_price, price, promo_price, quantity):
    """
    Calculates the unit price of the product. Logs potential data issues.
    """
    if quantity == 1:
        return min_price
    else:
        if price and promo_price != float('inf'):
            if round(price / quantity, 2) <= min_price or round(promo_price / quantity, 2) <= min_price:
                return min_price
            else:
                logging.warning(f"Assuming min_price ({min_price}) is total price. "
                                f"Price: {price}, Promo Price: {promo_price}, Quantity: {quantity}")
                return min_price / quantity
        else:
            logging.warning(f"Invalid price detected. Assuming min_price ({min_price}) is total price. "
                            f"Price: {price}, Promo Price: {promo_price}, Quantity: {quantity}")
            return min_price / quantity

def calculate_price_per_liter(unit_price, volume_value):
    """
    Calculates the price per liter of the product.
    """
    return unit_price / volume_value * 1000

def process_csv(input_file):
    """
    Processes the input CSV file to calculate unit prices and prices per liter,
    and writes the modified data to an output CSV file.
    """
    logging.info(f"Starting processing of {input_file}")
    data = read_csv(input_file)
    
    # Use ThreadPoolExecutor to process data in parallel
    with ThreadPoolExecutor() as executor:
        data = list(executor.map(process_product_row, data))
    
    output_file = write_processed_data(input_file, data)
    find_and_print_lowest_prices(data)
    logging.info(f"Completed processing of {input_file}")
    return output_file

def read_csv(input_file):
    """
    Reads the input CSV file and returns the data as a list of dictionaries.
    """
    with open(input_file, mode='r', newline='', encoding='latin-1') as infile:
        reader = csv.DictReader(infile)
        data = [row for row in reader]
    logging.info(f"Read {len(data)} rows from {input_file}")
    return data

def process_product_row(row):
    """
    Processes a single product row to extract volume, package type, and calculate prices.
    """
    description = row['product_description']
    volume, volume_value, package = extract_volume_and_package(description)
    row['volume'] = volume
    row['volume_value'] = volume_value
    row['package_type'] = package

    price = extract_price(row.get('product_price', row.get('product_discount_price', '')))
    promo_price = extract_price(row.get('product_discount_price', row.get('product_price', '')))

    min_price = min(price, promo_price)
    if min_price == float('inf'):
        logging.warning(f"Skipping row due to invalid price: {row}")
        return row  # Skip rows with invalid prices

    quantity = extract_quantity(description)
    unit_price = calculate_unit_price(min_price, price, promo_price, quantity)
    row['unit_price'] = unit_price

    price_per_liter = calculate_price_per_liter(unit_price, volume_value)
    row['price_per_liter'] = price_per_liter

    return row

def write_processed_data(input_file, data):
    """
    Writes the processed data to a new CSV file and returns the output file path.
    """
    fieldnames = data[0].keys()
    output_file = f"{input_file}_processed.csv"
    with open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    logging.info(f"Wrote processed data to {output_file}")
    return output_file

def find_and_print_lowest_prices(data):
    """
    Finds and prints the lowest price per liter for each type of product.
    """
    lowest_prices = find_lowest_price_per_type(data)
    for product_type, info in lowest_prices.items():
        lowest_price = info['price']
        lowest_product = info['product']
        print(f"Lowest price per liter for {product_type}: R${lowest_price:.2f}")
        if lowest_product is not None:
            product_description = lowest_product['product_description']
            unit_price = lowest_product['unit_price']
            url = lowest_product.get('url', '')
            print(f"Product Description: {product_description}")
            print(f"Unit Price: R${unit_price}")
            print(f"URL: {url}\n")
        else:
            print("No valid prices found for this product type.")


def find_lowest_price_per_type(data):
    """
    Finds the lowest price per liter for each type of item in the data.
    """
    lowest_prices = defaultdict(lambda: {'price': float('inf'), 'product': None})
    for row in data:
        try:
            product_type = row['item']
            price_per_liter = row.get('price_per_liter', float('inf'))
            
            if price_per_liter < lowest_prices[product_type]['price']:
                lowest_prices[product_type]['price'] = price_per_liter
                lowest_prices[product_type]['product'] = row
            elif price_per_liter == lowest_prices[product_type]['price'] and row['url'] != lowest_prices[product_type]['product']['url']:
                lowest_prices[product_type]['product']['url'] += ', ' + row['url']
        
        except TypeError as e:
            logging.warning(f"Skipping row due to TypeError: {e}")
            continue
        
        except KeyError as e:
            logging.warning(f"Skipping row due to missing key: {e}")
            continue

    return lowest_prices

process_csv("data/price_verification_28.05.2024_21.50.csv")