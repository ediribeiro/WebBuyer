import csv
import re
from collections import defaultdict

# Dictionaries
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
    "lata" : "lata",
    "garrafa" : "garrafa",
    "longneck" : "garrafa",
    "ln" : "garrafa",
    "barril" : "barril",
    "keg" : "barril"
}

def extract_quantity(description):
    match = re.search(r'\b(\d+)\s*unidades?\b', description, re.IGNORECASE)
    return int(match.group(1)) if match else 1

def extract_volume_and_package(description):
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

def find_lowest_price_per_type(data):
    # Dictionary to store the lowest price per liter for each type of item
    lowest_prices = defaultdict(lambda: {'price': float('inf'), 'product': None})

    # Iterate through the data and update lowest prices per type
    for row in data:
        product_type = row['item']
        price_per_liter = row.get('price_per_liter', float('inf'))
        if price_per_liter < lowest_prices[product_type]['price']:
            lowest_prices[product_type]['price'] = price_per_liter
            lowest_prices[product_type]['product'] = row
        if price_per_liter == lowest_prices[product_type]['price'] and row['url']!= lowest_prices[product_type]['product']['url']:
            lowest_prices[product_type]['product']['url'] += ','+ row['url']
    return lowest_prices

def process_csv(input_file):
    # Read input CSV file and load into list of dictionaries
    with open(input_file, mode='r', newline='', encoding='latin-1') as infile:
        reader = csv.DictReader(infile)
        data = [row for row in reader]
    
    
    # Add new columns for volume and package type
    for row in data:
        description = row['product_description']
        volume, volume_value, package = extract_volume_and_package(description)
        row['volume'] = volume
        row['volume_value'] = volume_value
        row['package_type'] = package

        # Calculate price per litre
        price_str = row.get('product_price', row.get('product_discount_prince', '')).replace('R$', '').strip().replace(',', '.')  # Remove currency symbol, spaces and commas
        promo_price_str = row.get('product_discount_prince', row.get('product_price', '')).replace('R$', '').strip().replace(',', '.')  # Remove currency symbol, spaces and commas 
        
        # Handle cases where price is not a number
        try:
            price = float(price_str)
        except ValueError:
            price = float('inf')  # Assign infinity as the sentinel value for invalid prices

        try:
            promo_price = float(promo_price_str)
        except ValueError:
            promo_price = float('inf')  # Assign infinity as the sentinel value for invalid prices

        # Calculate the minimum price
        min_price = min(price, promo_price)
        if min_price == float('inf'):
            continue  # Skip rows with invalid prices

        # Extract quantity of units from description
        quantity = extract_quantity(description)
        if quantity == 1:
            unit_price = min_price  
        else:
            if round(price/quantity, 2) == min_price or round(promo_price/quantity, 2) == min_price:
                unit_price = min_price
            else:
                unit_price = min_price / quantity # Calculate unit price if quantity is greater than 1
        row['unit_price'] = unit_price
        
        # Calculate price per litre
        price_per_liter = unit_price / volume_value * 1000
        row['price_per_liter'] = price_per_liter

    # Write modified data to output CSV file
    fieldnames = data[0].keys()
    output_file = f"{input_file}_processed.csv"
    with open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    # Find and print lowest price per liter for each type of item
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
            print(f"URL: {url}")
            print()
        else:
            print("No valid prices found for this product type.")