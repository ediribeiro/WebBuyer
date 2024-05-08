import pandas as pd
import csv
import re

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
    # Regular expression to find a number followed by "unidades" or "unidade"
    match = re.search(r'\b(\d+)\s*unidades?\b', description, re.IGNORECASE)
    if match:
        return int(match.group(1))
    else:
        return 1  # Default to 1 unit if no quantity is specified

def extract_volume_and_package(description):
    volume = None
    volume_value = None
    package = None
    
    # Remove spaces from the description
    description = description.replace(" ", "")

    # Iterate over volume_beer_unit dictionary to find matches
    for key, value in volume_beer_unit.items():
        if key in description:
            volume = key
            volume_value = value
            break
    
    # Iterate over package_type list to find matches
    for key, value in package_type.items():
        if key in description:
            package = value
            break
    
    return volume, volume_value, package

def process_csv(input_file, output_file):
    # Read input CSV file into a pandas DataFrame
    df = pd.read_csv(input_file, encoding='latin-1')

    # Remove currency symbol, spaces, and commas from price columns
    df['product_price'] = df['product_price'].str.replace('R$', '').str.strip().str.replace(',', '.')
    df['product_promotion_price'] = df['product_promotion_price'].str.replace('R$', '').str.strip().str.replace(',', '.')

    # Convert price columns to float
    df['product_price'] = df['product_price'].astype(float)
    df['product_promotion_price'] = df['product_promotion_price'].astype(float)

    # Calculate the minimum price between product_price and product_promotion_price
    df['min_price'] = df[['product_price', 'product_promotion_price']].min(axis=1)

    # Extract volume and package type
    df['volume'] = df['product_description'].apply(lambda x: extract_volume_and_package(x)[0])
    df['volume_value'] = df['product_description'].apply(lambda x: extract_volume_and_package(x)[1])
    df['package_type'] = df['product_description'].apply(lambda x: extract_volume_and_package(x)[2])

    # Calculate unit price
    df['quantity'] = df['product_description'].apply(extract_quantity)
    df['unit_price'] = df.apply(lambda row: row['min_price'] / row['quantity'], axis=1)

    # Calculate price per liter
    df['price_per_liter'] = (df['unit_price'] / df['volume_value']) * 1000

    # Find the product with the lowest price per liter
    lowest_price_product = df.loc[df['price_per_liter'].idxmin()]

    # Print the relevant information
    print(f"Lowest price per liter: R${lowest_price_product['price_per_liter']:.2f}")
    print(f"Product Description: {lowest_price_product['product_description']}")
    print(f"Unit Price: R${lowest_price_product['unit_price']:.2f}")
    print(f"URL: {lowest_price_product['url']}")
    
    # Write modified data to output CSV file
    df.to_csv(output_file, index=False, encoding='utf-8')

# Example usage
input_file = "D:\ProjectsAI\WebBuyer\data\price_verification_05.05.2024_13.56.csv"
output_file = f"{input_file}_processed.csv"

process_csv(input_file, output_file)