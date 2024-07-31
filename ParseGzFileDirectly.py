import ijson
import os
import gzip
import sqlite3
from decimal import Decimal

def connect_to_DB(title):
    con = sqlite3.connect(title)
    cur = con.cursor()
    cur.execute('CREATE TABLE provider_group(group_id, tin_type, tin_value)')
    cur.execute(
        'CREATE TABLE in_network(id, billing_code, name, billing_code_type, billing_code_type_version, negotiation_arrangement, description)')
    cur.execute(
        'CREATE TABLE negotiated_rates(negotiated_billing_code, provider_references, negotiated_type, negotiated_rate, expiration_date, billing_class, billing_code_modifier)')
    cur.execute('CREATE TABLE provider(NPI, provider_group_id)')
    con.commit()
    return con

def convert_value(value):
    if isinstance(value, Decimal):
        return float(value)
    return value

def insert_into_table(con, table, row_vals):
    cur = con.cursor()
    # Convert all Decimal values to float
    row_vals = {k: convert_value(v) for k, v in row_vals.items()}
    placeholder = [":" + key for key in row_vals]
    sql_row = f"INSERT INTO {table} VALUES({', '.join(placeholder)})"
    try:
        cur.execute(sql_row, row_vals)
    except Exception as e:
        print(f"Error inserting into {table}: {e}")
        print(sql_row)
        print(row_vals)
        exit()
    con.commit()

def close_sql_connection(con):
    con.close()

def JSON_to_SQL(JSONfile):
    # Open the JSON file and create a parser
    with gzip.open(JSONfile, 'rt', encoding='utf-8') as f:
        parser = ijson.parse(f)
        id = 0

        # Remove the existing database if it exists
        if os.path.exists("KPNCAL.db"):
            os.remove("KPNCAL.db")

        # Connect to the SQLite database
        con = connect_to_DB("KPNCAL.db")

        # Initialize state variables
        in_network_vars = {}
        negotiated_rates_vars = {}
        inside_negotiated_rates = False
        inside_negotiated_prices = False

        # Define mappings for JSON to database column names
        in_net = {
            'in_network.item.negotiation_arrangement': 'negotiation_arrangement',
            'in_network.item.billing_code': 'billing_code',
            'in_network.item.name': 'name',
            'in_network.item.billing_code_type': 'billing_code_type',
            'in_network.item.billing_code_type_version': 'billing_code_type_version',
            'in_network.item.description': 'description'
        }
        neg_prices = {
            'in_network.item.negotiated_rates.item.negotiated_prices.item.negotiated_rate': 'negotiated_rate',
            'in_network.item.negotiated_rates.item.negotiated_prices.item.negotiated_type': 'negotiated_type',
            'in_network.item.negotiated_rates.item.negotiated_prices.item.expiration_date': 'expiration_date',
            'in_network.item.negotiated_rates.item.negotiated_prices.item.billing_class': 'billing_class',
            'in_network.item.negotiated_rates.item.negotiated_prices.item.billing_code_modifier': 'billing_code_modifier'
        }

        # Process JSON data
        for prefix, event, value in parser:
            if 'provider_references' in prefix:
                if event == 'start_array':
                    # Initialize provider_references field
                    negotiated_rates_vars['provider_references'] = ''
                elif event == 'end_array':
                    # End of provider_references field
                    continue
                elif prefix == 'in_network.item.negotiated_rates.item.provider_references.item':
                    # Append provider_reference values
                    negotiated_rates_vars['provider_references'] += "|" + str(value)

            if 'in_network' in prefix:
                if [prefix, event] == ['in_network.item', 'start_map']:
                    # Initialize in_network_vars when a new in_network item starts
                    in_network_vars = {
                        'id': None,
                        'billing_code': None,
                        'name': None,
                        'billing_code_type': None,
                        'billing_code_type_version': None,
                        'negotiation_arrangement': None,
                        'description': None
                    }
                elif prefix == 'in_network.item.billing_code':
                    # Set billing_code and assign unique ID
                    in_network_vars['billing_code'] = value
                    in_network_vars['id'] = id
                elif prefix in in_net:
                    # Map JSON fields to database column names
                    in_network_vars[in_net[prefix]] = value
                elif prefix == 'in_network.item.negotiated_rates':
                    if event == 'start_array':
                        inside_negotiated_rates = True
                elif inside_negotiated_rates and [prefix, event] == ['in_network.item.negotiated_rates.item', 'start_map']:
                    # Initialize negotiated_rates_vars when a new negotiated_rates item starts
                    negotiated_rates_vars = {
                        'negotiated_billing_code': in_network_vars['id'],
                        'provider_references': '',
                        'negotiated_type': None,
                        'negotiated_rate': None,
                        'expiration_date': None,
                        'billing_class': None,
                        'billing_code_modifier': None
                    }
                elif inside_negotiated_rates and prefix == 'in_network.item.negotiated_rates.item.negotiated_prices':
                    if event == 'start_array':
                        inside_negotiated_prices = True
                elif inside_negotiated_prices and [prefix, event] == ['in_network.item.negotiated_rates.item.negotiated_prices.item', 'start_map']:
                    # Initialize negotiated_prices fields
                    continue
                elif inside_negotiated_prices and prefix in neg_prices:
                    # Map JSON fields to database column names
                    negotiated_rates_vars[neg_prices[prefix]] = value
                elif inside_negotiated_prices and [prefix, event] == ['in_network.item.negotiated_rates.item.negotiated_prices.item', 'end_map']:
                    # End of negotiated_prices item
                    continue
                elif inside_negotiated_rates and [prefix, event] == ['in_network.item.negotiated_rates.item', 'end_map']:
                    # Insert negotiated_rates_vars into the database
                    insert_into_table(con, 'negotiated_rates', negotiated_rates_vars)
                elif [prefix, event] == ['in_network.item', 'end_map']:
                    # Insert in_network_vars into the database
                    insert_into_table(con, 'in_network', in_network_vars)
                    in_network_vars = {}
                    inside_negotiated_rates = False
                    inside_negotiated_prices = False

            # Print progress every 10,000 records
            if id % 10000 == 0:
                print(".", end="")
            if id >= 2000000:
                # Close database connection after processing 2,000,000 records
                close_sql_connection(con)
                break
            id += 1

        # Final cleanup
        close_sql_connection(con)

if __name__ == '__main__':
    JSONfile='2024-07-01_2024-07-01_BIND-BENEFITS-INC-_Third-Party-Administrator_OHPH-Acupuncture-Massage-Naturopath_31_in-network-rates.json.gz'
    JSON_to_SQL(JSONfile)
