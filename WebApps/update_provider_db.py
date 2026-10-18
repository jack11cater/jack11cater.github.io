# update_provider_db.py
#
# This script intelligently updates an EXISTING provider database with
# new or updated provider information from geocoded CSV files.
#
# It is NON-DESTRUCTIVE. Instead of rebuilding the database, it will:
# 1. INSERT new providers if their NPI is not found.
# 2. UPDATE existing providers if their NPI is found, merging any
#    new taxonomy codes with the old ones.
#
# Usage:
#   python update_provider_db.py <database.sqlite> <input_directory>
#
# Example:
#   python update_provider_db.py providers.sqlite ./new_geocoded_files/

import sqlite3
import csv
import sys
import os
import glob
import json
import re

def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <database.sqlite> <input_directory>")
        sys.exit(1)
        
    db_path = sys.argv[1]
    input_dir = sys.argv[2]
    
    if not os.path.isfile(db_path):
        print(f"Error: Database file not found at '{db_path}'.")
        print("Please run create_final_db.py first to create the initial database.")
        sys.exit(1)
        
    if not os.path.isdir(input_dir):
        print(f"Error: Input directory not found at '{input_dir}'")
        sys.exit(1)

    # Use glob to find all the geocoded CSV files to process
    csv_files = glob.glob(os.path.join(input_dir, '*providers_*.csv'))
    csv_files.sort()
    
    if not csv_files:
        print(f"Error: No 'providers_*.csv' files found in '{input_dir}'")
        sys.exit(1)
        
    print(f"Found {len(csv_files)} CSV files to process.")
    
    # This regex will extract the taxonomy code from filenames
    taxonomy_regex = re.compile(r'_([0-9A-Z]{10})')
    
    # --- Aggregation Step (Same as create_final_db.py) ---
    # We build a complete record of all providers to be updated/inserted
    providers_data = {}
    print("Aggregating data from CSV files...")
    
    for file_path in csv_files:
        filename = os.path.basename(file_path)
        print(f"  Processing '{filename}'...")
        
        match = taxonomy_regex.search(filename)
        if not match:
            print(f"    Warning: Could not extract taxonomy code from filename '{filename}'. Skipping.")
            continue
            
        taxonomy_code = match.group(1)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as infile:
                reader = csv.DictReader(infile)
                
                for row in reader:
                    # Skip rows that don't have coordinates
                    if not row.get('latitude') or not row.get('longitude') or \
                       row['latitude'] == '' or row['longitude'] == '':
                        continue
                        
                    npi = row.get('NPI')
                    if not npi:
                        continue
                    
                    if npi not in providers_data:
                        providers_data[npi] = {
                            'npi': npi,
                            'name': row.get('Name'),
                            'address': row.get('Address'),
                            'city': row.get('City'),
                            'state': row.get('State'),
                            'postal_code': row.get('Postal Code', '')[:5],
                            'telephone': row.get('Telephone'),
                            'latitude': float(row['latitude']),
                            'longitude': float(row['longitude']),
                            'taxonomy_codes': set() # Use a set to auto-handle duplicates
                        }
                    
                    # Add the taxonomy code from THIS FILENAME
                    providers_data[npi]['taxonomy_codes'].add(taxonomy_code)

        except FileNotFoundError:
            print(f"  Warning: Could not find file {file_path}. Skipping.")
        except Exception as e:
            print(f"  An error occurred processing {file_path}: {e}")

    # --- Database Update/Insert Step ---
    print(f"\nAggregation complete. Found {len(providers_data)} unique providers to upsert.")
    print("Connecting to database to perform updates...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    insert_list = []
    update_list = []
    
    for npi, data in providers_data.items():
        # Check if this NPI already exists in the database
        cursor.execute("SELECT taxonomy_codes FROM providers WHERE npi = ?", (npi,))
        existing_record = cursor.fetchone()
        
        new_codes_set = data['taxonomy_codes']
        
        if existing_record:
            # --- UPDATE ---
            # Provider exists, merge taxonomies
            try:
                # Load existing codes from the JSON string in the DB
                existing_codes = set(json.loads(existing_record[0]))
            except:
                existing_codes = set()
                
            merged_codes = existing_codes.union(new_codes_set)
            json_codes = json.dumps(sorted(list(merged_codes)))
            
            # Add to the list of providers to be updated
            update_list.append((
                data['name'],
                data['address'],
                data['city'],
                data['state'],
                data['postal_code'],
                data['telephone'],
                data['latitude'],
                data['longitude'],
                json_codes,
                npi # npi is last for the WHERE clause
            ))
        else:
            # --- INSERT ---
            # Provider is new
            json_codes = json.dumps(sorted(list(new_codes_set)))
            
            # Add to the list of providers to be inserted
            insert_list.append((
                data['npi'],
                data['name'],
                data['address'],
                data['city'],
                data['state'],
                data['postal_code'],
                data['telephone'],
                data['latitude'],
                data['longitude'],
                json_codes
            ))

    # --- Execute Transactions ---
    if insert_list:
        print(f"Inserting {len(insert_list)} new providers...")
        cursor.executemany("""
        INSERT INTO providers (npi, name, address, city, state, postal_code, telephone, latitude, longitude, taxonomy_codes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, insert_list)
        
    if update_list:
        print(f"Updating {len(update_list)} existing providers...")
        cursor.executemany("""
        UPDATE providers SET
            name = ?,
            address = ?,
            city = ?,
            state = ?,
            postal_code = ?,
            telephone = ?,
            latitude = ?,
            longitude = ?,
            taxonomy_codes = ?
        WHERE npi = ?
        """, update_list)

    conn.commit()
    conn.close()
    
    print("\n" + "="*30)
    print("Database update complete.")
    print(f"New providers inserted: {len(insert_list)}")
    print(f"Existing providers updated: {len(update_list)}")
    print(f"Database saved: '{db_path}'")

if __name__ == '__main__':
    main()
