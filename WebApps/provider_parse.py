# This script is designed to parse the large, full replacement NPPES NPI Downloadable File (CSV)
# and filter it for specific mental health provider taxonomies and states.
# It now dynamically reads the required column names from the separate Header File Document
# by searching for known column name fragments, making it resilient to index changes.
# This version outputs a schema specifically tailored for use with the geocoding scripts and
# sorts the output into separate files for each target taxonomy.

import csv
import sys
from datetime import datetime
from contextlib import ExitStack

# --- Configuration (ADJUST THESE VALUES) ---

# !!! REQUIRED: Set the file path to the extracted NPI data CSV file !!!
# Example: NPI_INPUT_FILE_PATH = 'npidata_pfile_20250910.csv'
NPI_INPUT_FILE_PATH = '/home/bean/Desktop/NPI File/September/npidata_pfile_20050523-20250907.csv'

# !!! REQUIRED: Set the file path to the Header File Document (text file) !!!
# This file contains the official column names in order.
NPI_HEADER_FILE_PATH = '/home/bean/Desktop/NPI File/September/npidata_pfile_20050523-20250907_fileheader.csv'

# The states and taxonomies you are interested in.
# NPIs must match at least one state AND at least one taxonomy.
TARGET_STATES = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA', 
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 
    'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 
    'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 
    'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
    
TARGET_TAXONOMIES = [
    '2084P0800X',  # Psychology, Clinical
    '101Y00000X',  # Counselor
    '101YA0400X',  # Counselor, Addiction (Substance Use Disorder)
    '101YM0800X',  # Counselor, Mental Health
    '103T00000X',  # Social Worker
    '103TA0700X',  # Social Worker, Clinical
    '207Q00000X',  # Psychiatry
    '207QA0000X',  # Psychiatry, Addiction
]

# --- Column Name Fragments (Used to find the full column name in the header file) ---
# We map simple constant names to unique fragments of the full column names.
NPI_COLUMN_FRAGMENTS = {
    'NPI_CODE': 'NPI',
    'ENTITY_TYPE_CODE': 'Entity Type Code',
    'ORG_NAME': 'Provider Organization Name',
    'LAST_NAME': 'Provider Last Name',
    'FIRST_NAME': 'Provider First Name',
    # UPDATED to use practice location address for better geocoding accuracy
    'ADDRESS_LINE_1': 'Provider First Line Business Practice Location Address',
    'CITY': 'Provider Business Practice Location Address City',
    'STATE': 'Provider Business Practice Location Address State',
    'POSTAL_CODE': 'Provider Business Practice Location Address Postal Code',
    'FIRST_TAXONOMY_CODE': 'Healthcare Provider Taxonomy Code_1',
}

NUM_TAXONOMIES = 15

# Global variable to store the dynamically mapped column names (Full Name -> Full Name)
COLUMN_MAP = {}
TAXONOMY_CODE_COLUMNS = []


def load_and_map_columns():
    """
    Reads the Header File Document and creates a dictionary (COLUMN_MAP) 
    by searching for the known fragments within the actual column names.
    This is highly resilient to changes in column order.
    """
    global COLUMN_MAP, TAXONOMY_CODE_COLUMNS
    
    if NPI_HEADER_FILE_PATH == '/path/to/your/npi_header_file.txt':
         print("ERROR: Please set 'NPI_HEADER_FILE_PATH' to the correct location of your Header File Document.", file=sys.stderr)
         return False

    try:
        with open(NPI_HEADER_FILE_PATH, 'r', encoding='utf-8') as header_file:
            content = header_file.read().strip()
            
        if not content:
            print("ERROR: Header file is empty or could not be read.", file=sys.stderr)
            return False
            
        reader = csv.reader([content], delimiter=',', quotechar='"')
        
        try:
            full_header_names = next(reader)
        except StopIteration:
            print("ERROR: Header file is empty or could not be parsed as CSV.", file=sys.stderr)
            return False
        
        if len(full_header_names) < 200:
            print(f"ERROR: Only {len(full_header_names)} columns were parsed from the header file. Expected 200+.", file=sys.stderr)
            return False
            
        for key, fragment in NPI_COLUMN_FRAGMENTS.items():
            found_name = next((name for name in full_header_names if fragment in name), None)
            
            if found_name:
                COLUMN_MAP[key] = found_name
            else:
                print(f"ERROR: Could not find column containing fragment '{fragment}'.", file=sys.stderr)
                return False

        try:
            first_code_name = COLUMN_MAP['FIRST_TAXONOMY_CODE']
            first_code_idx = full_header_names.index(first_code_name)
        except ValueError:
             print("ERROR: Failed to find the full name of the first taxonomy code in the header list.", file=sys.stderr)
             return False

        for i in range(NUM_TAXONOMIES):
            code_idx = first_code_idx + i
            if code_idx < len(full_header_names):
                TAXONOMY_CODE_COLUMNS.append(full_header_names[code_idx])
            else:
                 print(f"WARNING: Could not map all {NUM_TAXONOMIES} taxonomy codes. Only mapped {i}.", file=sys.stderr)
                 break
                 
        print(f"Successfully loaded and mapped {len(COLUMN_MAP)} main columns and {len(TAXONOMY_CODE_COLUMNS)} taxonomy codes.")
        return True

    except FileNotFoundError:
        print(f"ERROR: Header file not found at '{NPI_HEADER_FILE_PATH}'.", file=sys.stderr)
        return False
    except Exception as e:
        print(f"An unexpected error occurred while processing the header file: {e}", file=sys.stderr)
        return False


def extract_provider_data(row):
    """
    Extracts information from a row, formatted for geocoding.
    """
    try:
        npi = row.get(COLUMN_MAP.get('NPI_CODE'), 'N/A')
        entity_type_code = row.get(COLUMN_MAP.get('ENTITY_TYPE_CODE'), '1')
        
        if entity_type_code == '1':
            first_name = row.get(COLUMN_MAP.get('FIRST_NAME'), '')
            last_name = row.get(COLUMN_MAP.get('LAST_NAME'), '')
            name = f"{first_name} {last_name}".strip()
        else:
            name = row.get(COLUMN_MAP.get('ORG_NAME'), 'N/A')

        address = row.get(COLUMN_MAP.get('ADDRESS_LINE_1'), 'N/A')
        city = row.get(COLUMN_MAP.get('CITY'), 'N/A')
        state = row.get(COLUMN_MAP.get('STATE'), 'N/A')
        # Ensure postal code is only the first 5 digits
        postal_code = row.get(COLUMN_MAP.get('POSTAL_CODE'), 'N/A')[:5]
        
        # --- UPDATED: Return list matches the new, geocoder-friendly schema ---
        return [npi, name, address, city, state, postal_code]
    except Exception as e:
        print(f"Error processing NPI {row.get(COLUMN_MAP.get('NPI_CODE', 'N/A'))}. Error: {e}", file=sys.stderr)
        return None

def process_npi_file():
    """
    Main function to read, filter, and write the NPI data into separate files per taxonomy.
    """
    if NPI_INPUT_FILE_PATH == '/path/to/your/npi_data.csv':
         print("ERROR: Please set 'NPI_INPUT_FILE_PATH' to the correct location of your downloaded NPI data file.", file=sys.stderr)
         return

    if not load_and_map_columns():
        print("Fatal error during column name initialization. Exiting.", file=sys.stderr)
        return

    # A set of the target taxonomies for quick lookups
    target_tax_set = set(TARGET_TAXONOMIES)
    
    print(f"Starting to process data file: {NPI_INPUT_FILE_PATH}. This may take a while...")
    print(f"Filtering for states: {', '.join(TARGET_STATES)}")
    print(f"Output will be sorted into separate files for each of {len(TARGET_TAXONOMIES)} taxonomies.")
    sys.stdout.flush()

    try:
        with ExitStack() as stack:
            # --- UPDATED: Open a file for each taxonomy ---
            file_writers = {}
            for taxonomy in TARGET_TAXONOMIES:
                filename = f"providers_{taxonomy}_{datetime.now().strftime('%Y-%m-%d')}.csv"
                outfile = stack.enter_context(open(filename, 'w', newline='', encoding='utf-8'))
                writer = csv.writer(outfile)
                
                # Write the header to each new file
                header = ['NPI', 'Name', 'Address', 'City', 'State', 'Postal Code']
                writer.writerow(header)
                file_writers[taxonomy] = writer

            # Dictionaries to track seen NPIs and counts for each output file
            seen_npis_per_file = {taxonomy: set() for taxonomy in TARGET_TAXONOMIES}
            counters = {taxonomy: 0 for taxonomy in TARGET_TAXONOMIES}

            infile = stack.enter_context(open(NPI_INPUT_FILE_PATH, 'r', encoding='utf-8'))
            reader = csv.DictReader(infile, delimiter=',', quotechar='"')
            
            # --- Main File Processing Loop ---
            for row_number, row in enumerate(reader, 1):
                if row_number % 500000 == 0:
                    print(f"Processed {row_number:,} rows...")
                    sys.stdout.flush()
                    
                provider_state = row.get(COLUMN_MAP.get('STATE'), '')
                if provider_state not in TARGET_STATES:
                    continue
                
                # Check all taxonomy columns in the row
                for col in TAXONOMY_CODE_COLUMNS:
                    provider_taxonomy = row.get(col)
                    
                    # If the provider's taxonomy is one we're targeting...
                    if provider_taxonomy and provider_taxonomy in target_tax_set:
                        extracted_data = extract_provider_data(row)
                        npi = extracted_data[0]
                        
                        # ...and we haven't already written this NPI to this specific file...
                        if npi != 'N/A' and npi not in seen_npis_per_file[provider_taxonomy]:
                            # ...write it to the correct file.
                            file_writers[provider_taxonomy].writerow(extracted_data)
                            seen_npis_per_file[provider_taxonomy].add(npi)
                            counters[provider_taxonomy] += 1
                            
        print(f"\n--- SUCCESS ---")
        print("File processing complete. Results sorted by taxonomy:")
        for taxonomy, count in counters.items():
             print(f" - providers_{taxonomy}_{datetime.now().strftime('%Y-%m-%d')}.csv: {count:,} records")

    except FileNotFoundError:
        print(f"ERROR: Input file not found at '{NPI_INPUT_FILE_PATH}'. Please update the path.", file=sys.stderr)
    except KeyError as e:
        print(f"ERROR: Column name error. The column '{e}' was not found in the data file.", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)

if __name__ == '__main__':
    process_npi_file()

