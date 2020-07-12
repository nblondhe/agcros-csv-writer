import sys
import requests
import logging
import json
import csv
import os
import glob
from timeit import default_timer as timer
import argparse
import uuid

API_URL = 'https://gpsr.ars.usda.gov/agcrospublicapi/api/v1'

def get_agcros_data(endpoint, offset, limit):
    """
        Automates creation of csv files containing data retrieved from 
        the AgCROS public API.
        https://gpsr.ars.usda.gov/agcrospublicapi/swagger/index.html (Swagger API doc)

        :param endpoint: API endpoint with leading slash, as: /Measurement/SoilChemistry
        :param offset: Records returnable in one call (integer value, default 2000)
        :param limit: Total records returned (integer value, default 0)
        :return: returns nothing
    """
    cursor = 0
    status = ''
    total_records_retrieved = 0
    requests_made = 0
    total_records_available = 0

    print(f"\nRequesting data from AgCROS endpoint: {endpoint} ... \n")
    while cursor <= total_records_available:
        try:
            response = requests.get(API_URL + endpoint, {'offset': cursor, 'limit': limit})
        except requests.exceptions.Timeout as e:
            print(f'Request to {API_URL + endpoint} timed out. Please try again.')
            logging.WARN(e)
            raise SystemExit(e)
        except requests.exceptions.TooManyRedirects as e:
            print('Bad Request. Please check your parameters and try again.')
            logging.WARN(e)
            raise SystemExit(e)
        except requests.exceptions.ConnectionError as e:
            logging.WARN(e)
            raise SystemExit(e)
        except requests.exceptions.RequestException as e:
            logging.WARN(e)
            raise SystemExit(e)
        except KeyboardInterrupt:
            raise
            print('Program exiting...')



        csv_filename = write_csv(response, endpoint)
        
        total_records_retrieved += json.loads(response.content)['resultCount']
        total_records_available = json.loads(response.content)['totalCount']
        cursor += offset
        requests_made += 1
        
        print(f"Total records retrieved {total_records_retrieved}")
        print(f"Cursor {cursor}")
        print(f"Requests made {requests_made}")

    status = f'**** Data successfully written to {csv_filename} ****'
    return status, csv_filename

def write_csv(response, endpoint):
    id = uuid.uuid4().hex
    response_dict = json.loads(response.content)
    results = response_dict['result']
    log_current =  f"First request returned status code {str(response.status_code)}, result count: {response_dict['resultCount']} of {response_dict['totalCount']} total results\n"
    logging.info(log_current)

    endpoint_paths = endpoint.split('/')
    endpoint_parent = endpoint_paths[1]
    endpoint_child = endpoint_paths[2]
    csv_file_prefix = f"agcros-api-{endpoint_parent}-{endpoint_child}"
    csv_file_name = f"{csv_file_prefix}-{id}.csv"

    previous_files = glob.glob(csv_file_prefix + "*")
    if previous_files and previous_files[0] != csv_file_name:
        os.remove(previous_files[0])
        print('Previous file version found. Deleting...')

    write_method = 'a' if os.path.isfile(csv_file_name) else 'w'
    with open(csv_file_name, write_method, newline='') as f:
        writer = csv.DictWriter(f, results[0].keys())
        if write_method == 'w':
            writer.writeheader()
        for result in results:
            writer.writerow(result)
    return csv_file_name

if __name__ == "__main__":
    start_time = timer()
    logging.basicConfig(filename='agCros-csv-writer.log',level=logging.DEBUG)
    status_msg = ''

    parser = argparse.ArgumentParser()
    parser.add_argument("endpoint", help="The AgCROS endpoint to send a request to")
    parser.add_argument("--offset", type=int, default=2000, help="Optional: the number of records to skip before taking the indicated number of records, defaults to 0")
    parser.add_argument("--limit", type=int, default=0, help="Optional: the number of number of records to retrieve, defaulted to 2000")
    
    args = parser.parse_args()
    limit = args.limit if args.limit > 0 else args.offset 
    print(f"endpoint {args.endpoint} limit {limit} offset {args.offset}")
    status_msg, outFileName = get_agcros_data(args.endpoint, args.offset, limit)
        
    elapsed = timer() - start_time
    time_msg = f'**** Finished execution in {elapsed} seconds ****\n'
    print (status_msg) 
    print (time_msg)
    logging.info(time_msg)
