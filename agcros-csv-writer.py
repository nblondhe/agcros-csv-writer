import sys
import requests
import logging
import json
import csv
import os.path
from timeit import default_timer as timer
import argparse


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
    url = 'https://gpsr.ars.usda.gov/agcrospublicapi/api/v1'
    totalRecords = getTotalRecords(url, endpoint)

    #  Need better algo (Send requests until recordCount = totalRecords)
    if totalRecords > offset:
        maxRequests = int(round(totalRecords / offset))
    else:
        maxRequests = 1

    cursor = 0
    status = ''
    print(f"\nGetting data from AgCROS endpoint: {endpoint} ... \n")
    url = 'https://gpsr.ars.usda.gov/agcrospublicapi/api/v1'
    for req in range(0, maxRequests):
        try:
            response = requests.get(url + endpoint, {offset: cursor, limit: limit})
        except requests.exceptions.Timeout:
            print(f'Request to {url + endpoint} timed out. Please try again.')
            raise
        except requests.exceptions.TooManyRedirects:
            print('Bad Request. Please check your parameters and try again.')
            raise
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)

        csv_filename = write_csv(response, endpoint)
        cursor += offset

    status = f'**** Data successfully written to {csv_filename} ****'
    return status, csv_filename


def getTotalRecords(url, endpoint):
    try:
         response = requests.get(url + endpoint, {'offset': 0, 'limit': 1})
    except requests.exceptions.Timeout:
        return f'Request timed to {url + endpoint} out. Please try again.'
    except requests.exceptions.TooManyRedirects:
        return 'Bad Request. Please check your parameters and try again.'
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)
    
    response_dict = json.loads(response.content)
    totalRecords = response_dict['totalCount']
    return totalRecords

def write_csv(response, endpoint):
    response_dict = json.loads(response.content)
    results = response_dict['result']
    log_current =  f"First request returned status code {str(response.status_code)}, result count: {response_dict['resultCount']} of {response_dict['totalCount']} total results\n"
    logging.debug('Current Request ')
    logging.info(log_current)

    endpoint_paths = endpoint.split('/')
    endpoint_parent = endpoint_paths[1]
    endpoint_child = endpoint_paths[2]
    csv_file_name = f"agcros-api-{endpoint_parent}-{endpoint_child}.csv"
    
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
    status_msg, outFileName = get_agcros_data(args.endpoint, args.offset, args.limit)
        
    elapsed = timer() - start_time
    print (status_msg) 
    print ('**** Finished execution in {elapsed} seconds ****\n')
