import sys
import requests
import json
import csv
import os.path
from timeit import default_timer as timer

def get_agcros_data(endpoint):
    """
        Automates creation of csv files containing data retrieved from 
        the AgCROS public API.
        https://gpsr.ars.usda.gov/agcrospublicapi/swagger/index.html (Swagger API doc)

        :param endpoint: API endpoint with leading slash, as: /Measurement/SoilChemistry
        :param offset: Records returnable in one call (Integer value)
        :param limit: Total records returned (Integer value)
        :param totalRecords: Total record quantity (Integer value)
        :return: returns nothing
    """
    # TODO user defined params
    totalRecords=58000
    limit=10
    offset=2000
    
    # if totalRecords > 1:
    #     maxCalls = int(round(totalRecords / offset))
    # else: 
    #     maxCalls = 1
    
    # ----------------

    log = []
    current_offset = 0

    maxCalls = 1

    print '\nGetting data from AgCROS endpoint: "{0}" ... \n'.format(endpoint)
    url = 'https://gpsr.ars.usda.gov/agcrospublicapi/api/v1'
    for req, i in enumerate(range(0, maxCalls)):
        try:
            response = requests.get(url + endpoint, {offset: current_offset, limit: limit})
        except Exception as e:
            print str(e)
            return 'Request to AgCROS API unsuccessful. Please try again.'
        log = build_response_log(log, response)
        csv_filename = write_csv(response, endpoint)
        current_offset += offset

    write_log_file(log)
    status = '**** Data successfully written to {0} ****'.format(csv_filename)
    return status

def write_csv(response, endpoint):
    response_dict = json.loads(response.content)
    results = response_dict['result']

    endpoint_paths = endpoint.split('/')
    endpoint_parent = endpoint_paths[1]
    endpoint_child = endpoint_paths[2]
    csv_file_name = "agcros-api-{0}-{1}.csv".format(endpoint_parent, endpoint_child)
    
    write_method = 'ab' if os.path.isfile(csv_file_name) else 'wb'
    with open(csv_file_name, write_method) as f:
        writer = csv.DictWriter(f, results[0].keys())
        if write_method == 'wb':
            writer.writeheader()
        for result in results:
            writer.writerow(result)
    return csv_file_name

def build_response_log(log, response):
    response_dict = json.loads(response.content)
    log_current =  """First request returned status code {0}, result count: {1} of {2} total results\n
                        """.format(str(response.status_code), response_dict['resultCount'], response_dict['totalCount'])
    log.append(log_current)
    return log

def write_log_file(log):
    with open("agcros-api-log.txt", 'w') as f:
        for msg in log:
            f.write(msg)

if __name__ == "__main__":
    start_time = timer()
    result_msg = ''
    try:
        endpoint = str(sys.argv[1])
        # offset = int(sys.argv[2])
        # limit = int(sys.argv[3])
        # totalRecords = int(sys.argv[4])
    except IndexError as e:
        result_msg = """ 
        **************************************************** 
        Please pass the following arguments:
        endpoint: 
            /Measurement/SoilChemistry
        offset: 
            Records returnable in one call (Integer value)
        limit: 
            Total records returned (Integer value)
        totalRecords:
            Total record quantity (Integer value)
        usage:
            python usag-api-call.py <endpoint path>
        *****************************************************
        """
        print result_msg

    result_msg = get_agcros_data(endpoint)
    # result_msg = get_agcros_data(endpoint, offset, limit, totalRecords)
    elapsed = timer() - start_time

    print result_msg 
    print '**** Finished execution in {0} seconds ****\n'.format(elapsed)