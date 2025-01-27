import json
import os
from time import sleep, time
from typing import Any, Dict, List, Optional

import pandas as pd
from pysdmx.io.csv.sdmx20.writer import write
from pysdmx.io.pd import PandasDataset
from pysdmx.model import Component, Role
from pysdmx.model.dataflow import DataStructureDefinition
from requests import get, post
from vtlengine import run

# Original columns and their simple name for next steps of this tutorial

# ------------------------------
# ------ Data Processing -------
# ------------------------------

RENAME_DICT = {
    "LEI": "LEI",
    "Entity.LegalName": "LEGAL_NAME",
    "Entity.LegalAddress.Country": "COUNTRY_INCORPORATION",
    "Entity.HeadquartersAddress.Country": "COUNTRY_HEADQUARTERS",
    "Entity.EntityCategory": "CATEGORY",
    "Entity.EntitySubCategory": "SUBCATEGORY",
    "Entity.LegalForm.EntityLegalFormCode": "LEGAL_FORM",
    "Entity.EntityStatus": "STATUS",
    "Entity.LegalAddress.PostalCode": "POSTAL_CODE",
}


def _process_chunk(data: pd.DataFrame):
    data.rename(columns=RENAME_DICT, inplace=True)
    data = data[list(RENAME_DICT.values())]
    data = data[data["STATUS"] == "ACTIVE"]
    del data["STATUS"]
    return data


def _save_as_sdmx_csv(data: pd.DataFrame):
    dataset = PandasDataset(
        structure="DataStructure=MD:LEI_DATA(1.0)", data=data
    )
    return write([dataset])


def __clean_output(output, header=False):
    """Currently may add some extra lines in windows,
    just removing the  CR character.
    We also clean the extra headers for chunking."""
    out_lst = output.splitlines()
    if not header:
        out_lst = out_lst[1:]
    output = "\n".join(out_lst)
    del out_lst
    return output


def streaming_load_save_csv_file(golden_copy_original_path, output_filename,
                                 use_sdmx_csv=False, nrows=None):
    """Load data and rename using small memory"""
    chunksize = None
    if nrows is None or nrows > 100000:
        chunksize = 100000
    data = pd.read_csv(golden_copy_original_path, dtype=str,
                       chunksize=chunksize, nrows=nrows)
    # Add header only to the first chunk
    add_header = True
    # Removing the file if already present
    if os.path.exists(output_filename):
        os.remove(output_filename)
    number_of_lines_written = 0
    if isinstance(data, pd.DataFrame):
        data = [data]
    for chunk in data:
        chunk = _process_chunk(chunk)
        if add_header:
            header = True
            add_header = False
        else:
            header = False
        number_of_lines_written += len(chunk)
        if not use_sdmx_csv:
            chunk.to_csv(output_filename, mode="a", index=False, header=header)
        else:
            out = _save_as_sdmx_csv(chunk)
            out = __clean_output(out, header)
            with open(output_filename, "w", encoding="utf-8") as f:
                f.write(out)
    print(f"Number of lines written: {number_of_lines_written}")
    if use_sdmx_csv:
        print("Written in SDMX-CSV 2.0")
    else:
        print("Written in CSV")


# ------------------------------
# ----------- VTL --------------
# ------------------------------

VTL_DTYPES_MAPPING = {
    "String": "String",
    "Alpha": "String",
    "AlphaNumeric": "String",
    "Numeric": "String",
    "BigInteger": "Integer",
    "Integer": "Integer",
    "Long": "Integer",
    "Short": "Integer",
    "Decimal": "Number",
    "Float": "Number",
    "Double": "Number",
    "Boolean": "Boolean",
    "URI": "String",
    "Count": "Integer",
    "InclusiveValueRange": "Number",
    "ExclusiveValueRange": "Number",
    "Incremental": "Number",
    "ObservationalTimePeriod": "Time_Period",
    "StandardTimePeriod": "Time_Period",
    "BasicTimePeriod": "Date",
    "GregorianTimePeriod": "Date",
    "GregorianYear": "Date",
    "GregorianYearMonth": "Date",
    "GregorianMonth": "Date",
    "GregorianDay": "Date",
    "ReportingTimePeriod": "Time_Period",
    "ReportingYear": "Time_Period",
    "ReportingSemester": "Time_Period",
    "ReportingTrimester": "Time_Period",
    "ReportingQuarter": "Time_Period",
    "ReportingMonth": "Time_Period",
    "ReportingWeek": "Time_Period",
    "ReportingDay": "Time_Period",
    "DateTime": "Date",
    "TimeRange": "Time",
    "Month": "String",
    "MonthDay": "String",
    "Day": "String",
    "Time": "String",
    "Duration": "Duration",
}
VTL_ROLE_MAPPING = {
    Role.DIMENSION: "Identifier",
    Role.MEASURE: "Measure",
    Role.ATTRIBUTE: "Attribute",
}


def to_vtl_json(
        dsd: DataStructureDefinition, path: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Formats the DataStructureDefinition as a VTL DataStructure."""
    dataset_name = dsd.id
    components = []
    NAME = "name"
    ROLE = "role"
    TYPE = "type"
    NULLABLE = "nullable"

    _components: List[Component] = []
    _components.extend(dsd.components.dimensions)
    _components.extend(dsd.components.measures)
    _components.extend(dsd.components.attributes)

    for c in _components:
        _type = VTL_DTYPES_MAPPING[c.dtype]
        _nullability = c.role != Role.DIMENSION
        _role = VTL_ROLE_MAPPING[c.role]

        component = {
            NAME: c.id,
            ROLE: _role,
            TYPE: _type,
            NULLABLE: _nullability,
        }

        components.append(component)

    result = {
        "datasets": [{"name": dataset_name, "DataStructure": components}]
    }
    if path is not None:
        with open(path, "w") as fp:
            json.dump(result, fp, indent=2)
        return None

    return result


def _load_script(filename):
    with open(filename, "r") as f:
        script = f.read()
    return script


def run_vtl(script: str, dataset: PandasDataset):
    # Load the dataset
    dataset = PandasDataset(data=dataset.data, structure=dataset.structure)

    # Generate datastructure from VTL
    data_structure = to_vtl_json(dataset.structure)

    # Run the VTL script
    result = run(script, data_structures=data_structure, datapoints={"LEI_DATA": dataset.data})
    return result


# ------------------------------
# ------------ FMR -------------
# ------------------------------

STATUS_IN_PROCESS = ['Initialising', 'Analysing', 'Validating',
                     'Consolidating']

# The list of load status which means is an error
STATUS_ERRORS = ['IncorrectDSD', 'InvalidRef', 'MissingDSD',
                 'Error']

STATUS_COMPLETED = ["Complete"]


def __handle_status(response_status):
    if response_status.json()['Status'] == 'Complete':
        if response_status.json()['Datasets'][0]['Errors']:
            return response_status.json()['Datasets'][0]['ValidationReport']
        else:
            return []
    if response_status.json()['Status'] in STATUS_ERRORS:
        raise Exception(response_status.json())


def __validation_status_request(status_url: str,
                                uid: str,
                                max_retries: int = 10,
                                interval_time: float = 0.5):
    """
    Polls the FMR instance to get the validation status of an uploaded file

    :param status_url: The URL for checking the validation status
    :type status_url: str

    :param uid: the unique identifier we have to send to the request
    :type uid: str

    :param max_retries: The maximum number of retries for
                        checking validation status
    :type max_retries: int

    :param interval_time: The interval time between retries in seconds
    :type interval_time: float

    :return: The validation status if successful

    :exception: raise an exception if the validation status
                is not found in the response
    :exception: raise an exception if the current time exceeds the timeout
    """

    # Record the starting time for the entire validation process
    start_global = time()

    # Initialize variables for tracking time intervals
    start = time()
    interval_counter = 0

    # Get the current time
    current = time()

    # Calculate the total timeout based on max retries and interval time
    timeout = max_retries * interval_time

    while current - start_global < timeout:
        current = time()

        # Calculate the time interval from the start of the validation process
        interval_start = current - start_global
        interval = current - start

        # Skip the current iteration if the time interval is less than the
        # specified interval time
        if interval_start <= interval_time:
            continue

        # Perform a get request to the server to check the load status
        response_status = get(url=status_url,
                              params={'uid': uid})

        # Check if the 'Status' key is present in the response JSON
        if 'Status' not in response_status.json():
            raise Exception("Error: Status not found in response")

        # Check if the status is still in process
        if response_status.json()['Status'] in STATUS_IN_PROCESS:
            if interval > interval_time:
                interval_counter += 1
                start = time()

            # Check if the maximum number of retries is reached
            if interval_counter == max_retries:
                raise Exception(
                    f"Error: Max retries exceeded ({interval_counter})")

        # Return the handled status if the validation is still in process
        return __handle_status(response_status)

    # Raise an exception if the total timeout is exceeded
    raise Exception(f"Timeout {timeout} exceeded on status request.")


def get_validation_status(status_url: str,
                          uid: str,
                          max_retries: int = 10,
                          interval_time: float = 0.5
                          ):
    """
    Gets the validation status of file uploaded using the FMR instance.

    :param status_url: The URL for checking the validation status
    :type status_url: str

    :param uid: The unique identifier of the uploaded file
    :type uid: str

    :param max_retries: The maximum number of retries for checking validation
                        status (default is 10)
    :type max_retries: int

    :param interval_time: The interval time between retries
                          in seconds (default is 0.5)
    :type interval_time: float

    :return: The validation status if successful
    :exception: raise an exception if the validation status
                is not found in the response
    :exception: raise an exception if the current time exceeds the timeout

    """

    # Pause execution for the specified interval time
    sleep(interval_time)

    # Perform a GET request to the server to check the load status
    response_status = get(url=status_url,
                          params={'uid': uid})

    # Check if the status is still in process
    if response_status.json()['Status'] in STATUS_IN_PROCESS:
        # If in process, recursively call the function with retries
        return __validation_status_request(status_url=status_url,
                                           uid=uid,
                                           max_retries=max_retries,
                                           interval_time=interval_time)
    # Return the handled status if the validation is complete
    return __handle_status(response_status)


def validate_data_fmr(csv_text: str,
                      host: str = 'localhost',
                      port: int = 8080,
                      use_https: bool = False,
                      delimiter: str = 'comma',
                      max_retries: int = 10,
                      interval_time: float = 0.5
                      ):
    """
    Validates an SDMX CSV file by uploading it to an FMR instance
    and checking its validation status

    :param csv_text: The SDMX CSV text to be validated
    :type csv_text: str

    :param host: The FMR instance host (default is 'localhost')
    :type host: str

    :param port: The FMR instance port (default is 8080 for HTTP and
                 443 for HTTPS)
    :type port: int

    :param use_https: A boolean indicating whether to use HTTPS
                     (default is False)
    :type use_https: bool

    :param delimiter: The delimiter used in the CSV file
                      (options: 'comma', 'semicolon', 'tab', 'space')
    :type delimiter: str

    :param max_retries: The maximum number of retries for checking
                        validation status (default is 10)
    :type max_retries: int

    :param interval_time: The interval time between retries
                          in seconds (default is 0.5)
    :type interval_time: float

    :exception: Exception with error details if validation fails
    """

    if use_https and port == 8080:
        port = 443

    # Constructing the base URL based on the provided parameters
    base_url = f'http{"s" if use_https else ""}://{host}:{port}'

    # Constructing the upload URL for the FMR instance
    upload_url = base_url + '/ws/public/data/load'

    # Checking if the provided delimiter is valid
    if delimiter not in ('comma', 'semicolon', 'tab', 'space'):
        raise ValueError('Delimiter must be comma, semicolon, tab or space')

    # Defining headers for the request
    headers = {'Data-Format': f'csv;delimiter={delimiter}'}

    # Perform a POST request to the server with the CSV data as an attachment
    response = post(upload_url,
                    files={'uploadFile': csv_text.replace("dataprovision", "datastructure")},
                    headers=headers)

    # Check the response from the server
    if not response.status_code == 200:
        raise Exception(response.text, response.status_code)

    # Constructing the URL for checking the validation status
    status_url = base_url + '/ws/public/data/loadStatus'

    # Getting the uid from the request response
    uid = response.json()['uid']

    # Return the validation status by calling a separate function
    return get_validation_status(status_url=status_url,
                                 uid=uid,
                                 max_retries=max_retries,
                                 interval_time=interval_time)
