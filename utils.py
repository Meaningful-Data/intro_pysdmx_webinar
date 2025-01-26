import json
import os
from typing import Any, Dict, List, Optional

import pandas as pd
from pysdmx.io.csv.sdmx20.writer import write
from pysdmx.io.pd import PandasDataset
from pysdmx.model import Component, Role
from pysdmx.model.dataflow import DataStructureDefinition

# Original columns and their simple name for next steps of this tutorial

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
