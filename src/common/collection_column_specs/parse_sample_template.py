"""
`parse_sample_spec` is a function designed to process a sample template YAML file and a core YAML file obtained from
the Sample Service. It transforms the sample template entries into a column specifications format, making them
suitable for use by the Collection Service.

The sample template file can be downloaded from
https://github.com/kbase/sample_service_validator_config/tree/master/templates
The core yaml file can be downloaded from
https://github.com/kbase/sample_service_validator_config/blob/master/vocabularies/core.yml

PLEASE NOTE:
users should manually exam the output yaml file and make sure the output is correct
then manually copy the output to src/common/collection_column_specs/samples-[collection].yml

"""
from datetime import datetime

import yaml

_NGRAM_KEY = ['description', 'other_names', 'comment', 'continent', 'country', 'county',
              'locality', 'locality_description', 'city_township',
              'purpose',
              'sesar:collection_method_description',
              'sesar:archive_contact_current',
              'sesar:collector_chief_scientist']
_DATE_KEY = ['modification_date']  # example from sample service cannot be parsed by datetime.strptime
# description from sample service needs to be corrected
_CUSTOM_DESCRIPTION = {'sesar:igsn': 'International Geo Sample Number',
                       'sesar:parent_igsn': 'Parent IGSN',
                       'enigma:method': 'Method used for measurement',
                       'enigma:well_type': 'Type of Well (SP; MP)',
                       'enigma:boring': 'boring depth in ft below ground surface (ft BGS)',
                       'enigma:packing_depth_end': 'Packing depth end in ft below ground surface (ft BGS)',
                       'enigma:packing_depth_start': 'Packing depth start in ft below ground surface (ft BGS)',
                       'longitude': 'Longitude of the location where the sample was collected in WGS 84 coordinate '
                                    'system.',
                       'latitude': 'Latitude of the location where the sample was collected in WGS 84 coordinate '
                                   'system.',
                       'longitude_end': 'End longitude of the location where the sample was collected in WGS '
                                        '84 coordinate system. Needs to be entered in decimal degrees. Negative values '
                                        "for 'West' longitudes.",
                       'latitude_end': 'End latitude of the location where the sample was collected in WGS '
                                       '84 coordinate system. Needs to be entered in decimal degrees. Negative values '
                                       "for 'South' latitudes."
                       }

# shared sample attributes with distinct display names across sample services (sesar and enigma).
_CUSTOM_DISPLAY_NAME = {'longitude': 'Longitude',
                        'latitude': 'Latitude',
                        'longitude_end': 'Longitude End',
                        'latitude_end': 'Latitude End',
                        }


def _is_date_string(example_value, key):
    if key in _DATE_KEY:
        return True

    # https://github.com/kbase/collections/blob/main/src/loaders/common/loader_helper.py#L52
    formats_to_try = ["%Y/%m/%d", "%Y-%m-%d", "%m/%d/%y", "%Y-%m-%dT%H:%M:%S%z"]
    for date_format in formats_to_try:
        try:
            datetime.strptime(example_value, date_format)
            return True
        except ValueError:
            pass


def _string_type(example_value, key):

    if isinstance(example_value, str):
        # handle a situation like '2; 10'
        # https://github.com/kbase/sample_service_validator_config/blob/master/templates/enigma_template.yml#L309C5-L309C19
        example_value = example_value.split(';')[0].strip()
        if _is_date_string(example_value, key):
            return {"type": "date"}
        try:
            int(example_value)
            return {"type": "int"}
        except ValueError:
            try:
                float(example_value)
                return {"type": "float"}
            except ValueError:
                return {"type": "string",
                        "filter_strategy": "ngram" if key in _NGRAM_KEY else "identity"}
    elif isinstance(example_value, int):
        return {"type": "int"}
    elif isinstance(example_value, float):
        return {"type": "float"}
    else:
        raise ValueError(f'Unknown type for {example_value} for key {key}')


def _parse_input_yaml(input_yaml):
    with open(input_yaml, 'r') as f:
        data = yaml.safe_load(f)

    result = []

    for column_name, column_data in data['Columns'].items():

        if 'transformations' not in column_data:
            key = column_data['aliases'][0]
        else:
            transformation_data = column_data['transformations'][0]
            key = transformation_data['parameters'][0]

        parsed_item = {
            'key': key
        }
        parsed_item.update(_string_type(column_data['example'], key))

        parsed_item.update({
            'display_name': _CUSTOM_DISPLAY_NAME.get(key, column_name),
            'category': column_data['category'].capitalize(),
            'description': _CUSTOM_DESCRIPTION.get(key, column_data['definition'])
        })

        result.append(parsed_item)

    return result


def _parse_core_yaml(core_yaml):
    with open(core_yaml, 'r') as f:
        data = yaml.safe_load(f)

    result = []
    for column_name, column_data in data['terms'].items():
        parsed_item = {
            'key': column_name
        }

        parsed_item.update(_string_type(column_data['examples'][0], column_name))

        parsed_item.update({
            'display_name': column_data['title'],
            'category': 'description'.capitalize(),
            'description': _CUSTOM_DESCRIPTION.get(column_name, column_data['description'])
        })

        result.append(parsed_item)

    return result


def parse_sample_spec(input_yaml, core_yaml, output_yaml):
    template_samples = _parse_input_yaml(input_yaml)
    core_samples = _parse_core_yaml(core_yaml)

    template_keys = {entry['key'] for entry in template_samples}
    filtered_core_samples = [entry for entry in core_samples if entry['key'] not in template_keys]

    output_data = template_samples + filtered_core_samples
    with open(output_yaml, 'w') as file:
        yaml.dump(output_data, file, default_flow_style=False, sort_keys=False)