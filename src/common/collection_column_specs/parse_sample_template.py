"""
usage: parse_sample_template.py [-h] --input_yaml INPUT_YAML --core_yaml CORE_YAML
                                --output_yaml OUTPUT_YAML

Parse the YAML structure of a sample template file and the core YAML file to generate
a YAML file containing specifications for sample collection columns

options:
  -h, --help            show this help message and exit
  --input_yaml INPUT_YAML
                        sample template YAML file
  --core_yaml CORE_YAML
                        core sample YAML file
  --output_yaml OUTPUT_YAML
                        output YAML file

The sample template file can be downloaded from
https://github.com/kbase/sample_service_validator_config/tree/master/templates
The core yaml file can be downloaded from
https://github.com/kbase/sample_service_validator_config/blob/master/vocabularies/core.yml

PLEASE NOTE:
users should manually examine the output yaml file and make sure the output is correct
then manually copy the output to src/common/collection_column_specs/samples-[collection].yml
In other words, the output of this script is to save the user's time when constructing the sample column
specifications and is not expected to be completely accurate.

TODO:
metadata_validation.yml file might be another source of information for constructing the column specifications.
We should consider using this file instead of combine the sample template and core yaml files if we are parsing more samples.
https://github.com/kbase/sample_service_validator_config/blob/master/metadata_validation.yml

"""
import argparse
from datetime import datetime

import yaml

# currently available samples fields in the collection service
_CURRENT_SAMPLES = ['enigma:collection_time', 'enigma:experiment_name', 'enigma:well_name', 'env_package', 'material',
                    'enigma:date', 'enigma:time_zone', 'latitude', 'longitude', 'sample_template',
                    'sesar:igsn', 'sesar:material', 'sesar:field_name', 'other_names', 'sesar:collection_method',
                    'sesar:collection_method_description', 'purpose', 'sesar:physiographic_feature_primary',
                    'sesar:physiographic_feature_name', 'country', 'sesar:field_program_cruise',
                    'sesar:collector_chief_scientist', 'sesar:collection_date', 'sesar:archive_contact_current']

_NGRAM_KEY = ['other_names',
              'purpose',
              'country',
              'sesar:collection_method_description',
              'sesar:archive_contact_current',
              'sesar:collector_chief_scientist',
              'sesar:collection_method',
              'sesar:field_program_cruise',
              'sesar:material',
              'sesar:physiographic_feature_name',
              'sesar:physiographic_feature_primary',
              'enigma:experiment_name',
              'enigma:well_name']

# description from sample service needs to be corrected
_CUSTOM_DESCRIPTION = {'sesar:igsn': 'International Geo Sample Number.',
                       'longitude': 'Longitude of the location where the sample was collected in WGS 84 coordinate '
                                    'system.',
                       'latitude': 'Latitude of the location where the sample was collected in WGS 84 coordinate '
                                   'system.',
                       }
# shared sample attributes with distinct display names across sample services (sesar and enigma).
_CUSTOM_DISPLAY_NAME = {'longitude': 'Longitude',
                        'latitude': 'Latitude',
                        }


def _is_date_string(example_value, key):
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
        # handle a situation like '2; 10' - IOW use the 1st of multiple examples
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
    output_data = [entry for entry in output_data if entry['key'] in _CURRENT_SAMPLES]
    with open(output_yaml, 'w') as file:
        yaml.dump(output_data, file, default_flow_style=False, sort_keys=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse the YAML structure of a sample template file and the core '
                                                 'YAML file to generate a YAML file containing specifications for '
                                                 'sample collection columns')
    parser.add_argument('--input_yaml', help='sample template YAML file', required=True)
    parser.add_argument('--core_yaml', help='core sample YAML file', required=True)
    parser.add_argument('--output_yaml', help='output YAML file', required=True)
    args = parser.parse_args()

    parse_sample_spec(args.input_yaml, args.core_yaml, args.output_yaml)