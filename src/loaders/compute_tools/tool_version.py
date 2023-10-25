import sys

import yaml


def extract_latest_version(file_path: str) -> str:
    """
    Extracts the latest version from a YAML file by referencing the latest date specified within the file.

    Args:
        file_path (str): The path to the YAML file.

    Returns:
        str: The latest version extracted from the YAML file.
    """

    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
        versions = data['versions']
        # Find the latest version based on the 'date' key
        latest_version = max(versions, key=lambda x: x['date'])
        return latest_version['version']


def extract_latest_reference_db_version(file_path: str) -> str:
    """
    Extracts the latest reference database version from a YAML file by referencing the latest date specified within the file.

    Args:
        file_path (str): The path to the YAML file.

    Returns:
        str: The latest reference database version extracted from the YAML file.
        None: If the latest version does not have a reference database version.
    """

    latest_ver = extract_latest_version(file_path)

    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
        versions = data['versions']

        found_version = next((v for v in versions if v['version'] == latest_ver), None)

        if not found_version:
            raise ValueError(f'Unable to find version: {latest_ver}')

        return found_version.get('reference_db_version')


def main():
    if len(sys.argv) < 2:
        print("Please provide the file path as an argument.")
        sys.exit(1)

    file_path = sys.argv[1]
    latest_version = extract_latest_version(file_path)
    print(latest_version)


if __name__ == '__main__':
    main()
