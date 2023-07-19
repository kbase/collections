import sys

import yaml


def extract_latest_version(file_path: str):
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
        versions = data['versions']
        latest_version = max(versions, key=lambda x: x['date'])
        return latest_version['version']


def main():
    if len(sys.argv) < 2:
        print("Please provide the file path as an argument.")
        sys.exit(1)

    file_path = sys.argv[1]
    latest_version = extract_latest_version(file_path)
    print(latest_version)


if __name__ == '__main__':
    main()