import json
from collections import defaultdict

def analyze_json_file(file_path):
    key_structure_counts = defaultdict(int)

    with open(file_path, 'r') as file:
        for line in file:
            json_data = json.loads(line)
            key_structure = get_json_key_structure(json_data)
            key_structure_str = json.dumps(key_structure, sort_keys=True)
            key_structure_counts[key_structure_str] += 1

    print_key_structure_summary(key_structure_counts)

def get_json_key_structure(json_data, parent_key=''):
    if isinstance(json_data, dict):
        key_structure = {}
        for key in json_data:
            nested_key = f'{parent_key}.{key}' if parent_key else key
            nested_structure = get_json_key_structure(json_data[key], nested_key)
            key_structure.update(nested_structure)
        return key_structure
    elif isinstance(json_data, list):
        if len(json_data) > 0:
            return get_json_key_structure(json_data[0], parent_key)
        else:
            return {}
    else:
        return {parent_key: 1}

def print_key_structure_summary(key_structure_counts):
    print("Summary of JSON key structure:")
    type_counts = defaultdict(int)
    for key_structure, count in key_structure_counts.items():
        json_structure = json.loads(key_structure)
        json_type = get_json_type(json_structure)
        type_counts[json_type] += count
    for json_type, count in type_counts.items():
        print(f"Type {json_type}:")
        for key_structure, count in key_structure_counts.items():
            json_structure = json.loads(key_structure)
            if get_json_type(json_structure) == json_type:
                print(f"{json_structure}: {count}")
        print()

def get_json_type(key_structure):
    num_keys = sum(1 for key in key_structure if isinstance(key, str))
    return num_keys + 1

# Example usage
file_path = 'onlyGDPRDeleted.csv'
analyze_json_file(file_path)
