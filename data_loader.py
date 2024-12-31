import json
from pymongo import MongoClient

from helper import get_all_files

# MongoDB connection
client = MongoClient("mongodb://mongo:27017/")
db = client.synthetic_data
people_collection = db.people


# Load JSON files into MongoDB
def load_data(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    people_collection.insert_many(data)


# Summary check
def check_summary(expected_count):
    total_count = people_collection.count_documents({})
    if total_count == expected_count:
        print(f"SUCCESS: All data loaded. Total records: {total_count}")
    else:
        print(f"WARNING: Data load mismatch. Expected: {expected_count}, Found: {total_count}")


# Main execution
if __name__ == "__main__":

    data_files = get_all_files("./data/synthetic/", ".json")
    print(data_files)

    total_expected_count = 0  # Initialize expected count

    for file_path in data_files:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            total_expected_count += len(data)  # Add to expected count
        load_data(file_path)

    check_summary(total_expected_count)
