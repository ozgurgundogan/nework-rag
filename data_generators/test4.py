import multiprocessing
import csv
import random
from faker import Faker

# Initialize Faker
fake = Faker()
Faker.seed(42)

# Define constants
NUM_PROFILES = 5_000_000  # Total number of profiles to generate
NUM_PROCESSES = multiprocessing.cpu_count()  # Number of CPU cores
CHUNK_SIZE = NUM_PROFILES // NUM_PROCESSES  # Profiles per process
OUTPUT_FILE = "synthetic_data.csv"

# Pre-generate random companies and universities
COMPANIES = [fake.company() for _ in range(50_000)]
UNIVERSITIES = [fake.company() + " University" for _ in range(50_000)]

# Function to generate a single profile
def generate_profile():
    profile = {
        "id": fake.uuid4(),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "full_name": f"{fake.first_name()} {fake.last_name()}",
        "occupation": fake.job(),
        "country": fake.country(),
        "city": fake.city(),
        "state": fake.state(),
        "education": [
            {
                "school": random.choice(UNIVERSITIES),
                "degree_name": fake.random_element(["BSc", "MSc", "PhD", "Associate", "Diploma"]),
                "field_of_study": fake.job(),
                "starts_at": random.randint(1990, 2018),
                "ends_at": random.randint(1991, 2024),
            }
        ],
        "experience": [
            {
                "company": random.choice(COMPANIES),
                "title": fake.job(),
                "location": fake.city(),
                "starts_at": random.randint(1990, 2018),
                "ends_at": random.randint(1991, 2024),
            }
        ],
    }
    return profile

# Function to generate profiles in chunks
def generate_chunk(start_index, end_index, output_file):
    with open(output_file, mode="w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "id",
                "first_name",
                "last_name",
                "full_name",
                "occupation",
                "country",
                "city",
                "state",
                "education",
                "experience",
            ],
        )
        writer.writeheader()
        for _ in range(start_index, end_index):
            profile = generate_profile()
            writer.writerow(profile)

# Parallel function
def parallel_generation():
    processes = []
    for i in range(NUM_PROCESSES):
        start_index = i * CHUNK_SIZE
        end_index = start_index + CHUNK_SIZE
        output_file = f"synthetic_data_part_{i}.csv"

        # Create a process
        process = multiprocessing.Process(target=generate_chunk, args=(start_index, end_index, output_file))
        processes.append(process)
        process.start()

    # Wait for all processes to complete
    for process in processes:
        process.join()

    # Combine files (optional step)
    with open(OUTPUT_FILE, mode="w", newline="") as output_file:
        writer = None
        for i in range(NUM_PROCESSES):
            input_file = f"synthetic_data_part_{i}.csv"
            with open(input_file, mode="r") as f:
                reader = csv.DictReader(f)
                if writer is None:
                    writer = csv.DictWriter(output_file, fieldnames=reader.fieldnames)
                    writer.writeheader()
                for row in reader:
                    writer.writerow(row)

if __name__ == "__main__":
    parallel_generation()

