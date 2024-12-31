import os
import csv
from faker import Faker
from random import randint, choice
from multiprocessing import Pool
from datetime import datetime

# Constants
NUM_PROFILES = 20_000_000
BATCH_SIZE = 100_000
OUTPUT_FILE = "synthetic_profiles.csv"

fake = Faker()

# Generate a list of universities and companies
UNIVERSITIES = [fake.company() + " University" for _ in range(50_000)]
COMPANIES = [fake.company() for _ in range(50_000)]

# Predefined list of job fields
JOB_FIELDS = [
    "Engineering", "Healthcare", "Finance", "Education", "Marketing",
    "IT", "Human Resources", "Legal", "Science", "Arts",
]

def generate_person(person_id):
    # Generate realistic DOB
    birth_year = randint(1960, 2004)  # Ensure people are between 20 and 64 years old in 2024
    birth_month = randint(1, 12)
    birth_day = randint(1, 28)
    dob = f"{birth_year}-{birth_month:02d}-{birth_day:02d}"
    
    # University education
    start_age = choice([18, 19])
    university_start_year = birth_year + start_age
    university_duration = choice([4, 5])
    university_end_year = university_start_year + university_duration
    if university_end_year > 2024:
        university_end_year = 2024  # Cap at the current year

    education = {
        "school": choice(UNIVERSITIES),
        "field_of_study": choice(JOB_FIELDS),  # Use predefined job fields
        "degree_name": choice(["Bachelor", "B.Sc", "B.A", "B.Eng"]),
        "start_year": university_start_year,
        "end_year": university_end_year,
    }

    # Generate work experience
    experiences = []
    current_year = 2024
    experience_start_year = randint(university_end_year, current_year)
    
    while experience_start_year < current_year:
        duration = randint(2, 5)
        experience_end_year = experience_start_year + duration
        if experience_end_year > current_year:
            experience_end_year = current_year
        
        experience = {
            "company": choice(COMPANIES),
            "title": fake.job(),
            "start_date": f"{experience_start_year}-{randint(1, 12):02d}-01",
            "end_date": f"{experience_end_year}-{randint(1, 12):02d}-01",
        }
        experiences.append(experience)
        experience_start_year = experience_end_year + 1

    return {
        "id": person_id,
        "name": fake.name(),
        "dob": dob,
        "university": education,
        "experiences": experiences,
    }

def write_batch(batch):
    with open(OUTPUT_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        for person in batch:
            for exp in person["experiences"]:
                writer.writerow([
                    person["id"],
                    person["name"],
                    person["dob"],
                    person["university"]["school"],
                    person["university"]["field_of_study"],
                    person["university"]["degree_name"],
                    person["university"]["start_year"],
                    person["university"]["end_year"],
                    exp["company"],
                    exp["title"],
                    exp["start_date"],
                    exp["end_date"],
                ])

def generate_and_save(start_id, end_id):
    batch = []
    for person_id in range(start_id, end_id):
        batch.append(generate_person(person_id))
        if len(batch) >= BATCH_SIZE:
            write_batch(batch)
            batch = []
    if batch:
        write_batch(batch)

if __name__ == "__main__":
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    # Write header row
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "id", "name", "dob", "university_school", "university_field_of_study",
            "university_degree_name", "university_start_year", "university_end_year",
            "experience_company", "experience_title", "experience_start_date", "experience_end_date"
        ])

    # Parallel processing
    num_workers = os.cpu_count()
    total_batches = NUM_PROFILES // BATCH_SIZE
    batch_ranges = [(i * BATCH_SIZE, (i + 1) * BATCH_SIZE) for i in range(total_batches)]

    with Pool(num_workers) as pool:
        pool.starmap(generate_and_save, batch_ranges)
