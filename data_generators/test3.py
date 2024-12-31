import random
from datetime import datetime
from faker import Faker
import pandas as pd

fake = Faker()

# Generate a pool of 50,000 unique companies and universities
companies = [fake.company() for _ in range(50000)]
universities = [fake.company() for _ in range(50000)]  # Using company names as universities for simplicity

# Function to generate synthetic experience
def generate_experience(start_year, end_year=None):
    if end_year is None:
        end_year = start_year
    start_date = datetime(start_year, random.randint(1, 12), random.randint(1, 28))
    end_date = None if end_year is None else datetime(end_year, random.randint(1, 12), random.randint(1, 28))
    return {
        "starts_at": {"day": start_date.day, "month": start_date.month, "year": start_date.year},
        "ends_at": {"day": end_date.day if end_date else None, "month": end_date.month if end_date else None, "year": end_date.year if end_date else None},
        "company": random.choice(companies),
        "title": fake.job(),
        "location": fake.city(),
    }

# Function to generate synthetic education
def generate_education():
    education_start_year = random.randint(1990, 2020)
    return [{
        "degree_name": "BS",
        "field_of_study": fake.job(),
        "school": random.choice(universities),
        "starts_at": {"year": education_start_year},
        "ends_at": None
    }]

# Function to generate a profile
def generate_person(id):
    first_name = fake.first_name()
    last_name = fake.last_name()
    full_name = f"{first_name} {last_name}"
    occupation = fake.job()
    city = fake.city()
    state = fake.state()

    # Generate experiences (random years between 1990 and 2024)
    experiences = []
    for year in range(random.randint(1990, 2000), 2024, random.randint(2, 5)):
        experiences.append(generate_experience(year, year + random.randint(1, 3)))
    
    # Generate education
    education = generate_education()
    
    # Generate certifications
    certifications = [{"name": "AIRC", "authority": "LOMA"}, {"name": "ALMI, ACS", "authority": "LOMA"}]

    person_data = {
        "public_identifier": str(id),
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "occupation": occupation,
        "headline": occupation,
        "country": "US",
        "country_full_name": "United States of America",
        "city": city,
        "state": state,
        "experiences": experiences,
        "education": education,
        "certifications": certifications,
        "connections": random.randint(50, 500),
        "people_also_viewed": [{"link": fake.uri(), "name": fake.name(), "location": fake.city()} for _ in range(5)]
    }
    return person_data

# Function to generate and save dataset in chunks
def generate_and_save_dataset(num_people=20000000, chunk_size=10000):
    # Open CSV file for writing
    with open('synthetic_profiles_large.csv', 'w') as f:
        # Define header for CSV
        header = ["public_identifier", "first_name", "last_name", "full_name", "occupation", 
                  "city", "state", "connections", "experiences", "education", "certifications"]
        f.write(",".join(header) + "\n")

        for start in range(0, num_people, chunk_size):
            chunk_data = []
            for person_id in range(start + 1, start + chunk_size + 1):
                person = generate_person(person_id)
                chunk_data.append(person)

            # Convert list of dicts to DataFrame for easy CSV writing
            df = pd.DataFrame(chunk_data)
            df.to_csv(f, header=False, index=False)

            print(f"Generated {start + chunk_size} out of {num_people}...")

# Generate dataset in chunks of 10,000 profiles
generate_and_save_dataset(200, 100)

