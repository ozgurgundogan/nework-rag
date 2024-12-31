import random
from faker import Faker
import pandas as pd
import json
from datetime import datetime

# Initialize the Faker object
fake = Faker()

# Function to generate synthetic experience
def generate_experience(start_year, end_year=None):
    if end_year is None:
        end_year = start_year
    start_date = datetime(start_year, random.randint(1, 12), random.randint(1, 28))
    if end_year:
        end_date = datetime(end_year, random.randint(1, 12), random.randint(1, 28))
    else:
        end_date = None
    return {
        "starts_at": {"day": start_date.day, "month": start_date.month, "year": start_date.year},
        "ends_at": {"day": end_date.day if end_date else None, "month": end_date.month if end_date else None, "year": end_date.year if end_date else None},
        "company": fake.company(),
        "title": fake.job(),
        "location": fake.city(),
        "logo_url": "none"
    }

# Function to generate a classmate relationship
def generate_classmate_relationship(person, all_people):
    classmates = []
    for other_person in all_people:
        if person["education"][0]["school"] == other_person["education"][0]["school"] and \
           person["education"][0]["starts_at"]["year"] == other_person["education"][0]["starts_at"]["year"]:
            classmates.append(other_person)
    return classmates

# Function to generate a colleague relationship
def generate_colleague_relationship(person, all_people):
    colleagues = []
    for other_person in all_people:
        for exp1 in person["experiences"]:
            for exp2 in other_person["experiences"]:
                if exp1["company"] == exp2["company"] and \
                   exp1["starts_at"]["year"] <= exp2["ends_at"]["year"] and \
                   exp2["starts_at"]["year"] <= exp1["ends_at"]["year"]:
                    colleagues.append(other_person)
    return colleagues

# Function to generate profile data for a person
def generate_person(id, all_people):
    first_name = fake.first_name()
    last_name = fake.last_name()
    full_name = f"{first_name} {last_name}"
    occupation = fake.job()
    city = fake.city()
    state = fake.state()
    
    # Create experiences for this person with random years between 1960 and 2024
    experiences = []
    for year in range(random.randint(1960, 2000), 2024, random.randint(2, 5)):  # Random time intervals within the 1960-2024 range
        experiences.append(generate_experience(year, year + random.randint(1, 3)))
    
    # Education with random years between 1960 and 2010
    education_start_year = random.randint(1960, 2010)
    education = [{"degree_name": "BS", "field_of_study": fake.job(), "school": fake.company(), 
                  "starts_at": {"year": education_start_year}, "ends_at": None}]
    
    # Certifications
    certifications = [{"name": "AIRC", "authority": "LOMA"}, {"name": "ALMI, ACS", "authority": "LOMA"}]
    
    # Classmates and Colleagues (relationships)
    classmates = generate_classmate_relationship(person={"education": education}, all_people=all_people)
    colleagues = generate_colleague_relationship(person={"experiences": experiences}, all_people=all_people)
    
    # Return synthetic profile data
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
        "people_also_viewed": [{"link": fake.uri(), "name": fake.name(), "location": fake.city()} for _ in range(5)],
        "classmates": [person["full_name"] for person in classmates],
        "colleagues": [person["full_name"] for person in colleagues]
    }
    return person_data

# Function to generate the synthetic dataset
def generate_dataset(num_people=20000):
    data = []
    
    # Generate a list of people first to handle relationships
    for person_id in range(1, num_people + 1):
        person = generate_person(person_id, data)  # Passing already generated data to handle relationships
        data.append(person)
    
    return data

# Generate 20,000 synthetic profiles
dataset = generate_dataset()

# Save the dataset to a JSON file
with open('synthetic_profiles_with_relationships.json', 'w') as f:
    json.dump(dataset, f, indent=4)

# Alternatively, convert to a Pandas DataFrame
df = pd.DataFrame(dataset)
print(df.head())

# Save the dataset as CSV
df.to_csv('synthetic_profiles_with_relationships.csv', index=False)

