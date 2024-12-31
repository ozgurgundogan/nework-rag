import json
import multiprocessing
from faker import Faker
from random import randint, choice, sample

# Initialize Faker
fake = Faker('en_US')

# Generate states and universities
from faker.providers.address.en_US import Provider as AddressProvider

NUMBER_OF_UN_AND_COMP = 20
BIRTH_DATE_START = 2004
BIRTH_DATE_END = 2005

STATES = AddressProvider.states_abbr  # Unique US state abbreviations
UNIVERSITIES = [fake.company() + " University" for _ in range(NUMBER_OF_UN_AND_COMP)]
COMPANIES = [fake.company() for _ in range(NUMBER_OF_UN_AND_COMP)]

# Parameters
NUM_PROFILES = 1_000_000
BATCH_SIZE = 500_000


# Helper function for date generation
def random_date(year, given_month=None, given_day=None):
    """Generate a random date within the given year."""
    month = randint(1, 12) if given_month is None else given_month
    day = randint(1, 28 if month == 2 else 30) if given_day is None else given_day
    return f"{year}-{month}-{day}"


def generate_person(person_id):
    """Generate a single person's profile."""
    # Birth year between 1960 and 2005
    birth_year = randint(BIRTH_DATE_START, BIRTH_DATE_END)
    birth_date = random_date(birth_year)

    # Education
    start_university_age = randint(18, 19)
    education_start_year = birth_year + start_university_age
    education_duration = randint(4, 5)
    education_end_year = education_start_year + education_duration
    university = choice(UNIVERSITIES)

    # Experience
    experiences = []
    current_year = 2024
    experience_start_year = randint(education_start_year, education_end_year)
    while experience_start_year <= current_year:
        duration = randint(2, 5)
        experience_end_year = min(current_year, experience_start_year + duration)
        company = choice(COMPANIES)
        position = fake.job()
        experiences.append({
            "start_date": random_date(experience_start_year),
            "end_date": random_date(experience_end_year) if experience_end_year < current_year else None,
            "company": company,
            "position": position,
        })
        experience_start_year = experience_end_year + 1

    # Address
    state = choice(STATES)
    address = fake.address().replace("\n", ", ")  # Ensure single-line addresses
    is_neighbor = randint(1, 100) <= 1  # 1% chance to be a neighbor

    return {
        "id": person_id,
        "name": fake.name(),
        "birth_date": birth_date,
        "state": state,
        "address": address if not is_neighbor else f"Neighbor of {address}",
        "education": {
            "university": university,
            "start_date": random_date(education_start_year, given_month=1, given_day=1),
            "end_date": random_date(education_end_year, given_month=1, given_day=1)
        },
        "experiences": experiences,
    }


def generate_and_save(start_id, end_id):
    """Generate a batch of profiles and save to a JSON file."""
    profiles = []
    for person_id in range(start_id, end_id):
        profiles.append(generate_person(person_id))
    filename = f"./data/synthetic/synthetic_data_{start_id}_{end_id}.json"
    with open(filename, "w", encoding='utf-8') as f:
        json.dump(profiles, f, indent=4)


# Main process
if __name__ == "__main__":
    batch_ranges = [(i, i + BATCH_SIZE) for i in range(0, NUM_PROFILES, BATCH_SIZE)]
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        pool.starmap(generate_and_save, batch_ranges)
