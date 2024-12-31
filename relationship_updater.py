import os

from pymongo import MongoClient
from multiprocessing import Pool
from timeit import default_timer as timer
from datetime import timedelta

client = MongoClient("mongodb://mongo:27017/")
db = client.synthetic_data
people_collection = db.people


def find_classmates(person):
    return list(
        people_collection.find({
            "education.university": person["education"]["university"],
            "education.start_date": {"$lte": person["education"]["end_date"]},
            "education.end_date": {"$gte": person["education"]["start_date"]},
            "_id": {"$ne": person["_id"]}
        }, {"_id": 1})
    )


def find_coworkers(person):
    coworkers = set()
    for exp in person["experiences"]:
        coworkers.update(
            p["_id"] for p in people_collection.find({
                "experiences": {
                    "$elemMatch": {
                        "company": exp["company"],
                        "start_date": {"$lte": exp["end_date"]},
                        "end_date": {"$gte": exp["start_date"]},
                    }
                },
                "_id": {"$ne": person["_id"]}
            }, {"_id": 1})
        )
    return list(coworkers)


def find_colleagues(person):
    colleagues = set()
    for exp in person["experiences"]:
        colleagues.update(
            p["_id"] for p in people_collection.find({
                "experiences.company": exp["company"],
                "_id": {"$ne": person["_id"]}
            }, {"_id": 1})
        )
    colleagues -= set(find_coworkers(person))
    return list(colleagues)


def process_person(person_id):
    person = people_collection.find_one({"_id": person_id})
    classmates = find_classmates(person)
    coworkers = find_coworkers(person)
    colleagues = find_colleagues(person)
    print(person_id, len(classmates), len(coworkers), len(colleagues))
    people_collection.update_one(
        {"_id": person_id},
        {"$set": {"relationships": {
            "classmates": classmates,
            "coworkers": coworkers,
            "colleagues": colleagues
        }}}
    )


# Multithread ile işlem
def update_relationships():
    default_pool_size = os.cpu_count()
    person_ids = people_collection.aggregate([{"$project": {"_id": 1}}])
    with Pool(processes=default_pool_size) as pool:
        pool.map(process_person, (doc["_id"] for doc in person_ids))


if __name__ == "__main__":
    start = timer()
    update_relationships()
    print("Relationships have been updated.")
    end = timer()
    print("Elapsed time : " + timedelta(seconds=end - start))
