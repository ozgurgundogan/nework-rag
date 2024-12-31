db = db.getSiblingDB('synthetic_data'); // Create or switch to 'synthetic_data' database

// Create 'people' collection if it doesn't exist
db.createCollection('people');

// Ensure indexes for faster queries
db.people.createIndex({ "education.university": 1 }); // Index for university-based queries
db.people.createIndex({ "experiences.company": 1 });  // Index for company-based queries
db.people.createIndex({ "relationships.classmates": 1 }); // Index for classmate relationships
db.people.createIndex({ "relationships.coworkers": 1 });  // Index for coworker relationships
db.people.createIndex({ "relationships.colleagues": 1 }); // Index for colleague relationships
db.people.createIndex({ "_id": 1 }); // Default unique index for person IDs

// Insert a small set of sample data for testing
db.people.insertMany([
    {
        _id: "1",
        name: "Alice Johnson",
        birth_date: "1985-03-15",
        state: "CA",
        education: {
            university: "Harvard University",
            start_date: "2003-01-01",
            end_date: "2007-01-01"
        },
        experiences: [
            {
                company: "Google",
                start_date: "2008-01-01",
                end_date: "2015-01-01",
                position: "Software Engineer"
            }
        ],
        relationships: {
            classmates: [],
            coworkers: [],
            colleagues: []
        }
    },
    {
        _id: "2",
        name: "Bob Smith",
        birth_date: "1986-07-20",
        state: "NY",
        education: {
            university: "Harvard University",
            start_date: "2003-01-01",
            end_date: "2007-01-01"
        },
        experiences: [
            {
                company: "Amazon",
                start_date: "2010-01-01",
                end_date: "2017-01-01",
                position: "Product Manager"
            }
        ],
        relationships: {
            classmates: [],
            coworkers: [],
            colleagues: []
        }
    }
]);

// Log a message to confirm initialization
print("Database initialized with collections, indexes, and sample data.");
