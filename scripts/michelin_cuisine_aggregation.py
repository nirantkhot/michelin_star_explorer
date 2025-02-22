from pymongo import MongoClient

# MongoDB connection details
#function to connect to MongoDB
def connect_to_mongo(MONGO_URI, DB_NAME, COLLECTION_NAME):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    return client, db, collection

def aggregate_top_cuisines_by_rating(MONGO_URI, DB_NAME, COLLECTION_NAME):
    """Aggregates Michelin data to find the top cuisines by average rating."""
    client, db, collection = connect_to_mongo(MONGO_URI, DB_NAME, COLLECTION_NAME)
    pipeline = [
        {
            "$group": {
                "_id": "$michelin_info.Cuisine",
                "avg_google_rating": {"$avg": "$google_info.google_rating"},
                "michelin_star_count": {"$sum": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": ["$michelin_info.Award", "3 Stars"]}, "then": 3},
                            {"case": {"$eq": ["$michelin_info.Award", "2 Stars"]}, "then": 2},
                            {"case": {"$eq": ["$michelin_info.Award", "1 Star"]}, "then": 1},
                            {"case": {"$eq": ["$michelin_info.Award", "Bib Gourmand"]}, "then": 0.5},
                            {"case": {"$eq": ["$michelin_info.Award", "Selected"]}, "then": 0},
                        ],
                        "default": 0,
                    }
                }},
                "total_restaurants": {"$sum": 1}
            }
        },
        {"$sort": {"avg_google_rating": -1}},
        {
            "$merge": {
                "into": "top_cuisines_by_rating",
                "whenMatched": "merge",
                "whenNotMatched": "insert"
            }
        }
    ]

    collection.aggregate(pipeline)
    print(f"Aggregated data stored in 'top_cuisines_by_rating'")

# Call the function
aggregate_top_cuisines_by_rating()
