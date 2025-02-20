from pymongo import MongoClient
MONGO_URI = 'mongodb://localhost:27017'
DB_NAME = 'msds-697-section-2'
COLLECTION_NAME = 'testRestaurants'

def aggregate_top_cuisines_by_rating():
    """Aggregates Michelin data to find the top cuisines by average rating."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {
            "$group": {
                "_id": "$Cuisine",
                "avg_google_rating": {"$avg": "$google_rating"},
                "michelin_star_count": {"$sum": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": ["$Award", "3 Stars"]}, "then": 3},
                            {"case": {"$eq": ["$Award", "2 Stars"]}, "then": 2},
                            {"case": {"$eq": ["$Award", "1 Star"]}, "then": 1},
                            {"case": {"$eq": ["$Award", "Bib Gourmand"]}, "then": 0.5},
                            {"case": {"$eq": ["$Award", "Selected"]}, "then": 0},
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
                "into": 'top_cuisines_by_rating',
                "whenMatched": "merge",
                "whenNotMatched": "insert"
            }
        }
    ]

    collection.aggregate(pipeline)
    print(f"Aggregated data stored in 'top_cuisines_by_rating'")
