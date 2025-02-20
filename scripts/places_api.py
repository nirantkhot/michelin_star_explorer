import pandas as pd
import requests
import time
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()
data_folder = os.getenv('DATA_FOLDER')
GOOGLE_PLACES_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
if not GOOGLE_PLACES_API_KEY:
    raise ValueError("Please set GOOGLE_PLACES_API_KEY in .env file")

def get_place_rating(name: str, address: str) -> Optional[float]:
    """Fetch Google Places rating for a restaurant."""
    base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"

    # Combine name and address for more accurate results
    query = f"{name} {address}"

    params = {
        'input': query,
        'inputtype': 'textquery',
        'fields': 'place_id,rating',
        'key': GOOGLE_PLACES_API_KEY
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        if data['candidates']:
            place_id = data['candidates'][0].get('place_id')
            if place_id:
                # Get detailed place information
                details_url = "https://maps.googleapis.com/maps/api/place/details/json"
                details_params = {
                    'place_id': place_id,
                    'fields': 'rating,user_ratings_total',
                    'key': GOOGLE_PLACES_API_KEY
                }
                details_response = requests.get(details_url, params=details_params)
                details_response.raise_for_status()
                details_data = details_response.json()
                return details_data.get('result', {}).get('rating'), details_data.get('result', {}).get('user_ratings_total')
        return None
    except Exception as e:
        print(f"Error fetching rating for {name}: {str(e)}")
        return None

# Read the CSV
df = pd.read_csv(f'{data_folder}michelin_my_maps.csv')

# Add Google ratings column
df['google_rating'] = None

# Fetch ratings for each restaurant
for idx, row in df.iterrows():
    rating, reviews = get_place_rating(row['Name'], row['Address'])
    df.at[idx, 'google_rating'] = rating
    df.at[idx, 'google_reviews'] = reviews
    time.sleep(0.5)  # Rate limiting to avoid API quota issues
    if idx % 10 == 0:
        print(f"Processed {idx} restaurants...")


# Save results
df.to_csv(f'{data_folder}michelin_with_google_ratings.csv', index=False)
print("Done! Results saved to michelin_with_google_ratings.csv")
