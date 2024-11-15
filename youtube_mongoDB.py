import os
import pandas as pd
from pymongo import MongoClient
import json

# Define the directory path
filepath = 'C:/Users/Gurma/Desktop/School/Fall24/415/GroupWork3/DataExtracted'

# Function to load and parse each file
def load_folder(filepath):
    # Initialize a list to hold video data
    videos_data = []

    for root, _, files in os.walk(filepath):
        for file_name in files:
            if file_name.endswith('.txt') and file_name != 'log.txt':
                file_path = os.path.join(root, file_name)
                
                # Open the file and read it line by line
                with open(file_path, 'r') as f:
                    lines = f.readlines()

                print('***')
                print(file_path)
                print(f"Number of lines in {file_name}: {len(lines)}\n")

                # Parse each line of the dataset
                for line in lines:
                    if not line.strip():
                        continue
                    parts = line.strip().split('\t')
                    if len(parts) < 9:
                        continue

                    # Assign parts to variables
                    video_id = parts[0]
                    uploader = parts[1]
                    age = int(parts[2])
                    category = parts[3]
                    length = int(parts[4])
                    views = int(parts[5])
                    rate = float(parts[6])
                    ratings = int(parts[7])
                    comments = int(parts[8])
                    related_ids = parts[9:]  # All remaining parts are related video IDs

                    # Create a dictionary for each video
                    video_data = {
                        "video_id": video_id,
                        "uploader": uploader,
                        "age": age,
                        "category": category,
                        "length": length,
                        "views": views,
                        "rate": rate,
                        "ratings": ratings,
                        "comments": comments,
                        "related_ids": related_ids
                    }

                    # Append the video data to the list
                    videos_data.append(video_data)

    print('\nTotal videos_data loaded:', len(videos_data), '\n')
    return videos_data

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['youtube']
collection = db['videos']

# Load data from all folders and files
videos_data = load_folder(filepath)

# Dictionary to store unique videos by 'video_id' as the key
merged_videos = {}
for video in videos_data:
    video_id = video['video_id']
    # Update the data with the latest crawl (overwrite if video_id already exists)
    merged_videos[video_id] = video

# Convert the merged dictionary to a DataFrame
df = pd.DataFrame(merged_videos.values())

# Convert data types
df['video_id'] = df['video_id'].astype(str)
df['uploader'] = df['uploader'].astype(str)
df['age'] = pd.to_numeric(df['age'], errors='coerce')
df['category'] = df['category'].astype(str)
df['length'] = pd.to_numeric(df['length'], errors='coerce')
df['views'] = pd.to_numeric(df['views'], errors='coerce')
df['rate'] = pd.to_numeric(df['rate'], errors='coerce')
df['ratings'] = pd.to_numeric(df['ratings'], errors='coerce')
df['comments'] = pd.to_numeric(df['comments'], errors='coerce')

# Handle missing values
df.fillna({
    'age': df['age'].median(),
    'length': df['length'].median(),
    'views': df['views'].median(),
    'rate': df['rate'].mean(),
    'ratings': 0,
    'comments': 0
}, inplace=True)

# Display some cleaned data to verify
print(df.head())

# Convert data back into a list of documents for MongoDB insertion
videos_data_for_mongo = df.to_dict(orient='records')

# Insert the deduplicated data into MongoDB
collection.insert_many(videos_data_for_mongo)

print("Data has been successfully inserted into MongoDB.")
