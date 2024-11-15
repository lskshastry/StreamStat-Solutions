from pymongo import MongoClient
import json
import pandas as pd


def load_folder(filepath, folder, filenames):
        # Initialize a list to hold video data
    videos_data = []

    for file in filenames:
        # Open the file and read it line by line
        with open(filepath + folder + '/' + file + '.txt', 'r') as f:
            lines = f.readlines()

        print('***')
        print(filepath + file + '.txt')
        print(len(lines))
        print('\n')

        # Parse each line of the dataset
        for line in lines:
            if not line:
                continue
            else: 
                parts = line.strip().split('\t')
                if len(parts)<9:
                    continue
                else:
            
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

    print('\n')
    print('videos_data')
    print(len(videos_data))
    print('\n')

    # # Convert to DataFrame for easier analysis (optional)
    # df = pd.DataFrame(videos_data)

    # # Display the parsed data
    # print(df.head())  # Show first few rows to verify
    # return df
    return videos_data


# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['youtube']
collection = db['videos']

foldernames= ['0301','0313','0329','0420']
filenames = ["0","1","2","3"]
filepath = 'D:/fall_2024_coding/dataset/'

# for file in filenames:
#     # Load data from the file
#     with open(filepath + file + '.txt', 'r') as f:
#         videos_data = json.load(f)

#     # Insert data into MongoDB
#     collection.insert_many(videos_data)

###################################
##################################

# df1 = pd.DataFrame(columns=["video_id","uploader","age", "category","length","views","rate","ratings","comments","related_ids"])
# df1.set_index("video_id")

# Dictionary to store videos, with video_id as the key
merged_videos = {}

for folder in foldernames:
    # df2 = load_folder(filepath, folder, filenames)
    # df2.set_index("video_id")
    # df2.combine_first(df1)
    crawl_data = load_folder(filepath, folder, filenames)
    for video in crawl_data:
        video_id = video['video_id']
        # Update the data with the latest crawl (overwrite if video_id already exists)
        merged_videos[video_id] = video

# Convert the merged dictionary to a DataFrame
df = pd.DataFrame(merged_videos.values())

print(df.head())
print(df.size)



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


# Insert data into MongoDB
#collection.insert_many(videos_data)

# Convert the dictionary back into a list of documents for MongoDB insertion
videos_data_for_mongo = list(merged_videos.values())

# Insert the deduplicated data into MongoDB
collection.insert_many(videos_data_for_mongo)

print("Data has been successfully inserted into MongoDB.")