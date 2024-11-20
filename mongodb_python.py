import pandas as pd
import zipfile
from pymongo import MongoClient
import networkx as nx
import matplotlib.pyplot as plt
import random

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



# Insert data into MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['youtube']
collection = db['videos']

foldernames= ['0301','0313','0329','0420']
filenames = ["0","1","2","3"]
filepath = 'D:/fall_2024_coding/dataset/'

collection.drop()  # Clear the collection if needed

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

# Degree Distribution Analysis
degree_distribution = list(
    db.videos.aggregate([
        {"$project": {"degree": {"$size": {"$ifNull": ["$related_ids", []]}}}},
        {"$group": {"_id": "$degree", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ])
)
degree_df = pd.DataFrame(degree_distribution)

# Plot Degree Distribution
plt.figure()
plt.bar(degree_df['_id'], degree_df['count'])
plt.xlabel('Degree')
plt.ylabel('Count')
plt.title('Degree Distribution')
plt.show()

# Category Statistics
category_stats = list(
    db.videos.aggregate([
        {"$group": {"_id": "$category", "video_count": {"$sum": 1}}},
        {"$sort": {"video_count": -1}}
    ])
)
category_df = pd.DataFrame(category_stats)

# Plot Category Statistics
plt.figure()
plt.bar(category_df['_id'], category_df['video_count'])
plt.xlabel('Category')
plt.ylabel('Video Count')
plt.title('Videos per Category')
plt.xticks(rotation=45)
plt.show()

# Size of Video Distribution
size_distribution = list(
    db.videos.aggregate([
        {"$bucket": {
            "groupBy": "$length",
            "boundaries": [0, 60, 300, 900, 3600, float('inf')],
            "default": "Unknown",
            "output": {"video_count": {"$sum": 1}}
        }}
    ])
)
size_df = pd.DataFrame(size_distribution)

# Plot Video Size Distribution
plt.figure()
size_labels = ['<1 min', '1-5 min', '5-15 min', '15-60 min', '>60 min']
plt.bar(size_labels, size_df['video_count'])
plt.xlabel('Video Length')
plt.ylabel('Count')
plt.title('Video Length Distribution')
plt.show()

# View Count Statistics
view_stats = list(
    db.videos.aggregate([
        {"$group": {
            "_id": None,
            "total_views": {"$sum": "$views"},
            "average_views": {"$avg": "$views"}
        }}
    ])
)
print("View Statistics:", view_stats)

##################

# Extract Graph for Influence Analysis
edges = []
cursor = db.videos.find({}, {"video_id": 1, "related_ids": 1})
for doc in cursor:
    video_id = doc['video_id']
    related_ids = doc.get('related_ids', [])
    edges.extend([(video_id, related) for related in related_ids])

# Handle case where edges is empty
if not edges:
    print("No edges found in the dataset. Ensure related_ids are populated.")
else:
    # PageRank Analysis and Visualization
    G = nx.DiGraph()
    G.add_edges_from(edges)

    pagerank = nx.pagerank(G)
    top_videos = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)[:10]
    print("Top 10 Influential Videos by PageRank:", top_videos)

# Plot Network Graph
# Check graph size
print(f"Number of nodes: {G.number_of_nodes()}")
print(f"Number of edges: {G.number_of_edges()}")

# Subset: Use high-degree nodes
top_degree_nodes = sorted(G.degree, key=lambda x: x[1], reverse=True)[:50]  # Top 50 nodes by degree
subset_nodes = [node for node, _ in top_degree_nodes]
subset = G.subgraph(subset_nodes)

# another way to subset, random sampling of nodes
# subset_nodes = random.sample(G.nodes(), min(50, G.number_of_nodes()))
# subset = G.subgraph(subset_nodes)

# Spring layout with subset
try:
    pos = nx.spring_layout(subset, seed=42, k=0.15, iterations=50)
except Exception as e:
    print("Error in spring_layout:", e)
    pos = nx.circular_layout(subset)

# Plot the subset graph
plt.figure(figsize=(10, 10))
nx.draw_networkx_nodes(subset, pos, node_size=50, alpha=0.7)
nx.draw_networkx_edges(subset, pos, alpha=0.5)
plt.title('Subset Network Graph', fontsize=14)
plt.axis('off')
plt.show()



# Subset of the graph (reuse the same subset as above)
subset_uploaders = set()  # Store uploaders corresponding to the subset nodes

# Extract uploaders from subset nodes
for node in subset.nodes():
    uploader = db.videos.find_one({"video_id": node}, {"uploader": 1})
    if uploader and "uploader" in uploader:
        subset_uploaders.add(uploader["uploader"])

# Aggregate uploader influence from the subset
subset_user_influence = list(
    db.videos.aggregate([
        {"$match": {"uploader": {"$in": list(subset_uploaders)}}},  # Filter by subset uploaders
        {"$unwind": "$related_ids"},
        {"$lookup": {
            "from": "videos",
            "localField": "related_ids",
            "foreignField": "video_id",
            "as": "related_videos"
        }},
        {"$group": {
            "_id": "$uploader",
            "influence_score": {"$sum": {"$size": "$related_videos"}}
        }},
        {"$sort": {"influence_score": -1}},
        {"$limit": 10}  # Top 10 uploaders
    ])
)

# Convert to DataFrame for easier plotting
subset_user_influence_df = pd.DataFrame(subset_user_influence)

# Plot Top 10 Influential Uploaders
if not subset_user_influence_df.empty:
    plt.figure(figsize=(10, 6))
    plt.bar(subset_user_influence_df['_id'], subset_user_influence_df['influence_score'], alpha=0.8)
    plt.xlabel('Uploader', fontsize=12)
    plt.ylabel('Influence Score', fontsize=12)
    plt.title('Top 10 Influential Uploaders (Subset)', fontsize=14)
    plt.xticks(rotation=45, fontsize=10)
    plt.tight_layout()
    plt.show()
else:
    print("No influential uploaders found in the subset.")



