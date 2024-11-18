from pymongo import MongoClient

# Connect to the MongoDB server (adjust the connection string if needed)
client = MongoClient('mongodb://localhost:27017/')

# Select the database and collection
db = client['yelpdb']
businesses_collection = db['businesses']

# a. Give a count of all the businesses
def count_all_businesses():
    total_businesses = businesses_collection.count_documents({})
    print(f"Total number of businesses: {total_businesses}")

# b. Find a restaurant with 'stars' value of 4.0
def find_restaurant_with_stars_4():
    restaurant = businesses_collection.find_one({"stars": 4.0})
    if restaurant:
        print("Restaurant with 4.0 stars:")
        print(restaurant)
    else:
        print("No restaurant found with 4.0 stars")

# c. Find all restaurants with 'review_count' >= 500 and 'stars' >= 4.5
def find_top_restaurants():
    top_restaurants = businesses_collection.find({"review_count": {"$gte": 500}, "stars": {"$gte": 4.5}})
    print("Restaurants with review_count >= 500 and stars >= 4.5:")
    for restaurant in top_restaurants:
        print(restaurant)

# Call the functions to perform the queries
if __name__ == "__main__":
    # a. Count all businesses
    count_all_businesses()
    print("\n================================\n")

    # b. Find a restaurant with stars 4.0
    find_restaurant_with_stars_4()
    print("\n================================\n")
    
    # c. Find all restaurants with review_count >= 500 and stars >= 4.5
    find_top_restaurants()
