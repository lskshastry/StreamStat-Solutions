import os
import zipfile
import pandas as pd

# Step 1: Extract Zip Files
# def extract_zip_files(zip_dir, extract_to):
#     for file_name in os.listdir(zip_dir):
#         if file_name.endswith('.zip'):
#             file_path = os.path.join(zip_dir, file_name)
#             with zipfile.ZipFile(file_path, 'r') as zip_ref:
#                 zip_ref.extractall(extract_to)
#             print(f"Extracted: {file_name}")

# # Define directories for extraction
#zip_directory = 'C:/Users/Gurma/Desktop/School/Fall24/415/GroupWork3/DataZip'
#extracted_directory = 'C:/Users/Gurma/Desktop/School/Fall24/415/GroupWork3/DataExtracted'

def extract_zip_files(zip_dir, extract_to):
        # Ensure the target directory exists
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)

    # Iterate through all .zip files in the zip directory
    for file_name in os.listdir(zip_dir):
        if file_name.endswith('.zip'):
            file_path = os.path.join(zip_dir, file_name)
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            print(f"Extracted: {file_name}")

    if not os.path.exists(extract_to):
        os.makedirs(extract_to)

    # Iterate through all files in the zip directory
    for file_name in os.listdir(zip_dir):
        if file_name.endswith('.zip'):
            file_path = os.path.join(zip_dir, file_name)
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            print(f"Extracted: {file_name}")

# Extract the zip files
#extract_zip_files(zip_directory, extracted_directory)

# Step 2: Load Data from Extracted Files
def load_data_from_directory(directory):
    data_frames = []
    for root, _, files in os.walk(directory):
        folder_data_frames = []
        for file_name in files:
            if file_name.endswith('.txt') and file_name != 'log.txt':
                file_path = os.path.join(root, file_name)
                try:
                    df = pd.read_csv(file_path, delimiter='\t', header=None)
                    folder_data_frames.append(df)
                    print(f"Loaded file: {file_path}")
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")

        if folder_data_frames:
            folder_df = pd.concat(folder_data_frames, ignore_index=True)
            data_frames.append(folder_df)
            folder_name = os.path.basename(root)
            print(f"Loaded {len(folder_df)} rows of data from folder {folder_name}\n")

    if not data_frames:
        print("No .txt files were loaded.")
    return pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()

# Load data from extracted directory
#raw_data = load_data_from_directory(extracted_directory)
#print(f"Loaded {len(raw_data)} rows of data in total.\n\nStarting Data Cleaning\n")

# Step 3: Cleanse and Transform Data
def cleanse_and_transform_data(df):
    # Rename columns as per given information
    base_columns = ['video_id', 'uploader', 'age', 'category', 'length', 'views', 'rate', 'ratings', 'comments']
    related_id_columns = [f'related_id_{i+1}' for i in range(df.shape[1] - len(base_columns))]
    
    # Combine base and related id columns
    all_columns = base_columns + related_id_columns
    df.columns = all_columns

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

    # Convert related_id columns to strings
    for col in related_id_columns:
        df[col] = df[col].astype(str)

    # Handle missing values
    df.fillna({
        'age': df['age'].median(),
        'length': df['length'].median(),
        'views': df['views'].median(),
        'rate': df['rate'].mean(),
        'ratings': 0,
        'comments': 0
    }, inplace=True)

    # New Milestone 3 - check and remove dupe's based on video_id
    initial_row_count = len(df)
    df = df.drop_duplicates(subset=['video_id'])
    final_row_count = len(df)
    duplicates_removed = initial_row_count - final_row_count
    print(f"Removed {duplicates_removed} duplicate rows based on 'video_id'.")

    return df

# Clean and transform the data
#cleaned_data = cleanse_and_transform_data(raw_data)
#print("Data Cleaning Finished!")
#print(cleaned_data.head())  # Display some cleaned data to verify
