import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter import ttk
from pymongo import MongoClient
from tempfile import mkdtemp

from youtube_start import extract_zip_files, load_data_from_directory, cleanse_and_transform_data
from youtube_mongoDB import load_folder, store_unique_videos, convert_data_types, fill_missing_data, print_data, insert_to_mongoDB

client = MongoClient('mongodb://localhost:27017/')
db = client['youtube']
collection = db['videos']

def process_files():
    zip_directory = filedialog.askdirectory(title="Select a Directory with Zip Files")
    if not zip_directory:
        messagebox.showerror("Error", "No directory selected!")
        return

    # Temporary directory to store extracted files
    extracted_directory = filedialog.askdirectory(title="Select a Directory for Extracted Files")
    if not extracted_directory:
        messagebox.showerror("Error", "No extraction directory selected!")
        return

    # Insert to MongoDB and show a success message
    try:
        # Step 1: extract
        extract_zip_files(zip_directory, extracted_directory)
        # Step 2: Load
        raw_data = load_data_from_directory(extracted_directory)
        # Step 3: Clean and Transform
        cleaned_data = cleanse_and_transform_data(raw_data)
        # Load and process video data
        video_data = load_folder(extracted_directory)
        df = store_unique_videos(video_data)
        df = convert_data_types(df)
        df = fill_missing_data(df)
        insert_to_mongoDB(df)
        messagebox.showinfo("Success", "Data inserted into Mongo!")
    except Exception as e:
        messagebox.showerror("Error", f"Failed to insert dtta into Mongo: {e}")

    # Maybe user hadoop/spark here to query from mongo to display data



def display_data():
    # Fetch data from MongoDB
    try:
        data = collection.find().limit(100)  # Limit to 100 rows for simplicity
        if data.count() == 0:
            messagebox.showinfo("Info", "No data available in MongoDB.")
            return
    except Exception as e:
        messagebox.showerror("Error", f"Failed to fetch data: {e}")
        return

    # Create a new window for displaying data
    data_window = tk.Toplevel(root)
    data_window.title("YouTube Data")
    data_window.geometry("800x400")

    # Create a Treeview widget
    tree = ttk.Treeview(data_window, columns=("video_id", "uploader", "age", "category", "length", "views", "rate", "ratings", "comments"), show="headings")
    tree.pack(fill="both", expand=True)

    # Define column headers
    columns = ["video_id", "uploader", "age", "category", "length", "views", "rate", "ratings", "comments"]
    for col in columns:
        tree.heading(col, text=col)
        tree.column(col, width=100, anchor="center")

    # Populate the Treeview with data
    for row in data:
        tree.insert("", "end", values=(
            row.get("video_id"),
            row.get("uploader"),
            row.get("age"),
            row.get("category"),
            row.get("length"),
            row.get("views"),
            row.get("rate"),
            row.get("ratings"),
            row.get("comments")
        ))

    # Add a close button
    close_button = tk.Button(data_window, text="Close", command=data_window.destroy)
    close_button.pack(pady=10)


#maing for our GUI
root = tk.Tk()
root.title("Youtube Data")
root.geometry("500x300")

#buttons

#Button to upload files
process_button = tk.Button(root, text="Upload and Process Files", command=process_files)
process_button.pack(pady=20)

#Button to display data
display_button = tk.Button(root, text="Display Data", command=display_data)
display_button.pack(pady=10)

exit_button = tk.Button(root, text="Exit", command=root.quit)
exit_button.pack(pady=10)

root.mainloop()