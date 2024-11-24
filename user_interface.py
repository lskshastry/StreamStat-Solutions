import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter import ttk

#from youtube_start import extract_zip_files, load_data_from_directory, cleanse_and_transform_data


def process_files():
    zip_directory = filedialog.askdirectory(title="Select a Directory with Zip Files")
    if not zip_directory:
        messagebox.showerror("Error", "No directory selected!")
        return
    
    # Call functiosn from youtube_start to transform and clean data


#maing for our GUI
root = tk.Tk()
root.title("Youtube Data")
root.geometry("500x300")

#buttons
process_button = tk.Button(root, text="Upload and Process Files", command=process_files)
process_button.pack(pady=20)

exit_button = tk.Button(root, text="Exit", command=root.quit)
exit_button.pack(pady=10)

root.mainloop()