import sys
import zipfile
import pandas as pd
from pymongo import MongoClient
import networkx as nx
import matplotlib.pyplot as plt
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QFileDialog, QVBoxLayout, QPushButton, QLabel, QWidget, QTextEdit
)


class YouTubeAnalysisApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("YouTube Data Analysis")
        self.setGeometry(100, 100, 800, 600)
        self.initUI()

        # MongoDB client and database
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['youtube']
        self.collection = self.db['videos']

    def initUI(self):
        # Main layout
        layout = QVBoxLayout()

        # Buttons for functionality
        self.load_data_btn = QPushButton("Load and Process Data")
        self.load_data_btn.clicked.connect(self.load_data)
        layout.addWidget(self.load_data_btn)

        self.analysis_btn = QPushButton("Run Analysis")
        self.analysis_btn.clicked.connect(self.run_analysis)
        layout.addWidget(self.analysis_btn)

        self.plot_btn = QPushButton("Show Graph")
        self.plot_btn.clicked.connect(self.show_graph)
        layout.addWidget(self.plot_btn)

        # Text box to display results
        self.result_text = QTextEdit()
        self.result_text.setReadOnly(True)
        layout.addWidget(self.result_text)

        # Label to display status
        self.status_label = QLabel("")
        layout.addWidget(self.status_label)

        # Main container
        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

    def load_data(self):
        # Open file chooser dialog to select multiple zip files
        files, _ = QFileDialog.getOpenFileNames(self, "Select Zip Files", "", "Zip Files (*.zip)")
        if not files:
            self.status_label.setText("No files selected.")
            return

        self.status_label.setText("Loading and processing data...")
        self.collection.drop()  # Clear the collection if needed

        # Dictionary to store videos, with video_id as the key
        merged_videos = {}
        for file in files:
            with zipfile.ZipFile(file, 'r') as zip_ref:
                for filename in zip_ref.namelist():
                    with zip_ref.open(filename) as f:
                        lines = f.readlines()
                        for line in lines:
                            parts = line.decode('utf-8').strip().split('\t')
                            if len(parts) < 9:
                                continue
                            video_data = {
                                "video_id": parts[0],
                                "uploader": parts[1],
                                "age": int(parts[2]),
                                "category": parts[3],
                                "length": int(parts[4]),
                                "views": int(parts[5]),
                                "rate": float(parts[6]),
                                "ratings": int(parts[7]),
                                "comments": int(parts[8]),
                                "related_ids": parts[9:]
                            }
                            merged_videos[video_data['video_id']] = video_data

        # Insert deduplicated data into MongoDB
        videos_data_for_mongo = list(merged_videos.values())
        self.collection.insert_many(videos_data_for_mongo)
        self.status_label.setText(f"Processed {len(videos_data_for_mongo)} videos.")

    def run_analysis(self):
        self.status_label.setText("Running analysis...")
        self.result_text.clear()

        # Degree Distribution
        degree_distribution = list(
            self.db.videos.aggregate([
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
            self.db.videos.aggregate([
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

        # Video Size Distribution
        size_distribution = list(
            self.db.videos.aggregate([
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

        # Display analysis summary in the text box
        self.result_text.append("Analysis Results:")
        self.result_text.append(f"Degree Distribution:\n{degree_df.to_string(index=False)}")
        self.result_text.append(f"Category Statistics:\n{category_df.to_string(index=False)}")
        self.result_text.append(f"Video Size Distribution:\n{size_df.to_string(index=False)}")

        self.status_label.setText("Analysis completed!")

    def show_graph(self):
        self.status_label.setText("Generating graph...")
        self.result_text.clear()

        # Extract Graph for Influence Analysis
        edges = []
        cursor = self.db.videos.find({}, {"video_id": 1, "related_ids": 1})
        for doc in cursor:
            video_id = doc['video_id']
            related_ids = doc.get('related_ids', [])
            edges.extend([(video_id, related) for related in related_ids])

        if not edges:
            self.status_label.setText("No edges found in the dataset.")
            return

        G = nx.DiGraph()
        G.add_edges_from(edges)

        # PageRank Analysis
        pagerank = nx.pagerank(G)
        top_videos = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)[:10]
        self.result_text.append("Top 10 Influential Videos by PageRank:")
        for video_id, score in top_videos:
            self.result_text.append(f"Video ID: {video_id}, PageRank Score: {score}")

        # Limit graph to a subset of nodes (e.g., top 50 high-degree nodes)
        top_degree_nodes = sorted(G.degree, key=lambda x: x[1], reverse=True)[:50]
        subset_nodes = [node for node, _ in top_degree_nodes]
        subset_nodes = sorted(subset_nodes)
        subset = G.subgraph(subset_nodes)

        # Spring layout with subset
        pos = nx.spring_layout(subset, seed=42, k=0.15, iterations=50)

        # Plot the subset graph
        plt.figure(figsize=(10, 10))
        nx.draw_networkx_nodes(subset, pos, node_size=50, alpha=0.7)
        nx.draw_networkx_edges(subset, pos, alpha=0.5)
        nx.draw_networkx_labels(subset, pos, font_size=8)
        plt.title('Subset Network Graph', fontsize=14)
        plt.axis('off')
        plt.show()

        self.status_label.setText("Graph displayed!")


# Run the application
if __name__ == "__main__":
    app = QApplication(sys.argv)
    main_window = YouTubeAnalysisApp()
    main_window.show()
    sys.exit(app.exec_())
