import os

# Check if the file exists
file_path = 'scripts/data/wiki_links.txt'
if not os.path.exists(file_path):
    raise FileNotFoundError(f"The file {file_path} does not exist.")