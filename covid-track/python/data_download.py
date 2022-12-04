import requests
import os
import sys

URL = sys.argv[1]

def down_load(url: str, dest_folder: str):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder) # create folder if it not exist

    filename = "data.csv"
    filepath = os.path.join(dest_folder, filename)

    r = requests.get(url, stream=True)

    if r.ok:
        print("saving to", os.path.abspath(filepath))
        with open(filepath, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024*8):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
                else:
                    print("Download failsed")
down_load(url=URL, dest_folder="/tmp/covid_data")
