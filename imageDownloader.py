import pandas as pd
import os
from google_images_download import google_images_download

imagesDownloader = google_images_download.googleimagesdownload()
csv_data = pd.read_csv('./fullDataset.csv', sep=',', encoding='latin-1')

# Remove unnecessary columns
data = csv_data.iloc[:, 2:]
for i, row in data.iterrows():
    try:
        if row['title'] != 'None' and not os.path.exists('./downloads/' + row['title']):
            imagesDownloader.download({"keywords": row['title'], "limit": 4})
    except:
        None