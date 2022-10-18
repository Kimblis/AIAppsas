import requests
import pickle
from mpi4py import MPI
from gatherData import gatherData
from filterData import filterData, processWikiArray
from kmeans import processData, applyKMeans, applyKmeansText, applyKmeansPhotos, getReducedFeatures
from utils import createDatabase, seedDatabase, fetchDataFromDatabase, getPlacePhotosPackage
import sqlalchemy as db
from flask import Flask, jsonify, request
from flask_cors import CORS
from qwikidata.linked_data_interface import get_entity_dict_from_api
from expandObjectData import expandObjectData
import numpy as np
import pandas as pd

# Create db connection for further usage
engine = db.create_engine('mysql://root:root@localhost:3307/big_data')
dbConnection = engine.connect()
# Make db migrations and seeds if needed
# createDatabase(engine)
# seedDatabase(dbConnection, engine)

app = Flask(__name__)
CORS(app)

def scrapeInformation():
    cityNames = ["Poland Warsaw", "Luxembourg Luxemburg", "United Kingdom London", "Czechia Prague", "Germany Hamburg",
                 "Lithuania Kaunas", "Spain Madrid", "Sweden Stockholm", "Spain Barcelona", "France Paris",
                 "Germany Munich", "Austria Vienna"]
    osmIds = ["336075", "2171347", "65606", "435514", "62782", "1567544", "5326784", "398021", "349035", "7444",
              "62428", "109166"]
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    polygons = {}
    if rank == 0:
        for osmID in osmIds:
            url = "https://nominatim.openstreetmap.org/details.php?osmtype=R&osmid=" + osmID + "&class=boundary&addressdetails=1&hierarchy=0&group_hierarchy=1&format=json&polygon_geojson=1"
            r = requests.get(url)
            jsonResult = r.json()
            polygon = jsonResult['geometry']
            polygons[osmID] = polygon
    else:
         polygons = {}
    polygons = comm.bcast(polygons, root=0)

    gatherData(osmIds, polygons)
    data = filterData(osmIds, cityNames, polygons, 5)
    processWikiArray(data)

# Scrape information if needed
# scrapeInformation()
data = fetchDataFromDatabase(dbConnection, engine)
# titles = data['title']
applyKmeansText(data, dbConnection, engine)
# processedDataForStructuralVariables = processData(data,engine)
applyKMeans(data, processedDataForStructuralVariables, dbConnection, engine)

# applyKmeansPhotos(data)

@app.route("/text", methods=['POST'])
def predictText():
    textModel = pickle.load(open("textModel.pkl", "rb"))
    vectorizer = pickle.load(open("vectorizer.pkl", 'rb'))

    data = request.get_json()
    description = data['description']
    X = vectorizer.transform([description])
    cluster = textModel.predict(X)[0]
    records = []
    queryResult = dbConnection.execute(f"SELECT * FROM objects WHERE objects.text_cluster = {cluster} AND objects.title != 'None'")
    for result in queryResult:
        r_dict = dict(result.items())
        records.append(r_dict)
    return jsonify(records)

@app.route("/photo", methods=['POST'])
def predictPhoto():
    photoModel = pickle.load(open("photoModel.pkl", "rb"))
    data = request.get_json()
    title = data['title']

    getPlacePhotosPackage(title)
    x = getReducedFeatures(title)
    cluster = photoModel.predict(x)[0]
    records = []
    queryResult = dbConnection.execute(f"SELECT * FROM objects WHERE objects.photo_cluster = {cluster}")
    for result in queryResult:
        r_dict = dict(result.items())
        records.append(r_dict)
    return jsonify(records)

@app.route("/categorical", methods=['POST'])
def predictCategorical():
    categoricalModel = pickle.load(open("categoricalModel.pkl", "rb"))
    data = request.get_json()
    qwikidata_id = data['qwikidataId']

    qwiki_dict = get_entity_dict_from_api(qwikidata_id)
    qwikidata = expandObjectData(qwiki_dict)
    fullData = np.concatenate(([qwikidata_id, 'None', 'None'], np.array(qwikidata)))
    dataset = pd.DataFrame([fullData],
                      columns=("qwikidata_id", "city_name", "category", "title", "instance_of", "pageview_count",
                        "registered_contributors_count",
                        "anonymous_contributors_count", "num_wikipedia_lang_pages", "description", "subsidiaries",
                        "stock_exchanges",
                        "industries", "parent_organizations", "located_next_to", "typically_sells", "headquarters",
                        "website", "owner",
                        "named_after", "postal_code", "foundation_date", "fax_number", "state", "employees",
                        "total_equity", "total_revenue",
                        "students_count", "net_profit", "total_assets", "volunteers", "published_about_on_BBC",
                        "published_about_on_BoardGameGeek",
                        "published_about_on_GaultEtMilau", "got_award", "exists_in_GameFAQ_database", "grid_reference",
                        "street_address", "freebase_id",
                        "twitter_id", "whosOnFirst_id", "ROR_id", "nicoNicoPedia_id", "tiktok_username", "topTens_id",
                        "downDetector_id",
                        "formation_location", "significant_event", "wmi_code"))
    processedData = processData(dataset)

    cluster = categoricalModel.predict(processedData)[0]
    records = []
    queryResult = dbConnection.execute(f"SELECT * FROM objects WHERE objects.categorical_cluster = {cluster}")
    for result in queryResult:
        r_dict = dict(result.items())
        records.append(r_dict)
    return jsonify(records)

