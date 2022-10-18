import os
import csv
import gc
import time
from shapely.geometry import shape, Point
from qwikidata.linked_data_interface import get_entity_dict_from_api
from mpi4py import MPI
from utils import getBoundaries, insertRecord, updateRecord, getRecordByQwikidataId
from expandObjectData import expandObjectData
import xml.etree.ElementTree as ET
import numpy as np

def filterData(osmIds, cityNames, polygons, chunksize):
    buildingKeys = ['amenity', 'building']
    facilitiesToGetADrinkBuildingValues = ['bar', 'restaurant', 'pub', 'cafe', 'fast_food']
    facilitiesToGetADrinkKeys = ['cuisine']
    transportationKeys = ['public_transport', 'railway', 'highway']
    transportationBuildingValues = ['fuel', 'car_rental']
    shoppingFacilitiesKeys = ['shop']
    educationBuildingValues = ['library', 'school', 'university', 'prep_school', 'driving_school', 'music_school',
                               'cooking_school']
    healthcareFacilitiesKeys = ['healthcare']
    healthcareFacilitiesBuildingValues = ['pharmacy', 'hospital']
    overpass_files = os.listdir("./overpass_data")

    lines = []
    total_datapoints = 0
    wiki_datapoints = 0

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    procSize = comm.Get_size()
    for check in range(30):
        if (len(overpass_files) >= chunksize * chunksize * len(osmIds)):
            break;
        time.sleep(5 * 60)

    overpassBoundaries = getBoundaries(procSize, rank, len(overpass_files))
    for i in range(overpassBoundaries['my_start'], overpassBoundaries['my_end']):
        file = overpass_files[i]
        filename = f"./overpass_data/{file}"
        print(f"Starting to analyze {filename}")
        tree = ET.parse(filename)
        root = tree.getroot()
        nnn = []
        for node in root.iter('node'):
            nnn.append(node)
        for node in root.iter('way'):
            nnn.append(node)
        root.clear()

        for node in nnn:
            if 'lat' not in node.attrib or 'lon' not in node.attrib:
                continue

            wiki_value = None

            for tag in node.iter('tag'):
                if 'wikidata' in tag.attrib['k']:
                    wiki_value = tag.attrib['v']

            total_datapoints += 1
            if wiki_value is None:
                continue;
            wiki_datapoints += 1

            placeCategory = None
            for tag in node.iter('tag'):
                if (tag.attrib['k'] in buildingKeys and tag.attrib['v'] in facilitiesToGetADrinkBuildingValues) or (
                        tag.attrib['k'] in facilitiesToGetADrinkKeys):
                    placeCategory = 'Grab a drink'
                if (tag.attrib['k'] in buildingKeys and tag.attrib['v'] in transportationBuildingValues) or (
                        tag.attrib['k'] in transportationKeys):
                    placeCategory = 'Transportation'
                if tag.attrib['k'] in shoppingFacilitiesKeys:
                    placeCategory = 'Shopping'
                if tag.attrib['k'] in buildingKeys and tag.attrib['v'] in educationBuildingValues:
                    placeCategory = 'Education'
                if (tag.attrib['k'] in buildingKeys and tag.attrib['v'] in healthcareFacilitiesBuildingValues) or (
                        tag.attrib['k'] in healthcareFacilitiesKeys):
                    placeCategory = 'Healthcare'
                if tag.attrib['k'] in buildingKeys:
                    placeCategory = 'Other'
            if placeCategory is None:
                continue;

            # check if object is in the city boundaries
            osmId = file.split('_')[0]
            lat = float(node.attrib['lat'])
            lng = float(node.attrib['lon'])
            polygon = shape(polygons[osmId])

            if not polygon.contains(Point(lng, lat)):
                continue

            line = [wiki_value, cityNames[osmIds.index(osmId)], placeCategory]
            lines.append(line)
        del nnn
        gc.collect(generation=2)

    print("My gathered wiki lines count:", + len(lines), "my rank is", + rank)

    data = comm.gather(lines, root=0)
    comm.Barrier()

    totalDatapoints = comm.reduce(total_datapoints, op=MPI.SUM, root=0)
    if rank == 0:
        data = [item for sublist in data for item in sublist]
        data = np.unique(np.array(data), axis=0)
        print("Total datapoints gathered were: ", totalDatapoints)
        print("Wiki datapoints after filtering: ", len(data))

        return data

def processWikiArray(data, engine):
    connection = engine.connect()
    finalFileHeaders = ["qwikidata_id", "city_name", "category", "title", "instance_of", "pageview_count",
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
                        "formation_location", "significant_event", "wmi_code"]
    fullLines = []

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    procSize = comm.Get_size()

    data = comm.bcast(data, root=0)

    for check in range(10):
        if (data is not None):
            break;
        time.sleep(60)

    print('Starting to expand objects, my rank is: ', + rank)
    boundaries = getBoundaries(procSize, rank, len(data))
    for i in range(boundaries['my_start'], boundaries['my_end']):
        line = data[i]
        try:
            qwiki_dict = get_entity_dict_from_api(line[0])
            part2Data = expandObjectData(qwiki_dict)
            if part2Data is None or len(part2Data) < 1:
                continue

            fullLine = np.concatenate((line, np.array(part2Data)))
            fullLines.append(fullLine)
        except:
            continue

    data = comm.gather(fullLines, root=0)
    if rank == 0:
        data = [item for sublist in data for item in sublist]
        for line in data:
            record = getRecordByQwikidataId(line.qwikidata_id, line.city_name)
            if record is not None:
                updateRecord(connection, engine, line)
            else:
                insertRecord(connection, engine, line)




