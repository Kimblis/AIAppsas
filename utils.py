import pandas as pd
import numpy as np
from selenium.webdriver import Chrome
import time
import requests
import os
import wikipedia
import re
from google_images_download import google_images_download
import sqlalchemy as db
from sqlalchemy.sql import func
from datetime import date

imagesDownloader = google_images_download.googleimagesdownload()


def getBoundaries(procSize, rank, dataSize):
    workloads = [dataSize // procSize for i in range(procSize)]
    for i in range(dataSize % procSize):
        workloads[i] += 1
    my_start = 0
    for i in range(rank):
        my_start += workloads[i]
    my_end = my_start + workloads[rank]

    return {'my_start': my_start, 'my_end': my_end}


def normalizeHasNotHas(colName, dt):
    dt[colName] = dt[colName].replace(to_replace="None", value=0)
    dt[colName] = dt[colName].replace(to_replace="[^0]+", value=0, regex=True)
    return dt


def normalizeBooleans(colName, dt):
    dt[colName] = dt[colName].replace(to_replace=False, value=0)
    dt[colName] = dt[colName].replace(to_replace="[^0]+", value=0, regex=True)
    return dt


def normalizeByCount(dataset, column, groupByColumn):
    dataset[column] = dataset[column].str.replace("'", "").str.replace('"', '').replace('', np.nan).str.split(
        ',').fillna({i: [] for i in dataset.index})
    datasetWithCount = (dataset[column].str.len()
                        .groupby(dataset[groupByColumn], sort=False).sum()
                        ).to_frame(name='count').reset_index()
    dataset[column] = datasetWithCount['count']
    dataset[column] = dataset[column].replace(to_replace=np.nan, value=0)
    return dataset


def addDummies(colName, dt):
    dummies = pd.get_dummies(dt[colName])
    dt = dt.join(dummies)
    if ('None' in dt):
        dt = dt.drop('None', axis=1)

    return dt.drop(colName, axis=1)


def addListDummies(colName, dt):
    # pd.set_option('display.max_rows', None)
    dt[colName] = dt[colName].str.replace("'", "").str.replace('"', '').replace('', np.nan).str.split(
        ',').fillna({i: [] for i in dt.index})
    dummies = pd.get_dummies(dt[colName].explode()).groupby(level=0).sum()

    for col in dummies.columns:
        occurance = (dummies[dummies[col] == 1].shape[0])
        if occurance < 100 and len(dt) > 1:
            dummies = dummies.drop(col, axis=1)
    dt = dt.join(dummies)
    if ('None' in dt):
        dt = dt.drop('None', axis=1)

    return dt.drop(colName, axis=1)


def getPlacePhotos(title):
    try:
        currentPhotosCount = 0
        target_folder = os.path.join('./images', '_'.join(title.lower().split(' ')))
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)
        else:
            target_files = os.listdir(target_folder)
            currentPhotosCount = len(target_files)

        fetchLimit = 4 - currentPhotosCount
        query = title + ' facility'
        if fetchLimit > 0:
            offset = 4 - fetchLimit
            with Chrome(executable_path='./chromedriver') as wd:
                res = fetch_image_urls(query, fetchLimit, wd, 0.5)

            for index, elem in enumerate(res):
                persist_image(target_folder, elem, index + 1 + offset)
    except:
        None


def fetch_image_urls(query: str, max_links_to_fetch: int, wd, sleep_between_interactions: int = 1):
    def scroll_to_end(wd):
        wd.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(sleep_between_interactions)

        # build the google query

    search_url = "https://www.google.com/search?safe=off&site=&tbm=isch&source=hp&q={q}&oq={q}&gs_l=img"

    # load the page
    wd.get(search_url.format(q=query))

    image_urls = set()
    image_count = 0
    results_start = 0
    while image_count < max_links_to_fetch:
        scroll_to_end(wd)

        # get all image thumbnail results
        thumbnail_results = wd.find_elements_by_css_selector("img.Q4LuWd")
        number_results = len(thumbnail_results)

        print(f"Found: {number_results} search results. Extracting links from {results_start}:{number_results}")

        for img in thumbnail_results[results_start:number_results]:
            # try to click every thumbnail such that we can get the real image behind it
            try:
                img.click()
                time.sleep(sleep_between_interactions)
            except Exception:
                continue

            # extract image urls
            actual_images = wd.find_elements_by_css_selector('img.n3VNCb')
            for actual_image in actual_images:
                if actual_image.get_attribute('src') and 'http' in actual_image.get_attribute('src'):
                    image_urls.add(actual_image.get_attribute('src'))

            image_count = len(image_urls)

            if len(image_urls) >= max_links_to_fetch:
                print(f"Found: {len(image_urls)} image links, done!")
                break
        else:
            print("Found:", len(image_urls), "image links, looking for more ...")
            time.sleep(30)
            return
            load_more_button = wd.find_element_by_css_selector(".mye4qd")
            if load_more_button:
                wd.execute_script("document.querySelector('.mye4qd').click();")

        # move the result startpoint further down
        results_start = len(thumbnail_results)

    return image_urls


def persist_image(folder_path: str, url: str, index):
    try:
        image_content = requests.get(url, timeout=10).content

    except Exception as e:
        print(f"ERROR - Could not download {url} - {e}")

    try:
        f = open(os.path.join(folder_path, 'jpg' + "_" + str(index) + ".jpg"), 'wb')
        f.write(image_content)
        f.close()
        print(f"SUCCESS - saved {url} - as {folder_path}")
    except Exception as e:
        print(f"ERROR - Could not save {url} - {e}")


def getPlaceInformation(title):
    try:
        wiki = wikipedia.summary(title, sentences=5)
        # Clean text
        wiki = re.sub(r'==.*?==+', '', wiki)
        wiki = wiki.replace('\n', '')
        return wiki
    except:
        return 'None'


def getPlacePhotosPackage(title):
    try:
        imagesDownloader.download({"keywords": title, "limit": 4, "format": 'jpg'})
    except:
        None


def createDatabase(engine):
    metadata = db.MetaData()
    objects = db.Table('objects', metadata,
                       db.Column('qwikidata_id', db.String(255)),
                       db.Column('city_name', db.String(255)),
                       db.Column('category', db.String(255)),
                       db.Column('title', db.String(255)),
                       db.Column('instance_of', db.String(255)),
                       db.Column('pageview_count', db.Integer()),
                       db.Column('registered_contributors_count', db.Integer()),
                       db.Column('anonymous_contributors_count', db.Integer()),
                       db.Column('num_wikipedia_lang_pages', db.Integer()),
                       db.Column('description', db.Text()),
                       db.Column('subsidiaries', db.Text()),
                       db.Column('stock_exchanges', db.String(255)),
                       db.Column('industries', db.String(255)),
                       db.Column('parent_organizations', db.String(255)),
                       db.Column('located_next_to', db.String(255)),
                       db.Column('typically_sells', db.String(255)),
                       db.Column('headquarters', db.String(255)),
                       db.Column('website', db.String(255)),
                       db.Column('owner', db.String(255)),
                       db.Column('named_after', db.String(255)),
                       db.Column('postal_code', db.String(255)),
                       db.Column('foundation_date', db.String(255)),
                       db.Column('fax_number', db.String(255)),
                       db.Column('state', db.String(255)),
                       db.Column('employees', db.Float()),
                       db.Column('total_equity', db.Float()),
                       db.Column('total_revenue', db.Float()),
                       db.Column('students_count', db.Integer()),
                       db.Column('net_profit', db.Float()),
                       db.Column('total_assets', db.Float()),
                       db.Column('volunteers', db.Integer()),
                       db.Column('published_about_on_BBC', db.Boolean()),
                       db.Column('published_about_on_BoardGameGeek', db.Boolean()),
                       db.Column('published_about_on_GaultEtMilau', db.Boolean()),
                       db.Column('got_award', db.Boolean()),
                       db.Column('exists_in_GameFAQ_database', db.Boolean()),
                       db.Column('grid_reference', db.String(255)),
                       db.Column('street_address', db.String(255)),
                       db.Column('freebase_id', db.String(255)),
                       db.Column('twitter_id', db.String(255)),
                       db.Column('whosOnFirst_id', db.String(255)),
                       db.Column('ROR_id', db.String(255)),
                       db.Column('nicoNicoPedia_id', db.String(255)),
                       db.Column('tiktok_username', db.String(255)),
                       db.Column('topTens_id', db.String(255)),
                       db.Column('downDetector_id', db.String(255)),
                       db.Column('formation_location', db.String(255)),
                       db.Column('significant_event', db.String(255)),
                       db.Column('wmi_code', db.String(255)),
                       db.Column('categorical_cluster', db.Integer()),
                       db.Column('text_cluster', db.Integer()),
                       db.Column('photo_cluster', db.Integer()),
                       db.Column("created_at",db.DateTime(timezone=True), default=func.now()),
                       db.Column("updated_at", db.DateTime(timezone=True), default=func.now(), onupdate=func.now())
                       )
    metadata.create_all(engine)  # Creates the table


def seedDatabase(connection, engine):
    metadata = db.MetaData()
    table = db.Table('objects', metadata, autoload_with=engine);
    data = pd.read_csv('./fullDataset.csv', sep=',')
    for i, row in data.iterrows():
        query = db.insert(table).values(qwikidata_id=row['qwikidata_id'],
                       city_name= row['city_name'],
                       category= row['category'],
                       instance_of= row['instance_of'],
                        title=row['title'],
                       pageview_count= row['pageview_count'],
                       registered_contributors_count= row['registered_contributors_count'],
                       anonymous_contributors_count= row['anonymous_contributors_count'],
                       num_wikipedia_lang_pages= row['num_wikipedia_lang_pages'],
                       description= row['description'],
                       subsidiaries= row['subsidiaries'],
                       stock_exchanges= row['stock_exchanges'],
                       industries= row['industries'],
                       parent_organizations= row['parent_organizations'],
                       located_next_to= row['located_next_to'],
                       typically_sells= row['typically_sells'],
                       headquarters= row['headquarters'],
                       website= row['website'],
                       owner= row['owner'],
                       named_after= row['named_after'],
                       postal_code= row['postal_code'],
                       foundation_date= row['foundation_date'],
                       fax_number= row['fax_number'],
                       state= row['state'],
                       employees= row['employees'],
                       total_equity= row['total_equity'],
                       total_revenue= row['total_revenue'],
                       students_count= row['students_count'],
                       net_profit= row['net_profit'],
                       total_assets= row['total_assets'],
                       volunteers= row['volunteers'],
                       published_about_on_BBC= row['published_about_on_BBC'],
                       published_about_on_BoardGameGeek= row['published_about_on_BoardGameGeek'],
                       published_about_on_GaultEtMilau= row['published_about_on_GaultEtMilau'],
                       exists_in_GameFAQ_database= row['exists_in_GameFAQ_database'],
                       grid_reference= row['grid_reference'],
                       street_address= row['street_address'],
                       freebase_id= row['freebase_id'],
                       twitter_id= row['twitter_id'],
                       whosOnFirst_id= row['whosOnFirst_id'],
                       ROR_id= row['ROR_id'],
                       nicoNicoPedia_id= row['nicoNicoPedia_id'],
                       tiktok_username= row['tiktok_username'],
                       topTens_id= row['topTens_id'],
                       downDetector_id= row['downDetector_id'],
                       formation_location= row['formation_location'],
                       significant_event= row['significant_event'],
                       wmi_code=row['wmi_code'],
                        got_award=row['got_award'],
                        created_at=date.today(),
                        updated_at=date.today())


        connection.execute(query)


def insertRecord(connection, engine, data):
    metadata = db.MetaData()
    table = db.Table('objects', metadata, autoload_with=engine);
    query = db.insert(table).values(qwikidata_id=data['qwikidata_id'],
                                    city_name=data['city_name'],
                                    category=data['category'],
                                    instance_of=data['instance_of'],
                                    title=data['title'],
                                    pageview_count=data['pageview_count'],
                                    registered_contributors_count=data['registered_contributors_count'],
                                    anonymous_contributors_count=data['anonymous_contributors_count'],
                                    num_wikipedia_lang_pages=data['num_wikipedia_lang_pages'],
                                    description=data['description'],
                                    subsidiaries=data['subsidiaries'],
                                    stock_exchanges=data['stock_exchanges'],
                                    industries=data['industries'],
                                    parent_organizations=data['parent_organizations'],
                                    located_next_to=data['located_next_to'],
                                    typically_sells=data['typically_sells'],
                                    headquarters=data['headquarters'],
                                    website=data['website'],
                                    owner=data['owner'],
                                    named_after=data['named_after'],
                                    postal_code=data['postal_code'],
                                    foundation_date=data['foundation_date'],
                                    fax_number=data['fax_number'],
                                    state=data['state'],
                                    employees=data['employees'],
                                    total_equity=data['total_equity'],
                                    total_revenue=data['total_revenue'],
                                    students_count=data['students_count'],
                                    net_profit=data['net_profit'],
                                    total_assets=data['total_assets'],
                                    volunteers=data['volunteers'],
                                    published_about_on_BBC=data['published_about_on_BBC'],
                                    published_about_on_BoardGameGeek=data['published_about_on_BoardGameGeek'],
                                    published_about_on_GaultEtMilau=data['published_about_on_GaultEtMilau'],
                                    exists_in_GameFAQ_database=data['exists_in_GameFAQ_database'],
                                    grid_reference=data['grid_reference'],
                                    street_address=data['street_address'],
                                    freebase_id=data['freebase_id'],
                                    twitter_id=data['twitter_id'],
                                    whosOnFirst_id=data['whosOnFirst_id'],
                                    ROR_id=data['ROR_id'],
                                    nicoNicoPedia_id=data['nicoNicoPedia_id'],
                                    tiktok_username=data['tiktok_username'],
                                    topTens_id=data['topTens_id'],
                                    downDetector_id=data['downDetector_id'],
                                    formation_location=data['formation_location'],
                                    significant_event=data['significant_event'],
                                    wmi_code=data['wmi_code'],
                                    got_award=data['got_award'],
                                    created_at=data.today(),
                                    updated_at=data.today())

    connection.execute(query)

def updateRecord(connection, engine, data):
    metadata = db.MetaData()
    table = db.Table('objects', metadata, autoload_with=engine);
    query = db.update(table).values(qwikidata_id=data['qwikidata_id'],
                                    city_name=data['city_name'],
                                    category=data['category'],
                                    instance_of=data['instance_of'],
                                    title=data['title'],
                                    pageview_count=data['pageview_count'],
                                    registered_contributors_count=data['registered_contributors_count'],
                                    anonymous_contributors_count=data['anonymous_contributors_count'],
                                    num_wikipedia_lang_pages=data['num_wikipedia_lang_pages'],
                                    description=data['description'],
                                    subsidiaries=data['subsidiaries'],
                                    stock_exchanges=data['stock_exchanges'],
                                    industries=data['industries'],
                                    parent_organizations=data['parent_organizations'],
                                    located_next_to=data['located_next_to'],
                                    typically_sells=data['typically_sells'],
                                    headquarters=data['headquarters'],
                                    website=data['website'],
                                    owner=data['owner'],
                                    named_after=data['named_after'],
                                    postal_code=data['postal_code'],
                                    foundation_date=data['foundation_date'],
                                    fax_number=data['fax_number'],
                                    state=data['state'],
                                    employees=data['employees'],
                                    total_equity=data['total_equity'],
                                    total_revenue=data['total_revenue'],
                                    students_count=data['students_count'],
                                    net_profit=data['net_profit'],
                                    total_assets=data['total_assets'],
                                    volunteers=data['volunteers'],
                                    published_about_on_BBC=data['published_about_on_BBC'],
                                    published_about_on_BoardGameGeek=data['published_about_on_BoardGameGeek'],
                                    published_about_on_GaultEtMilau=data['published_about_on_GaultEtMilau'],
                                    exists_in_GameFAQ_database=data['exists_in_GameFAQ_database'],
                                    grid_reference=data['grid_reference'],
                                    street_address=data['street_address'],
                                    freebase_id=data['freebase_id'],
                                    twitter_id=data['twitter_id'],
                                    whosOnFirst_id=data['whosOnFirst_id'],
                                    ROR_id=data['ROR_id'],
                                    nicoNicoPedia_id=data['nicoNicoPedia_id'],
                                    tiktok_username=data['tiktok_username'],
                                    topTens_id=data['topTens_id'],
                                    downDetector_id=data['downDetector_id'],
                                    formation_location=data['formation_location'],
                                    significant_event=data['significant_event'],
                                    wmi_code=data['wmi_code'],
                                    got_award=data['got_award'],
                                    created_at=data.today(),
                                    updated_at=data.today()).where(table.columns.qwikidata_id == data['qwikidata_id'] and table.columns.city_name == data['city_name'])

    connection.execute(query)

def updateTextCluster(connection, engine, qwikidata_id, value):
    metadata = db.MetaData()
    table = db.Table('objects', metadata, autoload_with=engine, autoload=True);

    connection.execute(db.update(table).values(text_cluster=value).where(table.columns.qwikidata_id == qwikidata_id))

def updateCategoricalCluster(connection, engine, qwikidata_id, value):
    metadata = db.MetaData()
    table = db.Table('objects', metadata, autoload_with=engine, autoload=True);

    connection.execute(db.update(table).values(categorical_cluster=value).where(table.columns.qwikidata_id == qwikidata_id))

def getRecordsByCategoricalCluster(connection, engine, cluster):
    metadata = db.MetaData()
    table = db.Table('objects', metadata, autoload_with=engine, autoload=True);

    connection.execute(db.select(table).where(table.columns.categorical_cluster == cluster))

def getRecordByQwikidataId(connection, engine, qwikidataId):
    metadata = db.MetaData()
    table = db.Table('objects', metadata, autoload_with=engine, autoload=True);

    connection.execute(db.select(table).where(table.columns.qwikidata_id == qwikidataId))

def updatePhotoCluster(connection, engine, qwikidata_id, value):
    metadata = db.MetaData()
    table = db.Table('objects', metadata, autoload_with=engine, autoload=True);

    connection.execute(db.update(table).values(photo_cluster=value).where(table.columns.qwikidata_id == qwikidata_id))

def getRecordsByPhotoCluster(connection, engine, cluster):
    metadata = db.MetaData()
    table = db.Table('objects', metadata, autoload_with=engine, autoload=True);

    connection.execute(db.select(table).where(table.columns.photo_cluster == cluster))

def fetchDataFromDatabase(connection, engine):
    metadata = db.MetaData()
    table = db.Table('objects', metadata, autoload_with=engine, autoload=True)
    ResultProxy = connection.execute(db.select([table]))
    ResultSet = ResultProxy.fetchall()
    return pd.DataFrame(ResultSet)



