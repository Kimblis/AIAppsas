# Sklearn
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn import preprocessing
from sklearn.metrics import adjusted_rand_score
from sklearn.decomposition import PCA

# Keras for loading/processing the images
from keras.preprocessing.image import load_img
from keras.preprocessing.image import img_to_array
from keras.applications.vgg16 import preprocess_input

# Keras models
from keras.applications.vgg16 import VGG16
from keras.models import Model

# Others
import pandas as pd
import numpy as np
from utils import normalizeHasNotHas, normalizeBooleans, addDummies, addListDummies, normalizeByCount,getPlacePhotos, getPlaceInformation, getPlacePhotosPackage, updateTextCluster, updateCategoricalCluster
from kneed import KneeLocator
import matplotlib.pyplot as plt
import warnings
from sklearn.decomposition import PCA
import seaborn as sns
import pickle
import os

warnings.filterwarnings("ignore")

def processData(data):
    data_copy = data.copy()
    # Normalize some columns to consist of a count to consist of count
    data_copy = normalizeByCount(data_copy, 'subsidiaries', 'qwikidata_id')
    data_copy = normalizeByCount(data_copy, 'parent_organizations', 'qwikidata_id')

    # Remove unnecessary columns
    try:
        data_copy.drop("qwikidata_id", axis=1, inplace=True)
        data_copy.drop("city_name", axis=1, inplace=True)
        data_copy.drop("title", axis=1, inplace=True)
        data_copy.drop("owner", axis=1, inplace=True)
        data_copy.drop("headquarters", axis=1, inplace=True)
        data_copy.drop("foundation_date", axis=1, inplace=True)
        data_copy.drop("located_next_to", axis=1, inplace=True)
        data_copy.drop("description", axis=1, inplace=True)
        data_copy.drop("created_at", axis=1, inplace=True)
        data_copy.drop("updated_at", axis=1, inplace=True)
        data_copy.drop("text_cluster", axis=1, inplace=True)
        data_copy.drop("categorical_cluster", axis=1, inplace=True)
        data_copy.drop("photo_cluster", axis=1, inplace=True)
    except:
        None

    # # Normalize columns in has/not has manner.
    data_copy = normalizeHasNotHas("tiktok_username", data_copy)
    data_copy = normalizeHasNotHas("topTens_id", data_copy)
    data_copy = normalizeHasNotHas("grid_reference", data_copy)
    data_copy = normalizeHasNotHas("street_address", data_copy)
    data_copy = normalizeHasNotHas("freebase_id", data_copy)
    data_copy = normalizeHasNotHas("twitter_id", data_copy)
    data_copy = normalizeHasNotHas("whosOnFirst_id", data_copy)
    data_copy = normalizeHasNotHas("ROR_id", data_copy)
    data_copy = normalizeHasNotHas("nicoNicoPedia_id", data_copy)
    data_copy = normalizeHasNotHas("downDetector_id", data_copy)
    data_copy = normalizeHasNotHas("formation_location", data_copy)
    data_copy = normalizeHasNotHas("significant_event", data_copy)
    data_copy = normalizeHasNotHas("wmi_code", data_copy)
    data_copy = normalizeHasNotHas("fax_number", data_copy)
    data_copy = normalizeHasNotHas("postal_code", data_copy)
    data_copy = normalizeHasNotHas("named_after", data_copy)
    data_copy = normalizeHasNotHas("website", data_copy)

    data_copy = normalizeBooleans("published_about_on_BBC", data_copy)
    data_copy = normalizeBooleans("published_about_on_BoardGameGeek", data_copy)
    data_copy = normalizeBooleans("published_about_on_GaultEtMilau", data_copy)
    data_copy = normalizeBooleans("got_award", data_copy)
    data_copy = normalizeBooleans("exists_in_GameFAQ_database", data_copy)

    # # Add dummies for columns
    data_copy = addDummies('category', data_copy)
    data_copy = addDummies('state', data_copy)
    data_copy = addDummies('typically_sells', data_copy)

    # # Add dummies for columns consisting of list of values (instance_of, stock_exchanges, industries)
    data_copy = addListDummies('instance_of', data_copy)
    data_copy = addListDummies('stock_exchanges', data_copy)
    data_copy = addListDummies('industries', data_copy)
    #
    # # Normalize numbers
    data_copy["pageview_count"] = preprocessing.scale(data_copy["pageview_count"])
    data_copy["registered_contributors_count"] = preprocessing.scale(data_copy["registered_contributors_count"])
    data_copy["anonymous_contributors_count"] = preprocessing.scale(data_copy["anonymous_contributors_count"])
    data_copy["num_wikipedia_lang_pages"] = preprocessing.scale(data_copy["num_wikipedia_lang_pages"])
    data_copy["employees"] = preprocessing.scale(data_copy["employees"])
    data_copy["total_equity"] = preprocessing.scale(data_copy["total_equity"])
    data_copy["total_revenue"] = preprocessing.scale(data_copy["total_revenue"])
    data_copy["students_count"] = preprocessing.scale(data_copy["students_count"])
    data_copy["net_profit"] = preprocessing.scale(data_copy["net_profit"])
    data_copy["total_assets"] = preprocessing.scale(data_copy["total_assets"])
    data_copy["volunteers"] = preprocessing.scale(data_copy["volunteers"])

    if(len(data_copy) < 2):
        columnNames = pickle.load(open("categorical_names", "rb"))
        currentColumns = data_copy.columns
        for column in currentColumns:
            if column in columnNames:
                continue;
            else:
                data_copy.drop(column, axis=1, inplace=True)

        for column in columnNames:
            if column in data_copy:
                continue;
            else:
                data_copy[column] = 0

    return data_copy

def findOptimalNumberOfClusters(data, kmeans_kwargs):
    Sum_of_squared_distances = []
    clusters_range = range(1, 15)
    for k in clusters_range:
        km = KMeans(n_clusters=k, **kmeans_kwargs).fit(data)
        Sum_of_squared_distances.append(km.inertia_)

    plt.plot(clusters_range, Sum_of_squared_distances, 'bx-')
    plt.xlabel('k')
    plt.ylabel('Sum_of_squared_distances')
    plt.title('Elbow Method For Optimal k')
    plt.show()

    return KneeLocator(clusters_range, Sum_of_squared_distances, curve="convex",
                                   direction="decreasing").elbow

def extract_features(image, title, model):
    # load the image as a 224x224 array
    img = load_img(f"./downloads/{title}/{image}", target_size=(224,224))
    # convert from 'PIL.Image.Image' to numpy array
    img = np.array(img)
    # reshape the data for the model reshape(num_of_samples, dim 1, dim 2, channels)
    reshaped_img = img.reshape(1,224,224,3)
    # prepare image for model
    imgx = preprocess_input(reshaped_img)
    # get the feature vector
    features = model.predict(imgx, use_multiprocessing=True)
    return features

def applyKMeans(data, processedData, dbConnection, engine):
    kmeans_kwargs = {"init": "random", "n_init": 10, "max_iter": 300, "random_state": 42}
    numberOfClusters = findOptimalNumberOfClusters(processedData, kmeans_kwargs)

    print('Optimal number of clusters: ', numberOfClusters)
    model = KMeans(n_clusters=4, **kmeans_kwargs)
    data['clusters'] = model.fit_predict(processedData)
    print(data['clusters'].value_counts())
    pickle.dump(model, open("categoricalModel.pkl", "wb"))
    for i, row in data.iterrows():
        updateCategoricalCluster(dbConnection, engine, row['qwikidata_id'], row['clusters']);

    pca_num_components = 2
    reduced_data = PCA(n_components=pca_num_components).fit_transform(processedData)
    results = pd.DataFrame(reduced_data, columns=['pca1', 'pca2'])

    sns.scatterplot(x="pca1", y="pca2", hue=data['clusters'], data=results)
    plt.title('K-means Clusters')
    plt.show()

def applyKmeansText(data, dbConnection, engine):
    descriptions = data['description']
    kmeans_kwargs = {"init": "k-means++", "n_init": 5, "max_iter": 100, "random_state": 42}
    vectorizer = TfidfVectorizer(stop_words='english')
    X = vectorizer.fit_transform(descriptions)
    numberOfClusters = findOptimalNumberOfClusters(X, kmeans_kwargs)
    print('Optimal number of clusters: ', numberOfClusters)

    model = KMeans(n_clusters=numberOfClusters, **kmeans_kwargs)
    data['cluster'] = model.fit_predict(X)
    # for i, row in data.iterrows():
    #     updateTextCluster(dbConnection, engine, row['qwikidata_id'], row['cluster']);

    print("Top terms per cluster:")
    order_centroids = model.cluster_centers_.argsort()[:, ::-1]
    terms = vectorizer.get_feature_names()
    for i in range(numberOfClusters):
        print("Cluster %d:" % i),
        for ind in order_centroids[i, :10]:
            print(' %s' % terms[ind]),
        print

    # pickle.dump(model, open("textModel.pkl", "wb"))
    # pickle.dump(vectorizer, open("vectorizer.pkl", "wb"))
    # print("\n")
    # print("Prediction")


def getReducedFeatures(title):
    model = VGG16()
    model = Model(inputs=model.inputs, outputs=model.layers[-2].output)
    imageData = {}
    try:
        if(os.path.exists(f"./downloads/{title}")):
            images = os.listdir(f"./downloads/{title}")
            for image in images:
                feat = extract_features(image, title, model)
                imageData[image] = feat
    except:
        None

    filenames = np.array(list(imageData.keys()))
    feats = np.array(list(imageData.values()))
    feats = feats.reshape(-1, 4096)

    pca = PCA(n_components=2, random_state=22)
    pca.fit(feats)
    return pca.transform(feats)


def applyKmeansPhotos(data):
    titles = data['title']
    # Filter for unique titles
    filteredSet = set(titles)
    titles = list(filteredSet)
    model = VGG16()
    model = Model(inputs=model.inputs, outputs=model.layers[-2].output)
    imageData = {}

    for title in titles:
        try:
            if(os.path.exists(f"./downloads/{title}")):
                images = os.listdir(f"./downloads/{title}")
                for image in images:
                    feat = extract_features(image, title, model)
                    imageData[image] = feat
        except:
            None

    filenames = np.array(list(imageData.keys()))
    feats = np.array(list(imageData.values()))
    feats = feats.reshape(-1, 4096)

    # reduce the amount of dimensions in the feature vector
    pca = PCA(n_components=100, random_state=22)
    pca.fit(feats)
    x = pca.transform(feats)

    # cluster feature vectors
    kmeans_kwargs = {"init": "k-means++", "max_iter": 100, "random_state": 22}
    numberOfClusters = findOptimalNumberOfClusters(x, kmeans_kwargs)
    print('Optimal number of clusters: ', numberOfClusters)

    kmeans = KMeans(n_clusters=numberOfClusters, **kmeans_kwargs)
    kmeans.fit(x)
    pickle.dump(kmeans, open("photoModel.pkl", "wb"))
    pickle.dump(pca, open("pca.pkl", "wb"))

    # groups = {}
    # for file, cluster in zip(filenames, kmeans.labels_):
    #     if cluster not in groups.keys():
    #         groups[cluster] = []
    #         groups[cluster].append(file)
    #     else:
    #         groups[cluster].append(file)



