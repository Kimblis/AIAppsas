FROM python:3.9
COPY expandObjectData.py filterData.py fullDataset.csv gatherData.py kmeans.py main.py utils.py ./
RUN pip3 install mysql-connector-python pandas mpi4py qwikidata numpy shapely kneed seaborn sklearn selenium
RUN git clone https://github.com/Joeclinton1/google-images-download.git
RUN cd google-images-download
RUN python setup.py install
RUN cd ../
CMD ["python3", "main.py"]