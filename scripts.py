import itertools
import numpy as np
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
procSize = comm.Get_size()

someDataSize = ['bobas', 'antanas', 'rytis', 'kolis', 'ruonis', 'zypa', 'zyke', 'rycka', 'rojus', 'bobelis', 'edga', 'tadas', 'erikas', 'tomas', 'gedas', 'mode', 'gentas']
comm.scatter(someDataSize, root=0)
print(someDataSize)
for i in range(len(someDataSize)):
    element = someDataSize[i]
    print(element)

    'qwikidata',
    'city_name',
    'category',
    'title',
    'instance_of',
    'pageview_count',
    'registered_contributors_count',
    'anonymous_contributors_count',
    'num_wikipedia_lang_pages',
    'description',
    'subsidiaries',
    'stock_exchanges',
    'industries',
    'parent_organizations',
    'located_next_to',
    'typically_sells',
    'headquarters',
    'website',
    'owner',
    'named_after',
    'postal_code',
    'foundation_date',
    'fax_number',
    'state',
    'employees',
    'total_equity',
    'total_revenue',
    'students_count',
    'net_profit',
    'total_assets',
    'volunteers',
    'published_about_on_BBC',
    'published_about_on_BoardGameGeek',
    'published_about_on_GaultEtMilau',
    'exists_in_GameFAQ_database',
    'grid_reference',
    'street_address',     'freebase_id',     'twitter_id',     'whosOnFirst_id',     'ROR_id',     'nicoNicoPedia_id',     'tiktok_username',
    'topTens_id',
    'downDetector_id',
    'formation_location',
    'significant_event',
    'wmi_code'