from qwikidata.linked_data_interface import get_entity_dict_from_api
import requests
from urllib.parse import urlparse
from utils import getPlaceInformation, getPlacePhotos
import os

def expandObjectData(data):
    instance_of = []
    subsidiaries = []
    stock_exchanges = []
    industries = []
    parent_organizations = []
    located_next_to = []
    typicallySells = []
    title = 'None'
    headquarters = 'None'
    website = 'None'
    owner = 'None'
    namedAfter = 'None'
    postalCode = 'None'
    foundationDate = 'None'
    faxNumber = 'None'
    state = 'None'
    gridReference = 'None'
    streetAddress = 'None'
    freebaseId = 'None'
    twitterId = 'None'
    whosOnFirstId = 'None'
    RORId = 'None'
    wmiCode = 'None'
    nicoNicoPediaId = 'None'
    tiktokUsername = 'None'
    topTensId = 'None'
    downDetectorId = 'None'
    formationLocation = 'None'
    significantEvent = 'None'
    description = 'None'
    publishedAboutOnBoardGameGeek = False
    publishedAboutOnBBC = False
    existsInGameFAQDatabase = False
    gotAward = False
    publishedOnGaultEtMilau = False
    employees = 0
    totalEquity = 0
    totalRevenue = 0
    studentsCount = 0
    netProfit = 0
    totalAssets = 0
    volunteers = 0

    if 'P31' in data["claims"]:
        for p31 in data["claims"]['P31']:
            try:
                instance_of_qid = p31['mainsnak']['datavalue']['value']['id']
                instance_dict = get_entity_dict_from_api(instance_of_qid)
                instance_value = instance_dict['labels']['en']['value']
                instance_of.append(instance_value)
            except:
                instance_of = 'None'
    else:
        instance_of = 'None'

    if 'P355' in data["claims"]:
        for p355 in data["claims"]['P355']:
            try:
                subsidiary_qid = p355['mainsnak']['datavalue']['value']['id']
                instance_dict = get_entity_dict_from_api(subsidiary_qid)
                instance_value = instance_dict['labels']['en']['value']
                subsidiaries.append(instance_value)
            except:
                subsidiaries = 'None'
    else:
        subsidiaries = 'None'

    if 'P7163' in data["claims"]:
        for p7163 in data["claims"]['P7163']:
            try:
                typicallySells_id = p7163['mainsnak']['datavalue']['value']['id']
                instance_dict = get_entity_dict_from_api(typicallySells_id)
                instance_value = instance_dict['labels']['en']['value']
                typicallySells.append(instance_value)
            except:
                typicallySells = 'None'
    else:
        typicallySells = 'None'

    if 'P414' in data["claims"]:
        for p414 in data["claims"]['P414']:
            try:
                stockExchange_qid = p414['mainsnak']['datavalue']['value']['id']
                instance_dict = get_entity_dict_from_api(stockExchange_qid)
                instance_value = instance_dict['labels']['en']['value']
                stock_exchanges.append(instance_value)
            except:
                stock_exchanges = 'None'
    else:
        stock_exchanges = 'None'

    if 'P749' in data["claims"]:
        for p749 in data["claims"]['P749']:
            try:
                parent_qid = p749['mainsnak']['datavalue']['value']['id']
                instance_dict = get_entity_dict_from_api(parent_qid)
                instance_value = instance_dict['labels']['en']['value']
                parent_organizations.append(instance_value)
            except:
                parent_organizations = 'None'
    else:
        parent_organizations = 'None'

    if 'P452' in data["claims"]:
        for p452 in data["claims"]['P452']:
            try:
                industry_qid = p452['mainsnak']['datavalue']['value']['id']
                instance_dict = get_entity_dict_from_api(industry_qid)
                instance_value = instance_dict['labels']['en']['value']
                industries.append(instance_value)
            except:
                industries = 'None'
    else:
        industries = 'None'

    if 'P3032' in data["claims"]:
        for p3032 in data["claims"]['P3032']:
            try:
                neighbour_qid = p3032['mainsnak']['datavalue']['value']['id']
                instance_dict = get_entity_dict_from_api(neighbour_qid)
                instance_value = instance_dict['labels']['en']['value']
                located_next_to.append(instance_value)
            except:
                located_next_to = 'None'
    else:
        located_next_to = 'None'

    if 'P5817' in data["claims"]:
        try:
            state_id = data["claims"]['P5817'][0]['mainsnak']['datavalue']['value']['id']
            instance_dict = get_entity_dict_from_api(state_id)
            state = instance_dict['labels']['en']['value']
        except:
            None
    if 'P159' in data["claims"]:
        try:
            headquarters_qid = data["claims"]['P159'][0]['mainsnak']['datavalue']['value']['id']
            instance_dict = get_entity_dict_from_api(headquarters_qid)
            headquarters = instance_dict['labels']['en']['value']
        except:
            None
    if 'P740' in data["claims"]:
        try:
            formation_qid = data["claims"]['P740'][0]['mainsnak']['datavalue']['value']['id']
            instance_dict = get_entity_dict_from_api(formation_qid)
            formationLocation = instance_dict['labels']['en']['value']
        except:
            None
    if 'P793' in data["claims"]:
        try:
            event_id = data["claims"]['P793'][0]['mainsnak']['datavalue']['value']['id']
            instance_dict = get_entity_dict_from_api(event_id)
            significantEvent = instance_dict['labels']['en']['value']
        except:
            None
    if 'P856' in data["claims"]:
        try:
            website = data["claims"]['P856'][0]['mainsnak']['datavalue']['value']
        except:
            None
    if 'P6552' in data["claims"]:
        try:
            twitterId = data["claims"]['P6552'][0]['mainsnak']['datavalue']['value']
        except:
            None
    if 'P6766' in data["claims"]:
        try:
            whosOnFirstId = data["claims"]['P6766'][0]['mainsnak']['datavalue']['value']
        except:
            None
    if 'P646' in data["claims"]:
        try:
            freebaseId = data["claims"]['P646'][0]['mainsnak']['datavalue']['value']
        except:
            None
    if 'P6782' in data["claims"]:
        try:
            RORId = data["claims"]['P6782'][0]['mainsnak']['datavalue']['value']
        except:
            None
    if 'P6793' in data["claims"]:
        try:
            wmiCode = data["claims"]['P6793'][0]['mainsnak']['datavalue']['value']
        except:
            None
    if 'P6900' in data["claims"]:
        try:
            nicoNicoPediaId = data["claims"]['P6900'][0]['mainsnak']['datavalue']['value']
        except:
            None
    if 'P7085' in data["claims"]:
        try:
            tiktokUsername = data["claims"]['P7085'][0]['mainsnak']['datavalue']['value']
        except:
            None
    if 'P7157' in data["claims"]:
        try:
            topTensId = data["claims"]['P7157'][0]['mainsnak']['datavalue']['value']
        except:
            None
    if 'P7306' in data["claims"]:
        try:
            downDetectorId = data["claims"]['P7306'][0]['mainsnak']['datavalue']['value']
        except:
            None
    if 'P6160' in data["claims"]:
        publishedAboutOnBoardGameGeek = True
    if 'P6182' in data["claims"]:
        existsInGameFAQDatabase = True
    if '6200' in data["claims"]:
        publishedAboutOnBBC = True
    if 'P6402' in data["claims"]:
        publishedOnGaultEtMilau = True
    if 'P6208' in data["claims"]:
        gotAward = True
    if 'P6375' in data["claims"]:
        try:
            streetAddress = data["claims"]['P6375'][0]['mainsnak']['datavalue']['value']['text']
        except:
            None
    if 'P1128' in data["claims"]:
        try:
            employees = data["claims"]['P1128'][0]['mainsnak']['datavalue']['value']['amount']
        except:
            None
    if 'P571' in data["claims"]:
        try:
            foundationDate = data["claims"]['P571'][0]['mainsnak']['datavalue']['value']['time']
        except:
            None
    if 'P281' in data["claims"]:
        try:
            postalCode = data["claims"]['P281'][0]['mainsnak']['datavalue']['value']
        except:
            None
    if 'P2900' in data["claims"]:
        try:
            faxNumber = data["claims"]['P2900'][0]['mainsnak']['datavalue']['value']
        except:
            None
    if 'P613' in data["claims"]:
        try:
            gridReference = data["claims"]['P613'][0]['mainsnak']['datavalue']['value']
        except:
            None
    if 'P2137' in data["claims"]:
        try:
            totalEquity = data["claims"]['P2137'][0]['mainsnak']['datavalue']['value']['amount']
        except:
            None
    if 'P6125' in data["claims"]:
        try:
            volunteers = data["claims"]['P6125'][0]['mainsnak']['datavalue']['value']['amount']
        except:
            None
    if 'P2139' in data["claims"]:
        try:
            totalRevenue = data["claims"]['P2139'][0]['mainsnak']['datavalue']['value']['amount']
        except:
            None
    if 'P2196' in data["claims"]:
        try:
            studentsCount = data["claims"]['P2196'][0]['mainsnak']['datavalue']['value']['amount']
        except:
            None
    if 'P2295' in data["claims"]:
        try:
            netProfit = data["claims"]['P2295'][0]['mainsnak']['datavalue']['value']['amount']
        except:
            None
    if 'P2403' in data["claims"]:
        try:
            totalAssets = data["claims"]['P2403'][0]['mainsnak']['datavalue']['value']['amount']
        except:
            None
    if 'P127' in data["claims"]:
        try:
            owner_qid = data["claims"]['P127'][0]['mainsnak']['datavalue']['value']['id']
            instance_dict = get_entity_dict_from_api(owner_qid)
            owner = instance_dict['labels']['en']['value']
        except:
            None
    if 'P138' in data["claims"]:
        try:
            named_after_qid = data["claims"]['P138'][0]['mainsnak']['datavalue']['value']['id']
            instance_dict = get_entity_dict_from_api(named_after_qid)
            namedAfter = instance_dict['labels']['en']['value']
        except:
            None

    try:
        title = data['labels']['en']['value']
    except:
        None

    pageview_count = 0
    registered_contributors_count = 0
    anonymous_contributors_count = 0
    num_wikipedia_lang_pages = 0
    for sitelink_key in data['sitelinks'].keys():
        num_wikipedia_lang_pages += 1
        sitelink_url = data['sitelinks'][sitelink_key]['url']
        wiki_subdomain = urlparse(sitelink_url).hostname.split('.')[0]
        wiki_url_title = sitelink_url.split('/')[-1]

        # count number of this year's pageviews (by real human users)
        response = requests.get(
            f'https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/{wiki_subdomain}.wikipedia/all-access/user/{wiki_url_title}/daily/2022010100/2022040800',
            headers={'User-agent': 'Mozilla/5.0'})
        response_data = response.json()
        try:
            for item in response_data['items']:
                pageview_count += item['views']
        except:
            None

        response = requests.get(
            f'https://{wiki_subdomain}.wikipedia.org/w/api.php?action=query&prop=contributors&format=json&titles={wiki_url_title}')
        response_data = response.json()
        try:
            registered_contributors_count += len(
                response_data['query']['pages'][list(response_data['query']['pages'].keys())[0]]['contributors'])
        except:
            None
        try:
            anonymous_contributors_count += \
                response_data['query']['pages'][list(response_data['query']['pages'].keys())[0]]['anoncontributors']
        except:
            None
    if 'P18' in data["claims"]:
        target_folder = os.path.join('./images', '_'.join(title.lower().split(' ')))
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)

        for index, p18 in enumerate(data["claims"]['P18']):
            try:
                photoFile = p18['mainsnak']['datavalue']['value']
                image_api_url = f'https://api.wikimedia.org/core/v1/commons/file/File:{photoFile}'
                response = requests.get(image_api_url, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'}, timeout=10);
                response_data = response.json()
                image_url = response_data['original']['url']
                img_data = requests.get(image_url, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'}, timeout=10).content
                image_extension = image_api_url.split('.')[-1]

                with open(os.path.join(target_folder, image_extension + "_" + str(index+1) + "." + {image_extension}), 'wb') as handler:
                    handler.write(img_data)
            except:
                None

    if title != 'None':
        getPlacePhotos(title)
        description = getPlaceInformation(title)

    if isinstance(instance_of, list): instance_of = ','.join(instance_of)
    if isinstance(subsidiaries, list): subsidiaries = ','.join(subsidiaries)
    if isinstance(stock_exchanges,list): stock_exchanges = ','.join(stock_exchanges)
    if isinstance(industries, list): industries = ','.join(industries)
    if isinstance(parent_organizations,list): parent_organizations = ','.join(parent_organizations)
    if isinstance(located_next_to,list): located_next_to = ','.join(located_next_to)
    if isinstance(typicallySells,list): typicallySells = ','.join(typicallySells)

    return [title, instance_of, pageview_count, registered_contributors_count, anonymous_contributors_count,
            num_wikipedia_lang_pages, description, subsidiaries, stock_exchanges, industries, parent_organizations,
            located_next_to, typicallySells, headquarters, website, owner, namedAfter, postalCode, foundationDate,
            faxNumber, state,
            employees, totalEquity, totalRevenue, studentsCount, netProfit, totalAssets, volunteers,
            publishedAboutOnBBC, publishedAboutOnBoardGameGeek, publishedOnGaultEtMilau, gotAward,
            existsInGameFAQDatabase, gridReference, streetAddress, freebaseId, twitterId, whosOnFirstId, RORId,
            nicoNicoPediaId, tiktokUsername, topTensId, downDetectorId, formationLocation, significantEvent, wmiCode]
