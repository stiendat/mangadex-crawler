import requests
import json
from utils.clickhouse import *
from utils.logger import *
from datetime import datetime
import time
from config import API_BASE_URL, MAX_MANGA_TITLE
import threading

data_type = {
    'title': [    'id' , 'title' ,'description' ,'publication_language' ,'publication_status' ,'publication_demographic' ,'isHentai','links_al' ,'links_ap' ,'links_kt' ,'links_mu','links_mal' ,'links_raw' ,'links_engtl' ,'rating_bayesian' ,'rating_mean' ,'rating_users' ,'views' ,'follows' ,'comments' ,'lastUploaded' ,'mainCover' 
],
    'authors': ['id' , 'manga_author'],
    'tags': ['id' , 'name' ,'group' ,'description'],
    'artists': ['id' , 'manga_artist'],
    'altTitles': ['id' , 'altTitle'],
    'chapters': ['id' , 'hash' ,'mangaId' ,'mangaTitle' ,'volume' ,'chapter' ,'title' ,'language' ,'uploader' ,'timestamp' ,'comments' ,'views']
}
bool_type = {
    True: 1,
    False: 0
}

def flatten_json(j_object):
    res = dict()
    def flat(obj, name):
        if type(obj) is dict:
            for a in obj:
                flat(obj[a], name + '_' + a)
        elif type(obj) is list:
            # res[name] = str(obj)
            res[name] = obj
        else:
            res[name] = obj
    for i in j_object:
        flat(j_object[i], i)
    return res

def normalize_data_for_clickhouse(object_type, dict_object):
    list_columns_for_db = data_type.get(object_type, None)
    res = dict()
    res['insertDate'] = str(datetime.now())[:-7]
    for item in list_columns_for_db:
        try:
            res[item] = dict_object[item]
        except Exception as err:
            warn('Missing data: {}'.format(item))
    return res

def crawl_data(GET_URI, id):
    ENDPOINT = API_BASE_URL + GET_URI
    try:
        res = requests.get(ENDPOINT + str(id))
        debug('Calling endpoint {} return {}'.format(ENDPOINT + str(id), res.status_code))
        if res.status_code == 200:
            res = json.loads(res.text)
            return res['data']
        elif res.status_code == 404:
            warn('Met 404. RIP')
            return 404
        else: 
            debug('Bad call. Get {}'.format(res.status_code))
            return None
    except Exception as err:
        debug('Something wrong: {}'.format(err))
        return None

def index_altTitles(id, altTitles_list):
    res = list()
    debug('Building altTitles list for manga id {}'.format(id))
    for item in altTitles_list:
        res_item = dict()
        res_item['insertDate'] = str(datetime.now())[:-7]
        res_item['id'] = str(id)
        res_item['altTitle'] = item
        res.append(res_item)
    insert_manga_altTitles(res)

def index_authors(id, authors_list):
    res = list()
    debug('Building authors list for manga id {}'.format(str(id)))
    for item in authors_list:
        res_item = dict()
        res_item['insertDate'] = str(datetime.now())[:-7]
        res_item['id'] = str(id)
        res_item['manga_author'] = item
        res.append(res_item)
    insert_manga_authors(res)

def index_artists(id, artists_list):
    res = list()
    debug('Building artists list for manga id {}'.format(str(id)))
    for item in artists_list:
        res_item = dict()
        res_item['insertDate'] = str(datetime.now())[:-7]
        res_item['id'] = str(id)
        res_item['manga_artist'] = item
        res.append(res_item)
    insert_manga_artists(res)

def index_title_tag(id, title_tag_list):
    res = list()
    debug('Building artists list for manga id {}'.format(str(id)))
    for item in title_tag_list:
        res_item = dict()
        res_item['insertDate'] = str(datetime.now())[:-7]
        res_item['id'] = str(id)
        res_item['manga_tag'] = item
        res.append(res_item)
    insert_manga_title_tag(res)
    
def main_crawler_title():
    info('Init crawler for: manga_title')
    manga_id = 1
    manga_limit = MAX_MANGA_TITLE - 1
    while manga_id < MAX_MANGA_TITLE:
        info('Running crawler on id: {}'.format(manga_id))
        manga_title = crawl_data('/manga/', manga_id)
        if manga_title == 404:
            error('Let the crawler rest for 2 secs')
            time.sleep(2)
            manga_id += 1
        elif manga_title is None:
            error('Something wrong with the crawler, skipping ...')
            time.sleep(2)
            manga_id += 1
        else:
            manga_title = flatten_json(manga_title)
            debug('Starting altTitles thread')
            altTitles_thread = threading.Thread(target=index_altTitles, args=(manga_title['id'], manga_title['altTitles']))
            altTitles_thread.start()
            debug('Starting authors thread')
            authors_thread = threading.Thread(target=index_authors, args=(manga_title['id'], manga_title['author']))
            authors_thread.start()
            debug('Starting artits thread')
            artists_thread = threading.Thread(target=index_artists, args=(manga_title['id'], manga_title['artist']))
            artists_thread.start()
            debug('Starting title tag thread')
            artists_thread = threading.Thread(target=index_title_tag, args=(manga_title['id'], manga_title['tags']))
            artists_thread.start()
            manga_title = normalize_data_for_clickhouse('title', manga_title)
            info('Found manga title: {}'.format(manga_title['title']))
            insert_manga_title(manga_title)
            manga_id += 1
            time.sleep(0.4)
            info('Successfully crawl {}, i guess ?'.format(manga_title['title']))
        if manga_id == manga_limit:
            info('Finish crawling {} mangas. Restarting ...'.format(MAX_MANGA_TITLE))

def main_crawler_tags():
    info('Init crawler for: manga_tags')
    tag_id = 1
    while True:
        manga_tag = crawl_data('/tag/', tag_id)
        if tag_id == 100:
            info('Finished crawling tags. Sleeping...')
            time.sleep(86400)
            tag_id = 1
        elif manga_tag == 404:
            info('Found 404 on ya fav tag. No furry for ya')
            time.sleep(2)
            tag_id += 1
        else:
            info('Crawl manga tag: {}'.format(manga_tag['name']))
            manga_tag['insertDate'] = str(datetime.now())[:-7]
            insert_manga_tag(manga_tag)
            tag_id += 1
            time.sleep(0.4)
            info('Successfully crawl tag {}, i guess ?'.format(manga_tag['name']))

def main_crawler_chapters():
    info('Init crawler for: manga_chapters')
    manga_id = 1
    manga_limit = MAX_MANGA_TITLE - 1
    while manga_id < MAX_MANGA_TITLE:
        manga_chapters = crawl_data('/manga/{}/chapters/'.format(manga_id), 0)
        if manga_chapters == 404:
            info('404 no chapters for your fav mango id {}. ZZZ'.format(manga_id))
            time.sleep(2)
            manga_id += 1
        elif manga_chapters is None:
            warn('Something wrong when pulling chapters data. Skipping ...')
            time.sleep(2)
            manga_id += 1
        else:
            manga_chapters = manga_chapters['chapters']
            info('Found {} chapters for manga id {}.'.format(len(manga_chapters), manga_id))
            debug('Building chapters list for manga id {}'.format(manga_id))
            chapter_list = list()
            for chapter in manga_chapters:
                chapter = normalize_data_for_clickhouse('chapters', chapter)
                chapter_list.append(chapter)
            insert_manga_chapters(chapter_list)
            manga_id += 1
            time.sleep(0.4)
            info('Successfully crawl chapters of manga id {}, i guess ?'.format(manga_id))
        if manga_id == manga_limit:
            info('Finish crawling {} manga chapters. Restarting ...'.format(MAX_MANGA_TITLE))