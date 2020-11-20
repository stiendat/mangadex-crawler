from clickhouse_driver import Client
import subprocess
import json
from utils.logger import *

db_client = Client(host='localhost')

def query(query):
    """
    Query database
    clickhouse.query(sql_statement) => query result object
    """
    return db_client.execute(query)

def insert_manga_title(title_object):
    info('Inserting manga title to Clickhouse')
    p = subprocess.run(['clickhouse-client', '--query', 'INSERT INTO manga_crawler.manga_title FORMAT JSONEachRow'], input=json.dumps(title_object), encoding='ascii', stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def insert_manga_tag(tag_object):
    info('Inserting manga tag to Clickhouse')
    p = subprocess.run(['clickhouse-client', '--query', 'INSERT INTO manga_crawler.manga_tags FORMAT JSONEachRow'], input=json.dumps(tag_object), encoding='ascii', stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def insert_manga_altTitles(altTitles_object):
    info('Inserting manga title alternative titles to Clickhouse')
    p = subprocess.run(['clickhouse-client', '--query', 'INSERT INTO manga_crawler.manga_altTitles FORMAT JSONEachRow'], input=json.dumps(altTitles_object), encoding='ascii', stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def insert_manga_authors(authors_object):
    info('Inserting manga title authors to Clickhouse')
    p = subprocess.run(['clickhouse-client', '--query', 'INSERT INTO manga_crawler.manga_authors FORMAT JSONEachRow'], input=json.dumps(authors_object), encoding='ascii', stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def insert_manga_artists(artists_object):
    info('Inserting manga title artists to Clickhouse')
    p = subprocess.run(['clickhouse-client', '--query', 'INSERT INTO manga_crawler.manga_artists FORMAT JSONEachRow'], input=json.dumps(artists_object), encoding='ascii', stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def insert_manga_title_tag(tag_object):
    info('Inserting manga title tag to Clickhouse')
    p = subprocess.run(['clickhouse-client', '--query', 'INSERT INTO manga_crawler.manga_tag FORMAT JSONEachRow'], input=json.dumps(tag_object), encoding='ascii', stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def insert_manga_chapters(chapters_object):
    info('Inserting manga chapters to Clickhouse')
    p = subprocess.run(['clickhouse-client', '--query', 'INSERT INTO manga_crawler.manga_chapters FORMAT JSONEachRow'], input=json.dumps(chapters_object), encoding='ascii', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
