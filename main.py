import requests
import json
from utils.clickhouse import insert_manga_title
from utils.logger import *
from datetime import datetime
import time
from crawler.crawl import *
from config import *
from multiprocessing import Process

if __name__ == "__main__":
    try:
        printWelcomeMessage()
        
        title_process = Process(target=main_crawler_title)
        tag_process = Process(target=main_crawler_tags)
        chapters_process = Process(target=main_crawler_chapters)

        process_list = [title_process, tag_process, chapters_process]

        for proc in process_list:
            debug('Starting process: {}'.format(str(proc)))
            proc.start()
        
    except KeyboardInterrupt:
        info('Detect Ctrl C. Commiting suicide...')
        for proc in process_list:
            proc.join()