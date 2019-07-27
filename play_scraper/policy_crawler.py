import json
import play_scraper
from collections import defaultdict
from time import sleep
from lists import CATEGORIES


MAX_NUM_APPS_PER_COLLECTION = 500
APPS_PER_PAGE = 100
MAX_PAGE_NUM = int(MAX_NUM_APPS_PER_COLLECTION / APPS_PER_PAGE)
PAUSE_BETWEEN_QUERIES = 60
PAUSE_BETWEEN_CRAWLS = 60

MAX_TRIES = 5
MAX_BACKOFF = 7200  # two hours


class PlayStoreCrawler(object):

    def __init__(self):
        self.result = defaultdict(list)
        self.result_file = "playstore_output.json"

    def dump_result(self):
        with open(self.result_file, 'w') as f:
            json.dump(self.result, f)

    def download_app_details(self, collection, category, pg_num):
        n_tries = 0
        while n_tries < MAX_TRIES:
            n_tries += 1
            print("Will crawl", collection, category, pg_num, n_tries)
            try:
                res = play_scraper.collection(
                    collection=collection, category=category,
                    results=APPS_PER_PAGE, page=pg_num, detailed=True)
                print(res)
                return res
            except Exception as exc:
                print("Error", n_tries, collection, category, pg_num, exc)
                pause = min(((2**n_tries) * 60 * 5), MAX_BACKOFF)  # 5, 10...
                sleep(pause)
        return None

    def crawl_collection(self, collection, category=None):
        print("Will crawl", collection, category)
        for pg_num in range(MAX_PAGE_NUM):
            res = self.download_app_details(collection, category, pg_num)
            if res is None:
                print("Error: download_app_details", collection, category,
                      pg_num)
            else:
                self.result["%s_%s" % (collection, category)].append(res)
                self.dump_result()
                sleep(PAUSE_BETWEEN_QUERIES)
        sleep(PAUSE_BETWEEN_CRAWLS)

    def crawl_play_store(self):
        COLLECTIONS = ['TOP_FREE', 'TOP_PAID']
        for collection in COLLECTIONS:
            self.crawl_collection(collection)
        for collection in COLLECTIONS:
            for category in CATEGORIES.keys():
                self.crawl_collection(collection, category=category)


if __name__ == '__main__':
    policy_crawler = PlayStoreCrawler()
    policy_crawler.crawl_play_store()
