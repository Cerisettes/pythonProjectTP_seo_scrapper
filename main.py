import sys
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from urllib.parse import urljoin, urldefrag, urlparse
import threading
from queue import Queue
import time

class Scraper:
    def __init__(self, start_urls, max_depth=3, domain_limit=None, directory_prefix=None):
        self.start_urls = start_urls
        self.max_depth = max_depth
        self.domain_limit = domain_limit
        self.directory_prefix = directory_prefix
        self.visited_urls = set()
        self.db_client = MongoClient('mongodb://localhost:27017/')
        self.db = self.db_client['scraping_db']
        self.link_collection = self.db['link']
        self.metadata_collection = self.db['metadata']
        self.pending_link_collection = self.db['pending_link']
        self.journal_collection = self.db['journal']
        self.url_queue = Queue()
        self.url_timestamps = {}
        self.counter = 0

    def scrape_website(self):
        for start_url in self.start_urls:
            self.url_queue.put(start_url)

        # Lancer les threads de scraping
        for _ in range(len(self.start_urls)):
            t = threading.Thread(target=self._scrape_links)
            t.daemon = True
            t.start()

        # Attendre que toutes les URLs soient traitées
        self.url_queue.join()

        print('Web scraping terminé.')

    def _scrape_links(self):
        while True:
            url = self.url_queue.get()

            if url in self.url_timestamps:
                elapsed_time = time.time() - self.url_timestamps[url]
                if elapsed_time < time_threshold:
                    continue

            self.url_timestamps[url] = time.time()

            try:
                response = requests.get(url)
                if self.counter == 10:
                    print('Le nombre maximum de documents a été atteint. Arrêt du web scraping.')
                    sys.exit()

                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')

                    new_links = self._get_url_links(url, soup)

                    unique_new_links = list(set(new_links))

                    # Collection 1: link
                    self._insert_links(url, unique_new_links)

                    # Collection 2: metadata
                    self._insert_metadata(url)

                    # Collection 3: pending_link
                    self._insert_pending_links(url, unique_new_links)

                    # Collection 4: journal
                    self._insert_journal(url)
                    self.counter += 1

                else:
                    print(f'La requête pour la page {url} a échoué avec le code de statut : {response.status_code}')
            except requests.exceptions.RequestException as e:
                print(f'Erreur lors de la requête pour la page {url}: {e}')
            finally:
                self.url_queue.task_done()

    def _get_url_links(self, url, soup):
        link_tags = soup.find_all('a')
        links = []
        for tag in link_tags:
            if 'href' in tag.attrs:
                link_url = urljoin(url, tag['href'])
                link_url = urldefrag(link_url)[0]
                parsed_url = urlparse(url)

                if parsed_url.netloc in link_url:
                    links.append(link_url)
        return links

    def _insert_links(self, url, unique_links):
        documents = []
        for link in unique_links:
            documents.append({
                "_id": url + link + time.strftime('%Y-%m-%d %H:%M:%S'),
                'link': link,
                'status': 'en-cours'
            })
        self.link_collection.insert_many(documents)

    def _insert_metadata(self, url):
        # Insertion des données de la page

        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extraire les balises de titre de la page
        title_tags = soup.find_all(['title', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'])

        # Récupérer le contenu des balises de titre
        title_content = [tag.get_text().strip() for tag in title_tags]

        # Extraire les balises d'emphase de la page
        emphasis_tags = soup.find_all(['b', 'strong', 'em'])

        # Récupérer le contenu des balises d'emphase
        emphasis_content = [tag.get_text().strip() for tag in emphasis_tags]

        document = {
            '_id': url,
            'date': time.strftime('%Y-%m-%d %H:%M:%S'),
            'titles': title_content,
            'emphasis': emphasis_content
        }

        self.metadata_collection.insert_one(document)

    def _insert_pending_links(self, url, unique_links):
        for link in unique_links:
            document = {
                "_id": url + link + time.strftime('%Y-%m-%d %H:%M:%S'),
                'link': link,
                'status': 'en-cours'
            }
            self.pending_link_collection.insert_one(document)
            self.url_queue.put(link)

    def _insert_journal(self, url):
        document = {
            '_id': url + time.strftime('%Y-%m-%d %H:%M:%S'),
            'url': url
        }
        self.journal_collection.insert_one(document)

time_threshold = 300  # 5 minutes

start_urls = [
    'https://fr.wikipedia.org/wiki/France'
]

scraper = Scraper(start_urls, max_depth=3, domain_limit='fr.wikipedia.org', directory_prefix='/')
scraper.scrape_website()
