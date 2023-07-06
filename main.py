import sys
import requests
from bs4 import BeautifulSoup
import pymongo
from urllib.parse import urljoin, urldefrag, urlparse
import time


class Scraper:
    def __init__(self, start_urls, max_depth=3, domain_limit=None, directory_prefix=None):
        self.start_urls = start_urls
        self.max_depth = max_depth
        self.domain_limit = domain_limit
        self.directory_prefix = directory_prefix
        self.visited_urls = set()
        self.db_client = pymongo.MongoClient('mongodb://localhost:27017/')
        self.db = self.db_client['scraping_db']
        self.link_collection = self.db['link']
        self.metadata_collection = self.db['content']
        self.journal_collection = self.db['journal']
        self.url_timestamps = {}

    def scrape_website(self):
        for start_url in self.start_urls:
            self._scrape_link(start_url)
        # tant qu'il reste des liens en attentes et < 10 documents'
        while self.metadata_collection.count_documents({}) < 10 and self.link_collection.find({'status': 'a traiter'}):
            # récupère le lien à scraper
            doc = self.link_collection.find_one({"status": 'a traiter'})
            link = doc['_id']

            # # si le lien est en entrain d'être scrapper mais que c'est trop long alors on relance (la machine qui l'a traité a peut-être planté ou autre erreur)
            # if link in self.url_timestamps:
            #     elapsed_time = time.time() - self.url_timestamps[link]
            #     if elapsed_time < time_threshold:
            #         continue

            # self.url_timestamps[link] = time.time()
            # print(self.url_timestamps[link])
            print(link)
            # modifie le status en "en-cours"
            self.link_collection.find_one_and_update({'_id': link}, {"$set": {'status': 'en-cours'}})
            # web scraping de la page donnée
            self._scrape_link(link)
            # modifie le status en "fini"
            self.link_collection.find_one_and_update({'_id': link}, {"$set": {'status': 'fini'}})

        print('Web scraping terminé.')

    def _scrape_link(self, url):
        try:
            print("get_url", url)
            response = requests.get(url)

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                new_links = self._get_url_links(url, soup)

                unique_new_links = sorted(set(new_links))

                # Collection 1: link
                self._insert_links(unique_new_links)

                # Collection 2: metadata
                self._insert_metadata(url, soup)

                # Collection 3: journal
                self._insert_journal(url)

            else:
                print(f'La requête pour la page {url} a échoué avec le code de statut : {response.status_code}')
        except requests.exceptions.RequestException as e:
            print(f'Erreur lors de la requête pour la page {url}: {e}')

    def _get_url_links(self, url, soup):
        link_tags = soup.find_all('a')
        links = []
        for tag in link_tags:
            if 'href' in tag.attrs:
                link_url = urljoin(url, tag['href'])
                link_url = urldefrag(link_url)[0]
                parsed_url = urlparse(link_url)

                if parsed_url.netloc == self.domain_limit:
                    links.append(link_url)
        return links

    def _insert_links(self, unique_links):
        for link in unique_links:
            try:
                self.link_collection.insert_one({
                    "_id": link,
                    'status': 'a traiter'
                })
            except pymongo.errors.DuplicateKeyError:
                pass

    def _insert_metadata(self, url, soup):
        try:
            # Insertion des données de la page
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
                'html': str(soup),
                'titles': title_content,
                'emphasis': emphasis_content
            }
            self.metadata_collection.insert_one(document)
        except pymongo.errors.DuplicateKeyError:
            pass


    def _insert_journal(self, url):
        document = {
            '_id': url,
        }
        self.journal_collection.insert_one(document)


time_threshold = 300  # 5 minutes

start_urls = ['https://fr.wikipedia.org/wiki/France']

scraper = Scraper(start_urls, max_depth=3, domain_limit='fr.wikipedia.org', directory_prefix='/')
scraper.scrape_website()
