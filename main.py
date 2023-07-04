import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from urllib.parse import urljoin, urldefrag
import time


# Nombre maximum de tentatives
max_attempts = 10

# Temps d'attente en secondes entre chaque tentative
retry_interval = 60


def get_url_text(url, soupe):
    # Extraire les URL de liens et leur texte associé
    link_tags = soupe.find_all('a')
    links = []
    for tag in link_tags:
        if 'href' in tag.attrs:
            # Enlève le fragment des url (ce qui se trouve après le #)
            link_url = urljoin(url, tag['href'])
            link_url = urldefrag(link_url)[0]
            link_text = tag.get_text().strip()

            if 'fr.wikipedia.org' in link_url:
                links.append({'url': link_url, 'text': link_text})
    return links


def titles(soupe):
    # Extraire les balises de titre de la page
    title_tags = soupe.find_all(['title', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'])

    # Récupérer le contenu des balises de titre
    title_content = [tag.get_text().strip() for tag in title_tags]

    return title_content


def emphasis(soupe):
    # Extraire les balises d'emphase de la page
    emphasis_tags = soupe.find_all(['b', 'strong', 'em'])

    # Récupérer le contenu des balises d'emphase
    emphasis_content = [tag.get_text().strip() for tag in emphasis_tags]

    return emphasis_content

try:
    # Envoyer une requête HTTP à l'URL souhaitée
    response = requests.get('https://fr.wikipedia.org/wiki/France')

    # Vérifier si la requête a réussi
    if response.status_code == 200:
        # Analyser le contenu HTML de la réponse
        soup = BeautifulSoup(response.content, 'html.parser')

        links = get_url_text(response.url, soup)

        # Stocker les méta-données dans MongoDB
        client = MongoClient('mongodb://localhost:27017')
        db = client['TPscraping']
        collection = db['Pages_web']

        # Filtrer les liens en supprimant les doublons
        unique_links = list({link['url']: link for link in links}.values())

        # Liste pour stocker tous les liens générés par les nouvelles pages
        all_generated_links = set(link['url'] for link in unique_links)

        # Insérer les méta-données dans la collection
        for link in unique_links[:10]:
            # Boucle pour les tentatives de récupération
            for attempt in range(1, max_attempts + 1):

                try:
                    # Envoyer une requête HTTP à l'URL du lien
                    page_response = requests.get(link['url'])

                    # Vérifier si la requête a réussi
                    if page_response.status_code == 200:
                        # Analyser le contenu HTML de la réponse de la page
                        page_soup = BeautifulSoup(page_response.content, 'html.parser')

                        title_content = titles(page_soup)

                        emphasis_content = emphasis(page_soup)

                        new_links = get_url_text(link['url'], page_soup)

                        # Filtrer les nouveaux liens en supprimant les doublons
                        unique_new_links = [link for link in new_links if link['url'] not in all_generated_links]

                        # Mettre à jour la liste des liens générés par les nouvelles pages
                        all_generated_links.update(link['url'] for link in unique_new_links)

                        collection.insert_one({
                            'url': link['url'],
                            'text': link['text'],
                            'title': title_content,
                            'emphasis': emphasis_content,
                            'link': unique_new_links
                        })
                        # Mettre à jour le champ 'link' du document avec tous les liens
                        collection.update_one({'url': link['url']}, {'$set': {'link': unique_new_links}})

                        # Sortir de la boucle si la récupération réussit
                        break

                    else:
                        print(f'La requête pour la page {link["url"]} a '
                              f'échoué avec le code de statut : {page_response.status_code}')
                except requests.exceptions.RequestException as e:
                    print(f'Erreur lors de la requête pour la page {link["url"]}: {e}')
                # Attendre le délai spécifié avant la prochaine tentative
                time.sleep(retry_interval)

    else:
        print('La requête a échoué avec le code de statut :', response.status_code)

except requests.exceptions.RequestException as e:
    print('Erreur lors de la requête pour la page principale :', e)
