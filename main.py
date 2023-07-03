import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from urllib.parse import urljoin

try:
    # Envoyer une requête HTTP à l'URL souhaitée
    response = requests.get('https://fr.wikipedia.org/wiki/France')

    # Vérifier si la requête a réussi
    if response.status_code == 200:
        # Analyser le contenu HTML de la réponse
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extraire les URL de liens et leur texte associé
        link_tags = soup.find_all('a')
        links = []
        for tag in link_tags:
            if 'href' in tag.attrs:
                link_url = urljoin(response.url, tag['href'])
                link_text = tag.get_text().strip()
                links.append({'url': link_url, 'text': link_text})

        # Filtrer les liens en supprimant les doublons
        unique_links = list({link['url']: link for link in links}.values())

        # Stocker les méta-données dans MongoDB
        client = MongoClient('mongodb://localhost:27017')
        db = client['TPscrapiong']
        collection = db['exemple']

        # Extraire les balises de titre de la page
        title_tags = soup.find_all(['title', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'])

        # Extraire les balises d'emphase de la page
        emphasis_tags = soup.find_all(['b', 'strong', 'em'])

        # Récupérer le contenu des balises de titre et d'emphase
        title_content = [tag.get_text().strip() for tag in title_tags]
        emphasis_content = [tag.get_text().strip() for tag in emphasis_tags]

        # Insérer le premier document avec le lien de la page principale
        collection.insert_one({
            'url': 'https://fr.wikipedia.org/wiki/France',
            'text': 'France',
            'title': title_content,
            'emphasis': emphasis_content,
            'links': links
        })

        # Insérer les méta-données dans la collection
        for link in unique_links[:10]:
            try:
                # Envoyer une requête HTTP à l'URL du lien
                page_response = requests.get(link['url'])

                # Vérifier si la requête a réussi
                if page_response.status_code == 200:
                    # Analyser le contenu HTML de la réponse de la page
                    page_soup = BeautifulSoup(page_response.content, 'html.parser')

                    # Extraire les balises de titre de la page
                    title_tags = page_soup.find_all(['title', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'])

                    # Extraire les balises d'emphase de la page
                    emphasis_tags = page_soup.find_all(['b', 'strong', 'em'])

                    # Récupérer le contenu des balises de titre et d'emphase
                    title_content = [tag.get_text().strip() for tag in title_tags]
                    emphasis_content = [tag.get_text().strip() for tag in emphasis_tags]

                    # Insérer les méta-données dans la collection
                    collection.insert_one({
                        'url': link['url'],
                        'text': link['text'],
                        'title': title_content,
                        'emphasis': emphasis_content
                    })
                else:
                    print(f'La requête pour la page {link["url"]} a échoué avec le code de statut : {page_response.status_code}')
            except requests.exceptions.RequestException as e:
                print(f'Erreur lors de la requête pour la page {link["url"]}: {e}')
    else:
        print('La requête a échoué avec le code de statut :', response.status_code)

except requests.exceptions.RequestException as e:
    print('Erreur lors de la requête pour la page principale :', e)
