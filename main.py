import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

# Envoyer une requête HTTP à l'URL souhaitée
response = requests.get('https://fr.wikipedia.org/wiki/France')

# Analyser le contenu HTML de la réponse
soup = BeautifulSoup(response.text, 'html.parser')

# Extraire les balises de titre
title_tags = soup.find_all(['title', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'])

# Extraire les balises d'emphase
emphasis_tags = soup.find_all(['b', 'strong', 'em'])

# Extraire les URL de liens
link_tags = soup.find_all('a')
links = [tag['href'] for tag in link_tags if 'href' in tag.attrs]

# Récupérer le contenu des balises de titre et d'emphase
title_content = [tag.get_text().strip() for tag in title_tags]
emphasis_content = [tag.get_text().strip() for tag in emphasis_tags]

# Stocker les méta-données dans MongoDB
client = MongoClient('mongodb://localhost:27017')
db = client['TPscrapiong']
collection = db['exemple']

# Insérer les méta-données dans la collection
collection.insert_one({
    'url': 'https://fr.wikipedia.org/wiki/France',
    'title': title_content,
    'emphasis': emphasis_content,
    'links': links
})

