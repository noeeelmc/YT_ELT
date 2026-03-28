import requests 
import json

import os 
from dotenv import load_dotenv


# Se hace a una petición a la URL asociada al canal buscado. La API_KEY se obtuvo desde Google Cloud
# El canal es el @. Y la URL se extrae de la Referencia de la API de Youtube.

load_dotenv(dotenv_path="./.env")

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "MrBeast"

def get_playlist_id():
    
    try:
        
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)

        # print(response)
        response.raise_for_status()

        data = response.json()

        # print(json.dumps(data, indent=4)) # convierte el objeto a JSON

        # En este momento, se puede copiar el json y crear un fichero .json con ese contenido para observar las relaciones.
        # De ahí se sacaría el ID de la playlist pulsando en el último elemento del JSON crack. Eliminar ese .json.

        channel_items = data["items"][0]
        channel_playlistID = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]

        # print(channel_playlistID)
        return channel_playlistID
    
    except requests.exceptions.RequestException as e:
        raise e

if __name__ == "__main__":
    get_playlist_id()