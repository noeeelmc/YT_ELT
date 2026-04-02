import requests 
import json

import os 
from dotenv import load_dotenv
from datetime import date


# Se hace a una petición a la URL asociada al canal buscado. La API_KEY se obtuvo desde Google Cloud
# El canal es el @. Y la URL se extrae de la Referencia de la API de Youtube.

load_dotenv(dotenv_path="./.env")

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "IbaiLlanos"

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


# Una vez se dispone del identificador de la Playlist, hay que acceder a los elementos de ella (playlistitems en la referencia a la API).

maxResults = 50 # número máximo de elementos que se devuelven por 'set'.

def get_video_ids(playlistId):
    
    '''Esta función recorre los diferentes elementos de la playlist, obteniendo los IDS de los vídeos. Una vez 
    obtiene los vídeos de una página, pasa a la siguiente, así hasta obtener todos los vídeos del canal'''
    
    video_ids = []
    
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlistId}&key={API_KEY}"
    
    pageToken = None
    
    try:
    
        while True:
            
            url = base_url
            
            if pageToken: 
                url += f"&pageToken={pageToken}"
            
            response = requests.get(url)

            response.raise_for_status()

            data = response.json()
            
            for item in data.get("items", []):
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)
            
            pageToken = data.get("nextPageToken")
            
            if not pageToken:
                break 
            
        return video_ids
    
    except requests.exceptions.RequestException as e:
            raise e
        

def extract_video_data(video_ids):
    '''Función que facilita los datos de los diferentes vídeos del canal'''
    extracted_data = [] 
    
    def batch_list(video_id_lst, batch_size):
        '''Función que accede a conjuntos de una cntidad de IDs de vídeos especificada en el batch_size'''
        for video_id in range(0, len(video_id_lst), batch_size):
            yield video_id_lst[video_id: video_id + batch_size] 
            
    try: 
        for batch in batch_list(video_ids, maxResults): # se obtienen los resultados de la consulta a los datos de los diferentes batches
            video_ids_str = ",".join(batch)
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"
            
            response = requests.get(url)
 
            response.raise_for_status()

            data = response.json()
            
            for item in data.get("items", []): # se obtienen las variables de la consulta que se quieren tener en cuenta (desde la referencia a la API de Youtube, consulta en vídeos)
                video_id = item["id"]
                snippet = item["snippet"]
                contentDetails = item["contentDetails"]
                statistics = item["statistics"]
                
                video_data = {
                    "video": video_id, 
                    "publishedAt": snippet["publishedAt"],
                    "title": snippet["title"],
                    "duration": contentDetails["duration"],
                    "viewCount": statistics.get("viewCount", None),
                    "likeCount": statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None)
                }
                
                extracted_data.append(video_data)
                
        return extracted_data
            
    except requests.exceptions.RequestException as e:
        raise e
    
def save_to_json(extracted_data):
    '''Esta función permite guardar los datos obtenidos en .json correspondientes al día de ejecución, con los datos de los vídeos disponibled hasta ese momento'''
    file_path = f"./data/YT_data_{date.today()}"
    
    with open(file_path, "w", encoding="utf-8") as json_outfile:  # la w indica que es escritura
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False) # convierte los datos a json y los guarda en el fichero

if __name__ == "__main__":
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)