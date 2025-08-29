from sqlalchemy import create_engine
from scrape_sofa import get_sofascore_api_data
from datetime import datetime
import pandas as pd

engine = create_engine('mysql+pymysql://root@localhost/footviz')
df_paises = pd.read_sql('SELECT id_pais, nombre FROM pais', engine)
mapa_paises = dict(zip(df_paises['nombre'], df_paises['id_pais']))

ligas_ids = [17,8,23,35,34]
temporadas_ids = [76986, 77559, 76457, 77333, 77356]

lista_jugadores= []
for ligas in range(0,5):
    print(f"Inicializando scrapeo a api/v1/unique-tournament/{ligas_ids[ligas]}/season/{temporadas_ids[ligas]}/standings/total")
    data_temporada = get_sofascore_api_data(path=f"api/v1/unique-tournament/{ligas_ids[ligas]}/season/{temporadas_ids[ligas]}/standings/total")
    torneo = data_temporada['standings'][0]
    base_url = 'https://www.sofascore.com/team/football/'
    for row in range(len(torneo['rows'])):
        id_equipo = torneo['rows'][row]['team']['id']
        nombre_equipo = torneo['rows'][row]['team']['name']
        print(f"Scrapeando a {nombre_equipo}...")
        data_jugadores = get_sofascore_api_data(path=f"api/v1/team/{id_equipo}/players")
        for player in range(len(data_jugadores['players'])):
            nombre_jugador = data_jugadores['players'][player]['player']['name'] 
            dorsal = data_jugadores['players'][player]['player'].get('jerseyNumber', None)
            dob_timestamp = data_jugadores['players'][player]['player'].get('dateOfBirthTimestamp', None)
            if dob_timestamp is not None:
                fecnac = datetime.utcfromtimestamp(dob_timestamp).strftime('%Y-%m-%d')
            else: 
                fecnac = None
            posicion = data_jugadores['players'][player]['player'].get('position')
            valor_mercado = data_jugadores['players'][player]['player'].get('proposedMarketValue', None)
            altura = data_jugadores['players'][player]['player'].get('height', None)
            pie_preferido = data_jugadores['players'][player]['player'].get('preferredFoot', None)
            id_sofascore = data_jugadores['players'][player]['player']['id']
            base_url_img = 'https://img.sofascore.com/api/v1/player/'
            url_imagen = f"{base_url_img}{id_sofascore}/image"
            pais = data_jugadores['players'][player]['player']['country'].get('name', None)
            diccionario_jugadores = {
                'nombre' : nombre_jugador,
                'dorsal' : dorsal,
                'fec_nac' : fecnac,
                'posicion' : posicion,
                'valor_mercado' : valor_mercado,
                'altura' : altura,
                'pie_preferido' : pie_preferido,
                'url_imagen' : url_imagen,
                'pais nombre' : pais,
                'id_sofascore' : id_sofascore
            }
            lista_jugadores.append(diccionario_jugadores)
    print("Exito")
            

        


#data_temporada = get_sofascore_api_data(path='api/v1/unique-tournament/34/season/77356/standings/total')


df_jugadores = pd.DataFrame(lista_jugadores)

df_jugadores['pais'] = df_jugadores['pais nombre'].map(mapa_paises)
df_jugadores.drop(columns=['pais nombre'], axis=1)
df_jugadores_final = df_jugadores[['nombre', 'dorsal','fec_nac','posicion','valor_mercado','altura','pie_preferido', 'url_imagen', 'pais', 'id_sofascore']]
df_jugadores_final.to_sql('jugador', engine, if_exists='append', index=False)