import os
from sqlalchemy import create_engine, MetaData, Table
from dotenv import load_dotenv
from sqlalchemy.dialects.mysql import insert
from scrape_sofa import get_sofascore_api_data, init_driver, get_all_urls_fbref
from chat import  get_match_report_links_selenium
from datetime import datetime, timedelta
from funciones import liga_fbref_fixtures, ligas_ids, temporadas_ids, insert_tabla_posiciones, insert_update_partidos, insert_estadistica_partido, insert_update_plantilla_equipos, insert_mapa_de_calor, insert_mapa_de_disparos, insert_estadistica_jugador, update_standings_evolution_graph, ultimos_partidos, update_fecha_partidos, prematch_odds, postmatch_odss, pending_odds, insert_confirmed_lineups, prematch_ref, insert_predicted_lineups, extract_img_url, transfer_to_database, procesar_puntos_partido
import pandas as pd

load_dotenv()

# --- 2. CONSTRUIR LA URL DE LA BASE DE DATOS DE FORMA SEGURA ---
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')

db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
today = datetime.now()
yesterday = today - timedelta(days=1)
yesterday = yesterday.strftime('%Y-%m-%d')
engine = create_engine(db_url)

future = today + timedelta(days=1) 
future = future.strftime('%Y-%m-%d')
print(f"Obteniendo data de {yesterday}")
#selecciona los partidos que se jugaron el dia de ayer
df_partidos_ayer = pd.read_sql(f"SELECT * FROM partido WHERE fecha = '{yesterday}' ", engine)
df_partidos_futuros = pd.read_sql(f"SELECT * FROM partido WHERE fecha = '{future}'", engine)
df_partidos_pendientes = pd.read_sql(f"SELECT * FROM partido WHERE fecha < '{future}'", engine)
print("Inicializando driver...")
driver = init_driver()

    
liga_fbref = [
    '9/Premier-League',
    '12/La-Liga',
    '11/Serie-A',
    '20/Bundesliga',
    '13/Ligue-1'
]

ids_para_scrapear_temporada = df_partidos_futuros['temporada'].unique() 
#insert_estadistica_jugador(engine,  ids_para_scrapear_temporada, db_url)
#insert_update_plantilla_equipos(engine, driver, ligas_ids, temporadas_ids, db_url)
#print("Todo las plantillas estan actualizadas")


#df_premier = pd.read_sql(f"SELECT * FROM partido where id_partido > 1446 and id_partido < 1519", engine)
#for j in range(len(df_premier)):
    #id_partido_actual = df_premier.iloc[j]['id_partido']
    #print(f"--- Empezando procesamiento del partido: {id_partido_actual} ---")
    
    #try:
        # Llama a tu nueva función optimizada
        #procesar_puntos_partido(engine, id_partido_actual)
        
    #except Exception as e:
        #print(f"*** ERROR procesando partido {id_partido_actual}: {e} ***")
        # Continúa con el siguiente partido
        #continue

#print("--- Procesamiento de todos los partidos terminado ---")

#if len(df_partidos_futuros) > 0:
    #print("Entrando...")
    #for i in range(len(df_partidos_futuros)):
        #prematch_ref(engine, df_partidos_futuros.iloc[i] )
        #prematch_odds(engine, df_partidos_futuros.iloc[i])
        #insert_predicted_lineups(engine, df_partidos_futuros.iloc[i])

#verifica que si haya partidos el dia de ayer
if len(df_partidos_ayer) == 0:
    print("Hoy no hay nada que hacer mi loco")
    driver.quit()
else:
    lista_ids_sofascore = []  #guarda los ids de los partidos(sofascore) que se jugaron ayer en una lista     
    #lista_ids_bd = []
    #partidos = df_partidos_ayer['id_partido']['temporada']
    
    #links_totales = []
    #for i in range(len(ids_para_scrapear_temporada)):
        #url = f"https://fbref.com/en/comps/{liga_fbref_fixtures[iteracion]}"
        #lista_links = get_all_urls_fbref(driver, liga_url=url)
        #links_totales.extend(lista_links)
    #for i in range(len(df_partidos_ayer)):
        #print(f"Entrando a partido: {df_partidos_ayer['id_partido'][i]}")
        #partido_sofascore = df_partidos_ayer['url_sofascore'][i].split(':')[2]  
        #lista_ids_sofascore.append(partido_sofascore)
        #insert_update_partidos(engine, df_partidos_ayer.iloc[i]) 
        #insert_confirmed_lineups(engine, df_partidos_ayer.iloc[i])
        #print("Partido actualizado correctamente")
        #postmatch_odss(engine, df_partidos_ayer.iloc[i])
        #insert_estadistica_partido(engine, df_partidos_ayer.iloc[i])
        #print("Estadistica de partidos insertada correctamente")
        #insert_mapa_de_calor(engine, driver, partido_sofascore)
        #print(f"Mapa de calor de partido {partido_sofascore} insertado correctamente")
        #insert_mapa_de_disparos(engine, driver, partido_sofascore)
        #procesar_puntos_partido(engine, df_partidos_ayer.iloc[i])
         
    #for i in range(len(ids_para_scrapear_temporada)):
        #liga_a_scrapear = ids_para_scrapear_temporada[i]
        #insert_tabla_posiciones(liga_a_scrapear, engine=engine,temporada=temporadas_ids#[liga_a_scrapear-1], liga=ligas_ids[liga_a_scrapear-1])   #actualiza la tabla de posiciones de todas las ligas
        #update_standings_evolution_graph(liga_a_scrapear,engine=engine, #temporada=temporadas_ids[liga_a_scrapear-1])
        #print(f"Tabla {liga_a_scrapear} insertada correctamente")
    
#ultimos_partidos(engine, temporada = 4)
#ultimos_partidos(engine, temporada = 5)
#update_fecha_partidos(engine, temporada=1, jornada = 9)
#update_fecha_partidos(engine, temporada=2, jornada = 10)
#update_fecha_partidos(engine, temporada=3, jornada = 8)
#update_fecha_partidos(engine, temporada=4, jornada = 8)
#update_fecha_partidos(engine, temporada=5, jornada = 9)
#pending_odds(engine, df_partidos_pendientes)


#df_entrenadores = pd.read_sql(f"select * from jugador where id_jugador > 3000;", engine)
#arreglo_url = extract_img_url(engine, df_entrenadores, driver)
#transfer_to_database(engine, arreglo_url, df_entrenadores)



print("Cerrando driver...")
driver.quit()