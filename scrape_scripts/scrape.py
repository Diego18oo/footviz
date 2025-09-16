from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.mysql import insert
from scrape_sofa import get_sofascore_api_data, init_driver, get_all_urls_fbref
from chat import  get_match_report_links_selenium
from datetime import datetime, timedelta
from funciones import liga_fbref_fixtures, ligas_ids, temporadas_ids, insert_tabla_posiciones, insert_update_partidos, insert_estadistica_partido, insert_update_plantilla_equipos, insert_mapa_de_calor, insert_mapa_de_disparos, insert_estadistica_jugador
import pandas as pd

today = datetime.now()
yesterday = today - timedelta(days=1)
yesterday = yesterday.strftime('%Y-%m-%d')
engine = create_engine('mysql+pymysql://root@localhost/footviz')


print(f"Obteniendo data de {yesterday}")
#selecciona los partidos que se jugaron el dia de ayer
df_partidos_ayer = pd.read_sql(f"SELECT * FROM partido WHERE fecha = '{yesterday}'", engine)
print("Inicializando driver...")
driver = init_driver()

if today.day == 30: 
    insert_update_plantilla_equipos(engine, driver, ligas_ids, temporadas_ids)
    print("Todo las plantillas estan actualizadas")
liga_fbref = [
    '9/Premier-League',
    '12/La-Liga',
    '11/Serie-A',
    '20/Bundesliga',
    '13/Ligue-1'
]
#verifica que si haya partidos el dia de ayer
if len(df_partidos_ayer) == 0:
    print("Hoy no hay nada que hacer mi loco")
    driver.quit()
else:
    lista_ids_sofascore = []  #guarda los ids de los partidos(sofascore) que se jugaron ayer en una lista     
    #lista_ids_bd = []
    #partidos = df_partidos_ayer['id_partido']['temporada']
    ids_para_scrapear_temporada = df_partidos_ayer['temporada'].unique() 
    insert_estadistica_jugador(engine,  ids_para_scrapear_temporada)
    #links_totales = []
    #for i in range(len(ids_para_scrapear_temporada)):
        #iteracion = ids_para_scrapear_temporada[i]
        #url = f"https://fbref.com/en/comps/{liga_fbref_fixtures[iteracion]}"
        #lista_links = get_all_urls_fbref(driver, liga_url=url)
        #links_totales.extend(lista_links)
    for i in range(len(df_partidos_ayer)):
        partido_sofascore = df_partidos_ayer['url_sofascore'][i].split(':')[2]  
        lista_ids_sofascore.append(partido_sofascore)
        #insert_update_partidos(engine, df_partidos_ayer.iloc[i]) 
        #print("Partido actualizado correctamente")
        #insert_estadistica_partido(engine, df_partidos_ayer.iloc[i])
        #print("Estadistica de partidos insertada correctamente")
        #insert_mapa_de_calor(engine, driver, partido_sofascore)
        #print(f"Mapa de calor de partido {partido_sofascore} insertado correctamente")
        #insert_mapa_de_disparos(engine, driver, partido_sofascore)
         
    #for i in range(len(ids_para_scrapear_temporada)):
        #liga_a_scrapear = ids_para_scrapear_temporada[i]
        #insert_tabla_posiciones(liga_a_scrapear, engine=engine,temporada=temporadas_ids[liga_a_scrapear-1], liga=ligas_ids[liga_a_scrapear-1])   #actualiza la tabla de posiciones de todas las ligas
        #print(f"Tabla {liga_a_scrapear} insertada correctamente")
        

print("Cerrando driver...")
driver.quit()