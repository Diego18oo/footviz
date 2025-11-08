import pandas as pd
import cloudinary
import cloudinary.uploader
import requests
import time
import numpy as np

from playwright.async_api import async_playwright 


import LanusStats as ls
from sqlalchemy import create_engine, MetaData, Table, text, update, bindparam
from sqlalchemy.dialects.mysql import insert
from scrape_sofa import get_sofascore_api_data, init_driver, get_all_teams_stats_fbref, get_all_urls_fbref, get_all_players_stats_fbref
#from scrape import links_totales 
from datetime import datetime, timedelta
from rapidfuzz import process, fuzz


fbref = ls.Fbref()
# --- 1. CONFIGURACIÓN DE CLOUDINARY ---
# Pega aquí tus credenciales
cloudinary.config(
  cloud_name = "donni11al",
  api_key = "725784717463829",
  api_secret = "URBwQFCj3Q7Ukl1KVioBnTUr6_E",
  secure = True
)

ligas_ids = [17, 8, 23, 35, 34] 
temporadas_ids = [76986, 77559, 76457, 77333, 77356]

liga_fbref = [
    '9/Premier-League',
    '12/La-Liga',
    '11/Serie-A',
    '20/Bundesliga',
    '13/Ligue-1'
]

liga_fbref_fixtures = [
    '9/schedule/Premier-League-Scores-and-Fixtures',
    '12/schedule/La-Liga-Scores-and-Fixtures',
    '11/schedule/Serie-A-Scores-and-Fixtures',
    '20/schedule/Bundesliga-Scores-and-Fixtures',
    '13/schedule/Ligue-1-Scores-and-Fixtures'
]
nombre_map = {
    "Manchester City" : "Manchester City",
    "Sunderland" : "Sunderland",
    "Tottenham Hotspur" : "Tottenham",
    "Liverpool" : "Liverpool",
    "Nottingham Forest" : "Nott'ham Forest",
    "Arsenal" : "Arsenal",
    "Fulham" : "Fulham",
    "Brighton & Hove Albion" : "Brighton",
    "Aston Villa" : "Aston Villa",
    "Newcastle United" : "Newcastle Utd",
    "Chelsea" : "Chelsea",
    "Crystal Palace" : "Crystal Palace",
    "Everton" : "Everton",
    "Leeds United" : "Leeds United",
    "Manchester United" : "Manchester Utd",
    "Bournemouth" : "Bournemouth",
    "Brentford" : "Brentford",
    "Burnley" : "Burnley",
    "West Ham United" : "West Ham",
    "Wolverhampton" : "Wolves",
    "Athletic Club" : "Athletic Club",
    "Atlético Madrid" : "Atlético Madrid",
    "Osasuna" : "Osasuna",
    "Elche" : "Elche",
    "Deportivo Alavés" : "Alavés",
    "Espanyol" : "Espanyol",
    "Barcelona" : "Barcelona",
    "Getafe" : "Getafe",
    "Girona FC" : "Girona",
    "Levante UD" : "Levante",
    "Rayo Vallecano" : "Rayo Vallecano",
    "Celta Vigo" : "Celta Vigo",
    "Mallorca" : "Mallorca",
    "Real Betis" : "Betis",
    "Real Madrid" : "Real Madrid",
    "Real Oviedo" : "Oviedo", 
    "Real Sociedad" : "Real Sociedad",
    "Sevilla" : "Sevilla",
    "Valencia" : "Valencia",
    "Villarreal" : "Villarreal",
    "Milan" : "Milan",
    "Fiorentina" : "Fiorentina",
    "Roma" : "Roma",
    "Atalanta" : "Atalanta",
    "Bologna" : "Bologna",
    "Cagliari" : "Cagliari",
    "Como" : "Como",
    "Genoa" : "Genoa",
    "Hellas Verona" : "Hellas Verona",
    "Inter" : "Inter",
    "Juventus" : "Juventus",
    "Lazio" : "Lazio",
    "Parma" : "Parma",
    "Pisa" : "Pisa",
    "Sassuolo" : "Sassuolo",
    "Napoli" : "Napoli",
    "Torino" : "Torino",
    "Udinese" : "Udinese",
    "Cremonese" : "Cremonese",
    "Lecce" : "Lecce",
    "1. FC Köln" : "Köln",
    "1. FC Heidenheim" : "Heidenheim",
    "Bayer 04 Leverkusen" : "Leverkusen",
    "FC Bayern München" : "Bayern Munich",
    "Borussia Dortmund" : "Dortmund",
    "Borussia M'gladbach" : "Gladbach",
    "Eintracht Frankfurt" : "Eint Frankfurt",
    "FC Augsburg" : "Augsburg",
    "FC St. Pauli" : "St. Pauli",
    "1. FSV Mainz 05" : "Mainz 05",
    "Hamburger SV" : "Hamburger SV",
    "RB Leipzig" : "RB Leipzig",
    "SC Freiburg" : "Freiburg",
    "TSG Hoffenheim" : "Hoffenheim",
    "1. FC Union Berlin" : "Union Berlin",
    "VfB Stuttgart" : "Stuttgart",
    "VfL Wolfsburg" : "Wolfsburg",
    "SV Werder Bremen" : "Werder Bremen",
    "Auxerre" : "Auxerre",
    "Angers" : "Angers",
    "AS Monaco" : "Monaco",
    "Lorient" : "Lorient",
    "Metz" : "Metz",
    "Nantes" : "Nantes",
    "Le Havre" : "Le Havre",
    "Lille" : "Lille",
    "Nice" : "Nice",
    "Olympique Lyonnais" : "Lyon",
    "Olympique de Marseille" : "Marseille",
    "Paris Saint-Germain" : "Paris S-G",
    "RC Lens" : "Lens",
    "Stade Brestois" : "Brest",
    "Stade Rennais" : "Rennes",
    "RC Strasbourg" : "Strasbourg",
    "Toulouse" : "Toulouse"
}

# --- 2. FUNCIÓN REUTILIZABLE PARA SUBIR IMÁGENES ---
# --- LA NUEVA FUNCIÓN BASADA EN SELENIUM ---
def upload_image_from_url(driver, image_url, public_id):
    """
    Usa un driver de Selenium para navegar a una URL de imagen,
    la convierte a Base64 y la sube a Cloudinary.
    """
    if not image_url:
        print("URL de imagen vacía, omitiendo subida.")
        return None
        
    try:
        # 1. Navegamos a la URL de la imagen con el driver que ya tenemos
        driver.get(image_url)
        # Una pequeña pausa para asegurar que cargue
        time.sleep(1)

        # 2. Truco mágico: Usamos JavaScript para leer la imagen como Base64
        # Esto es necesario porque Selenium no puede "descargar" imágenes directamente
        script = """
        const canvas = document.createElement('canvas');
        const ctx = canvas.getContext('2d');
        const img = document.querySelector('img');
        if (!img) return null;
        canvas.width = img.naturalWidth;
        canvas.height = img.naturalHeight;
        ctx.drawImage(img, 0, 0);
        return canvas.toDataURL('image/png');
        """
        # La página de la imagen de SofaScore es un <img> dentro de un <body>
        base64_image_data = driver.execute_script(script)

        if not base64_image_data:
            raise Exception("No se pudo extraer la imagen de la página.")

        # 3. Subimos la imagen en formato Base64 a Cloudinary
        upload_result = cloudinary.uploader.upload(
            base64_image_data, # Cloudinary entiende Base64 directamente
            public_id=public_id,
            folder="footviz",
            overwrite=True
        )
        
        secure_url = upload_result.get('secure_url')
        if secure_url:
            print(f"Imagen subida exitosamente: {secure_url}")
            return secure_url
        else:
            raise Exception(f"La subida a Cloudinary falló. Respuesta: {upload_result}")

    except Exception as e:
        print(f"Error procesando la imagen {image_url}: {e}")
        return None  

def get_links_totales(engine):
    
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    yesterday = yesterday.strftime('%Y-%m-%d')
    df_partidos_ayer = pd.read_sql(f"SELECT * FROM partido WHERE fecha = '{yesterday}'", engine)
    ids_para_scrapear_temporada = df_partidos_ayer['temporada'].unique() 
    links_totales = []
    for i in range(len(ids_para_scrapear_temporada)):
        iteracion = ids_para_scrapear_temporada[i]
        url = f"https://fbref.com/en/comps/{liga_fbref_fixtures[iteracion-1]}"
        lista_links = get_all_urls_fbref(liga_url=url)
        links_totales = links_totales + lista_links
    return links_totales

def insert_tabla_posiciones(liga_a_scrapear, engine, temporada):
    df_equipo = pd.read_sql('SELECT id_equipo, nombre FROM equipo', engine)
    df_temporada = pd.read_sql('SELECT id_temporada, nombre FROM temporada', engine)
    mapa_equipo = dict(zip(df_equipo['nombre'], df_equipo['id_equipo']))
    mapa_temporada = dict(zip(df_temporada['nombre'], df_temporada['id_temporada']))
    meta = MetaData()
    meta.reflect(bind=engine)
    tabla_estadisticas_equipo = meta.tables['estadisticas_equipo']
    driver = init_driver()
    with engine.begin() as conn:
        print(f"Iniiciando scrapeo a {liga_a_scrapear}")
        data = get_sofascore_api_data(driver, path=f"api/v1/unique-tournament/{ligas_ids[liga_a_scrapear - 1]}/season/{temporadas_ids[liga_a_scrapear -1]}/standings/total")
        torneo = data['standings'][0]
        if temporada == 76986:
            id_temporada = 1
            id_liga_fbref = liga_fbref[liga_a_scrapear - 1]
        elif temporada == 77559:
            id_temporada = 2
            id_liga_fbref = liga_fbref[liga_a_scrapear - 1]
        elif temporada == 76457:
            id_temporada = 3
            id_liga_fbref = liga_fbref[liga_a_scrapear - 1]
        elif temporada == 77333:
            id_temporada = 4
            id_liga_fbref = liga_fbref[liga_a_scrapear - 1]
        else:
            id_temporada = 5
            id_liga_fbref = liga_fbref[liga_a_scrapear - 1]
        print(f"Scrapeando {id_liga_fbref}...")
        data_fbref = get_all_teams_stats_fbref(id_liga_fbref)
        print("Scrapeo exitoso")

        for row in torneo['rows']:
            nombre_equipo_sofa = row['team']['name']
            nombre_equipo_fbref = nombre_map.get(nombre_equipo_sofa, nombre_equipo_sofa)  

            id_equipo = mapa_equipo.get(nombre_equipo_sofa)
            try:
                data_equipo = data_fbref[data_fbref['Squad'] == nombre_equipo_fbref].iloc[0]
            except IndexError:
                print(f"No se encontraron stats para {nombre_equipo_sofa}")
                continue

            if id_equipo is None or id_temporada is None:
                print(f"No se encontró ID para equipo '{nombre_equipo_sofa}' o temporada {temporada}")
                continue

            tarjetas_rojass = data_equipo['CrdR']
            segunda_amarilla = data_equipo['2CrdY']

            tiros_libre_disparo = data_equipo['FKaPuerta']
            tiros_libre_centro = data_equipo['FK']

            insert_stmt = insert(tabla_estadisticas_equipo).values(
                equipo=id_equipo,
                temporada=id_temporada,
                posicion=row['position'],
                puntos=row['points'],
                victorias=row['wins'],
                empates=row['draws'],
                derrotas=row['losses'],
                partidos_jugados=row['matches'],
                goles_anotados=row['scoresFor'],
                goles_recibidos=row['scoresAgainst'],
                diferencia_de_goles=row['scoreDiffFormatted'],
                xg = data_equipo['xG'],
                disparos = data_equipo['Sh'],
                faltas = data_equipo['Fls'],
                tarjetas_amarillas = data_equipo['CrdY'],           
                tarjetas_rojas = tarjetas_rojass + segunda_amarilla,
                tiros_de_esquina = data_equipo['CK'],
                saques_de_puerta = data_equipo['goalKick'],
                disparos_a_puerta = data_equipo['SoT'],
                tiros_libre = tiros_libre_centro + tiros_libre_disparo,
                goles_de_penal = data_equipo['PK'],
                pases_intentados = data_equipo['PasesIntentados'],
                pases_completados = data_equipo['Cmp'],
                entradas = data_equipo['Tkl'],
                bloqueos = data_equipo['Blocks'],
                intercepciones = data_equipo['Int'],
                despejes = data_equipo['Clr'],
                duelos_aereos_ganados = data_equipo['Won'],
                regates = data_equipo['dribblesAtt'],
                acciones_creadas = data_equipo['GCA'],
                acarreos_progresivos = data_equipo['PrgC'],
                pases_progresivos = data_equipo['PrgP'],
                xg_en_contra = data_equipo['xgAgainst']
            )

            update_stmt = insert_stmt.on_duplicate_key_update(
                posicion=insert_stmt.inserted.posicion,
                puntos=insert_stmt.inserted.puntos,
                victorias=insert_stmt.inserted.victorias,
                empates=insert_stmt.inserted.empates,
                derrotas=insert_stmt.inserted.derrotas,
                partidos_jugados=insert_stmt.inserted.partidos_jugados,
                goles_anotados=insert_stmt.inserted.goles_anotados,
                goles_recibidos=insert_stmt.inserted.goles_recibidos,
                diferencia_de_goles=insert_stmt.inserted.diferencia_de_goles,
                xg=insert_stmt.inserted.xg,
                disparos=insert_stmt.inserted.disparos,
                faltas=insert_stmt.inserted.faltas,
                tarjetas_amarillas=insert_stmt.inserted.tarjetas_amarillas,
                tarjetas_rojas=insert_stmt.inserted.tarjetas_rojas,
                tiros_de_esquina=insert_stmt.inserted.tiros_de_esquina,
                saques_de_puerta=insert_stmt.inserted.saques_de_puerta,
                disparos_a_puerta=insert_stmt.inserted.disparos_a_puerta,
                tiros_libre=insert_stmt.inserted.tiros_libre,
                goles_de_penal=insert_stmt.inserted.goles_de_penal,
                pases_intentados=insert_stmt.inserted.pases_intentados,
                pases_completados=insert_stmt.inserted.pases_completados,
                entradas=insert_stmt.inserted.entradas,
                bloqueos=insert_stmt.inserted.bloqueos,
                intercepciones=insert_stmt.inserted.intercepciones,
                despejes=insert_stmt.inserted.despejes,
                duelos_aereos_ganados=insert_stmt.inserted.duelos_aereos_ganados,
                regates=insert_stmt.inserted.regates,
                acciones_creadas=insert_stmt.inserted.acciones_creadas,
                acarreos_progresivos=insert_stmt.inserted.acarreos_progresivos,
                pases_progresivos=insert_stmt.inserted.pases_progresivos,
                xg_en_contra = insert_stmt.inserted.xg_en_contra

            )

            conn.execute(update_stmt)
    driver.quit()
    return


def verificar_fecha_partido(driver, url_sofascore, jornada, temporada):
    match_id = url_sofascore.split(':')[-1]
    url = f"api/v1/event/{match_id}"
    data = get_sofascore_api_data(driver, path=url)
    slug = data['event']['slug']
    if data['event']['status']['description'] != 'Ended': #en caso de que hayan cambiado la fecha del partido
        print("Fecha del partido ha sido modificada")
        new_url = f"api/v1/unique-tournament/{ligas_ids[temporada-1]}/season/{temporadas_ids[temporada-1]}/events/round/{jornada}"
        new_data = get_sofascore_api_data(driver, path=new_url)
        for row in new_data['events']:  #itera sobre todos los partidos de la jornada hasta encontrar uno que coinicida con el slug
            if row['slug'] != slug:
                continue
            else:
                new_fecha = row.get('startTimestamp', None)
                return new_fecha
    else: 
        fecha = data['event'].get('startTimestamp', None) 
        fecha = datetime.utcfromtimestamp(fecha).strftime('%Y-%m-%d')
        print("Fecha confirmada")
        return fecha

def get_arbitro(driver, url_sofascore):
    match_id = url_sofascore.split(':')[-1]
    url = f"api/v1/event/{match_id}"
    data = get_sofascore_api_data(driver, path=url)
    try: 
        arbitro = data['event']['referee']
        return arbitro
    except KeyError:
        print("No hubo data de arbitro")
        return
    
def insert_arbitro(engine, arbitro_data, tabla_arbitro, conn):
    df_pais = pd.read_sql('SELECT id_pais, nombre FROM pais', engine)
    mapa_pais = dict(zip(df_pais['nombre'], df_pais['id_pais']))

    nombre = arbitro_data.get('name', None)
    tarjetas_amarillas = arbitro_data.get('yellowCards', None)
    segunda_amarilla = arbitro_data.get('yellowRedCards', None)
    roja_directa = arbitro_data.get('redCards', None)
    tarjetas_rojas = roja_directa + segunda_amarilla
    partidos_arbitrados = arbitro_data.get('games', None)
    pais_nombre = arbitro_data.get('country', {}).get('name', None)
    pais = mapa_pais.get(pais_nombre)

    insert_arbitro_stmt = insert(tabla_arbitro).values(
        nombre = nombre,
        tarjetas_amarillas=tarjetas_amarillas,
        tarjetas_rojas=tarjetas_rojas,
        partidos_arbitrados=partidos_arbitrados,
        id_pais=pais
    )

    update_arbitro_stmt = insert_arbitro_stmt.on_duplicate_key_update(
        nombre=insert_arbitro_stmt.inserted.nombre,
        tarjetas_amarillas=insert_arbitro_stmt.inserted.tarjetas_amarillas,
        tarjetas_rojas=insert_arbitro_stmt.inserted.tarjetas_rojas,
        partidos_arbitrados=insert_arbitro_stmt.inserted.partidos_arbitrados,
        id_pais=insert_arbitro_stmt.inserted.id_pais
    )

    conn.execute(update_arbitro_stmt)
    return

def insert_update_partidos(engine, partido):
    meta = MetaData()
    meta.reflect(bind=engine)
    df_arbitro = pd.read_sql('SELECT id_arbitro, nombre FROM arbitro', engine)
    mapa_arbitro = dict(zip(df_arbitro['nombre'], df_arbitro['id_arbitro']))
    
    df_equipo = pd.read_sql('SELECT id_equipo, nombre FROM equipo', engine)
    mapa_equipo = dict(zip(df_equipo['nombre'], df_equipo['id_equipo']))

    tabla_partido = meta.tables['partido']
    tabla_arbitro = meta.tables['arbitro']
    driver = init_driver()
    fbref_urls = [
        "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures",
        "https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures",
        "https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures",
        "https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures",
        "https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures"
    ]
    with engine.begin() as conn:
        id_partido = partido[0]
        jornada = partido[1]
        temporada = partido[5]
        url_sofascore = partido[3]
        fecha = partido[4]
        equipo_local = partido[6]
        equipo_visitante = partido[7]
        nombre_equipo_local = pd.read_sql(f'SELECT nombre FROM equipo WHERE id_equipo = {equipo_local}', engine)
        nombre_equipo_visitante = pd.read_sql(f'SELECT nombre FROM equipo WHERE id_equipo = {equipo_visitante}', engine)
        nombre_equipo_visitante = nombre_equipo_visitante['nombre'][0]
        nombre_equipo_local = nombre_equipo_local['nombre'][0]
        local_fbref = nombre_map.get(nombre_equipo_local)
        visitante_fbref = nombre_map.get(nombre_equipo_visitante)
        slug = f"{local_fbref}-{visitante_fbref}"
        slug = slug.replace(" ", "-")
        url_fbref = None
        print("Entrando a get_links_totales")
        links_totales = get_links_totales(engine) 
        mejor_link, score, _ = process.extractOne(
            slug,                   
            links_totales,        
            scorer=fuzz.partial_ratio  
        )

        if score > 70:  
            url_fbref = mejor_link
            print(f"URL FBref encontrado: {url_fbref} (score: {score})")
        else:
            print(f"No se encontró un link suficientemente parecido para {slug}")

        estadio = partido[8]
        arbitro_data = get_arbitro(driver, url_sofascore)
        print("Data de arbitro obtenida")
        try:
            arbitro_nombre = arbitro_data['name']
            id_arbitro = mapa_arbitro.get(arbitro_nombre, None)
            if id_arbitro is None:
                print(f"Insertando a {arbitro_nombre}...")
                insert_arbitro(engine, arbitro_data, tabla_arbitro, conn)
                print(f"{arbitro_nombre} insertado correctamente")
                df_arbitro_actualizado = pd.read_sql('SELECT id_arbitro, nombre FROM arbitro', engine)
                mapa_arbitro.update(dict(zip(df_arbitro_actualizado['nombre'], df_arbitro_actualizado['id_arbitro'])))
                id_arbitro = mapa_arbitro.get(arbitro_nombre)
            insert_partido_stmt = insert(tabla_partido).values(
                id_partido = id_partido,
                jornada=jornada,
                url_fbref=url_fbref,
                url_sofascore=url_sofascore,
                fecha=fecha,
                temporada=temporada,
                equipo_local=equipo_local,
                equipo_visitante=equipo_visitante,
                estadio=estadio,
                arbitro=id_arbitro
            )

            update_partido_stmt=insert_partido_stmt.on_duplicate_key_update(
                id_partido=insert_partido_stmt.inserted.id_partido,
                jornada=insert_partido_stmt.inserted.jornada,
                url_fbref=insert_partido_stmt.inserted.url_fbref,
                url_sofascore=insert_partido_stmt.inserted.url_sofascore,
                fecha=insert_partido_stmt.inserted.fecha,
                temporada=insert_partido_stmt.inserted.temporada,
                equipo_local=insert_partido_stmt.inserted.equipo_local,
                equipo_visitante=insert_partido_stmt.inserted.equipo_visitante,
                estadio=insert_partido_stmt.inserted.estadio,
                arbitro=insert_partido_stmt.inserted.arbitro
            )

            conn.execute(update_partido_stmt)
            print(f"Partido {equipo_local} - {equipo_visitante} actualizado correctamente")
        except TypeError:
            print("Partido a revision")

    driver.quit()
    return 


def get_stat(stat_list, stat_name, default=None):
    return next((item for item in stat_list if item['name'] == stat_name), default)


def insert_estadistica_partido(engine, df_partido): 
    meta = MetaData()
    meta.reflect(bind=engine)

    tabla_estadistica_partido = meta.tables['estadisticas_partido']
    driver = init_driver()

    with engine.begin() as conn:
        url_sofascore = df_partido[3]
        id_sofascore = url_sofascore.split(':')[-1]
        url = f"api/v1/event/{id_sofascore}"
        raw_data = get_sofascore_api_data(driver, path=url)
        try:
            data = get_sofascore_api_data(driver, path=f"{url}/statistics")
            info = data['statistics'][0]['groups'][0]['statisticsItems']
            print("Data del partido obtenida")
            yellow_cards = get_stat(info, "Yellow cards", default={"home": 0, "away": 0})
            red_cards = get_stat(info, "Red cards", default={"home": 0, "away": 0})
            ball_possession = get_stat(info, "Ball possession", default={"home": 0, "away": 0})
            passes = get_stat(info, "Passes", default={"home": 0, "away": 0})
            corners = get_stat(info, "Corner kicks", default={"home": 0, "away": 0})
            shots = get_stat(info, "Total shots", default={"home": 0, "away": 0})
            fouls = get_stat(info, "Fouls", default={"home": 0, "away": 0})
            tackles = get_stat(info, "Tackles", default={"home": 0, "away": 0})
            xg = get_stat(info, "Expected goals", default={"home":0, "away":0})



            insert_estadistica_partido_stmt = insert(tabla_estadistica_partido).values(
                partido = df_partido[0],
                goles_local = raw_data['event']['homeScore']['current'],
                goles_visitante = raw_data['event']['awayScore']['current'],
                tarjetas_amarillas_local = yellow_cards['home'],
                tarjetas_amarillas_visitante = yellow_cards['away'],
                tarjetas_rojas_local = red_cards['home'],
                tarjetas_rojas_visitante = red_cards['away'],
                posesion_local = ball_possession['homeValue'],
                posesion_visitante = ball_possession['awayValue'],
                pases_local = passes['home'],
                pases_visitante = passes['away'],
                tiros_de_esquina_local = corners['home'],
                tiros_de_esquina_visitante = corners['away'],
                disparos_local = shots['home'],
                disparos_visitante = shots['away'],
                disparos_a_puerta_local = data['statistics'][0]['groups'][1]['statisticsItems'][1]['home'],
                disparos_a_puerta_visitante = data['statistics'][0]['groups'][1]['statisticsItems'][1]['away'],
                faltas_local = fouls['home'],
                faltas_visitante = fouls['away'],
                entradas_local = tackles['home'],
                entradas_visitante = tackles['away'],
                xg_local = xg['home'],
                xg_visitante = xg['away']
            )

            update_estadistica_partido_stmt = insert_estadistica_partido_stmt.on_duplicate_key_update(
                partido=insert_estadistica_partido_stmt.inserted.partido,
                goles_local=insert_estadistica_partido_stmt.inserted.goles_local,
                goles_visitante=insert_estadistica_partido_stmt.inserted.goles_visitante,
                tarjetas_amarillas_local=insert_estadistica_partido_stmt.inserted.tarjetas_amarillas_local,
                tarjetas_amarillas_visitante=insert_estadistica_partido_stmt.inserted.tarjetas_amarillas_visitante,
                tarjetas_rojas_local=insert_estadistica_partido_stmt.inserted.tarjetas_rojas_local,
                tarjetas_rojas_visitante=insert_estadistica_partido_stmt.inserted.tarjetas_rojas_visitante,
                posesion_local=insert_estadistica_partido_stmt.inserted.posesion_local,
                posesion_visitante=insert_estadistica_partido_stmt.inserted.posesion_visitante,
                pases_local=insert_estadistica_partido_stmt.inserted.pases_local,
                pases_visitante=insert_estadistica_partido_stmt.inserted.pases_visitante,
                tiros_de_esquina_local=insert_estadistica_partido_stmt.inserted.tiros_de_esquina_local,
                tiros_de_esquina_visitante=insert_estadistica_partido_stmt.inserted.tiros_de_esquina_visitante,
                disparos_local=insert_estadistica_partido_stmt.inserted.disparos_local,
                disparos_visitante=insert_estadistica_partido_stmt.inserted.disparos_visitante,
                disparos_a_puerta_local=insert_estadistica_partido_stmt.inserted.disparos_a_puerta_local,
                disparos_a_puerta_visitante=insert_estadistica_partido_stmt.inserted.disparos_a_puerta_visitante,
                faltas_local=insert_estadistica_partido_stmt.inserted.faltas_local,
                faltas_visitante=insert_estadistica_partido_stmt.inserted.faltas_visitante,
                entradas_local=insert_estadistica_partido_stmt.inserted.entradas_local,
                entradas_visitante=insert_estadistica_partido_stmt.inserted.entradas_visitante,
                xg_local = insert_estadistica_partido_stmt.inserted.xg_local,
                xg_visitante = insert_estadistica_partido_stmt.inserted.xg_visitante            
            )

            conn.execute(update_estadistica_partido_stmt)
        except KeyError: 
            print("partido en revision")

    return 


def get_jugador(driver, id_jugador_sofascore):
    url = f"api/v1/player/{id_jugador_sofascore}"
    data_jugador = get_sofascore_api_data(driver, url) 
    return data_jugador
 
def insert_jugador(engine, data_jugador, conn, tabla_jugador):
    
    df_pais = pd.read_sql('SELECT id_pais, nombre FROM pais', engine)
    mapa_pais = dict(zip(df_pais['nombre'], df_pais['id_pais']))
    dob_timestamp = data_jugador['player'].get('dateOfBirthTimestamp', None)
    base_url_img = 'https://img.sofascore.com/api/v1/player/'
    id_sofascore = data_jugador['player']['id']
    nombre_pais = data_jugador['player']['country'].get('name', None)
    try:
        if dob_timestamp is not None:
            fecnac = datetime.utcfromtimestamp(dob_timestamp).strftime('%Y-%m-%d')
        else: 
            fecnac = None
        dorsal_raw = data_jugador['player'].get('jerseyNumber')
        
        # 2. Limpiar el valor
        dorsal_final = None  # Empezamos asumiendo que será NULL
        
        # Comprobamos que no sea None Y que no sea un string vacío
        if dorsal_raw is not None and dorsal_raw != '':
            try:
                # Intentamos convertirlo a entero
                dorsal_final = int(dorsal_raw)
            except ValueError:
                # Si falla (ej: es "N/A" o algo raro), lo dejamos como None
                print(f"Valor de dorsal no válido '{dorsal_raw}' para {data_jugador['player']['name']}. Se usará NULL.")
                dorsal_final = None
        insert_jugador_stmt = insert(tabla_jugador).values(
            nombre = data_jugador['player']['name'],
            dorsal = dorsal_final,
            fec_nac = fecnac,
            posicion = data_jugador['player'].get('position', None),
            valor_mercado = data_jugador['player'].get('proposedMarketValue', None),
            altura = data_jugador['player'].get('height', None),
            pie_preferido = data_jugador['player'].get('preferredFoot', None),
            id_sofascore = id_sofascore,
            url_imagen = f"{base_url_img}{id_sofascore}/image",
            pais = mapa_pais.get(nombre_pais)
        )

        update_jugador_stmt = insert_jugador_stmt.on_duplicate_key_update(
            nombre=insert_jugador_stmt.inserted.nombre,
            dorsal=insert_jugador_stmt.inserted.dorsal,
            fec_nac=insert_jugador_stmt.inserted.fec_nac,
            posicion=insert_jugador_stmt.inserted.posicion,
            valor_mercado=insert_jugador_stmt.inserted.valor_mercado,
            altura=insert_jugador_stmt.inserted.altura,
            pie_preferido=insert_jugador_stmt.inserted.pie_preferido, 
            id_sofascore=insert_jugador_stmt.inserted.id_sofascore, 
            url_imagen=insert_jugador_stmt.inserted.url_imagen, 
            pais=insert_jugador_stmt.inserted.pais, 
        )

        conn.execute(update_jugador_stmt)
        print(f"Jugador {data_jugador['player']['name']} insertado correctamente")
    except OSError:
        print("Ocurrio un error al insertar este jugador")
        return
    
    return 

def insert_update_plantilla_equipos(engine, driver, ligas, temporadas, db_url):
    engine = create_engine(db_url)
    df_jugadores = pd.read_sql('SELECT id_jugador, nombre FROM jugador', engine)
    mapa_jugadores = dict(zip(df_jugadores['nombre'], df_jugadores['id_jugador']))
    df_equipos = pd.read_sql('SELECT id_equipo, nombre FROM equipo', engine)
    mapa_equipos = dict(zip(df_equipos['nombre'], df_equipos['id_equipo']))
    meta = MetaData()
    meta.reflect(bind=engine)
    tabla_plantilla_equipos = meta.tables['plantilla_equipos']
    tabla_jugador = meta.tables['jugador']
    with engine.begin() as conn:
        for i in range(len(ligas)):
            print(f"Inicializando scrape a {ligas[i]}")
            url = f"api/v1/unique-tournament/{ligas[i]}/season/{temporadas[i]}/standings/total"
            data_temporada = get_sofascore_api_data(driver, path=url)
            torneo = data_temporada['standings'][0]
            fecha_inicio = '2025-08-01'
            for row in range(len(torneo['rows'])):
                id_equipo = torneo['rows'][row]['team']['id']
                nombre_equipo = torneo['rows'][row]['team']['name']
                print(f"Scraping {nombre_equipo}...")
                data_jugadores = get_sofascore_api_data(driver, path=f"api/v1/team/{id_equipo}/players")
                for player in range(len(data_jugadores['players'])):
                    nombre_jugador = data_jugadores['players'][player]['player']['name']
                    id_jugador = mapa_jugadores.get(nombre_jugador, None)
                    id_jugador_sofascore = data_jugadores['players'][player]['player']['id']
                    if id_jugador is None:
                        data_jugador = get_jugador(driver, id_jugador_sofascore)
                        print(f"Insertando a {nombre_jugador}...")
                        insert_jugador(engine, data_jugador, conn, tabla_jugador)
                        df_jugador_actualizado = pd.read_sql('SELECT id_jugador, nombre FROM jugador', engine)
                        mapa_jugadores.update(dict(zip(df_jugador_actualizado['nombre'], df_jugador_actualizado['id_jugador'])))
                        id_jugador = mapa_jugadores.get(nombre_jugador)
                    insert_plantilla_equipos_stmt = insert(tabla_plantilla_equipos).values(
                        jugador = id_jugador,
                        equipo = mapa_equipos.get(nombre_equipo, None),
                        temporada = i +1
                    )

                    update_plantilla_equipos_stmt = insert_plantilla_equipos_stmt.on_duplicate_key_update(
                        jugador=insert_plantilla_equipos_stmt.inserted.jugador,
                        equipo=insert_plantilla_equipos_stmt.inserted.equipo,
                        temporada=insert_plantilla_equipos_stmt.inserted.temporada,
                    )

                    conn.execute(update_plantilla_equipos_stmt)
                print(f"Equipo {nombre_equipo} actualizado correctamente")
            print(f"Liga {ligas} actualizada correctamente")
    return 

def extraer_id_sofascore(url):
    return url.split(':')[-1]


def insert_estadistica_partido_jugador(data, engine ,id_sofascore, posicion, tarjeta_amarilla, tarjeta_roja, nombre_jugador):
    df_jugadores = pd.read_sql('SELECT id_jugador, nombre FROM jugador', engine)
    mapa_jugadores = dict(zip(df_jugadores['nombre'], df_jugadores['id_jugador']))
    df_partido = pd.read_sql('SELECT id_partido, url_sofascore FROM partido', engine)
    mapa_partidos = {
        extraer_id_sofascore(url): id_partido
        for id_partido, url in zip(df_partido['id_partido'], df_partido['url_sofascore'])
    }
    id_partido = mapa_partidos.get(str(id_sofascore))
    meta = MetaData()
    meta.reflect(bind=engine)
    tabla_estadistica_jugador_partido = meta.tables['estadistica_jugador_partido']
    tabla_estadistica_jugador_portero = meta.tables['estadistica_jugador_portero']
    with engine.begin() as conn: 
        if posicion == 'G':
            insert_estadistica_jugador_portero_stmt = insert(tabla_estadistica_jugador_portero).values(
                pases_totales = data['statistics'].get('totalPass', 0),
                pases_acertados = data['statistics'].get('accuratePass', 0),
                pases_largos_totales = data['statistics'].get('totalLongBalls', 0),
                pases_largos_acertados = data['statistics'].get('accurateLongBalls', 0),
                asistencias = data['statistics'].get('goalAssist', 0),
                goles = data['statistics'].get('goals', 0),
                atajadas_dentro_del_area = data['statistics'].get('savedShotsFromInsideTheBox', 0),
                atajadas = data['statistics'].get('saves', 0),
                minutos = data['statistics'].get('minutesPlayed', 0),
                toques = data['statistics'].get('touches', 0),
                rating = data['statistics'].get('rating', 0),
                goles_prevenidos = round(data['statistics'].get('goalsPrevented', 0), 5),
                penales_atajados = data['statistics'].get('penaltySave', 0),
                tarjeta_amarilla = tarjeta_amarilla,
                tarjeta_roja = tarjeta_roja,
                jugador = mapa_jugadores.get(nombre_jugador),
                partido = id_partido
            )

            update_estadistica_jugador_portero_stmt = insert_estadistica_jugador_portero_stmt.on_duplicate_key_update(
                pases_totales=insert_estadistica_jugador_portero_stmt.inserted.pases_totales,
                pases_acertados=insert_estadistica_jugador_portero_stmt.inserted.pases_acertados,
                pases_largos_totales=insert_estadistica_jugador_portero_stmt.inserted.pases_largos_totales,
                pases_largos_acertados=insert_estadistica_jugador_portero_stmt.inserted.pases_largos_acertados,
                asistencias=insert_estadistica_jugador_portero_stmt.inserted.asistencias,
                goles=insert_estadistica_jugador_portero_stmt.inserted.goles,
                atajadas_dentro_del_area=insert_estadistica_jugador_portero_stmt.inserted.atajadas_dentro_del_area,
                atajadas=insert_estadistica_jugador_portero_stmt.inserted.atajadas,
                minutos=insert_estadistica_jugador_portero_stmt.inserted.minutos,
                toques=insert_estadistica_jugador_portero_stmt.inserted.toques,
                rating=insert_estadistica_jugador_portero_stmt.inserted.rating,
                goles_prevenidos=insert_estadistica_jugador_portero_stmt.inserted.goles_prevenidos,
                penales_atajados=insert_estadistica_jugador_portero_stmt.inserted.penales_atajados,
                tarjeta_amarilla=insert_estadistica_jugador_portero_stmt.inserted.tarjeta_amarilla,
                tarjeta_roja=insert_estadistica_jugador_portero_stmt.inserted.tarjeta_roja,
                jugador=insert_estadistica_jugador_portero_stmt.inserted.jugador,
                partido=insert_estadistica_jugador_portero_stmt.inserted.partido,
            )
            conn.execute(update_estadistica_jugador_portero_stmt)
        else: 
            insert_estadistica_jugador_partido_stmt = insert(tabla_estadistica_jugador_partido).values(
                jugador = mapa_jugadores.get(nombre_jugador),
                partido = id_partido,
                goles =  data['statistics'].get('goals', 0),
                pases_totales = data['statistics'].get('totalPass', 0),
                pases_acertados = data['statistics'].get('accuratePass', 0),
                pases_largos_totales = data['statistics'].get('totalLongBalls', 0),
                pases_largos_acertados = data['statistics'].get('accurateLongBalls', 0),
                asistencias = data['statistics'].get('goalAssist', 0), 
                despejes = data['statistics'].get('totalClearance', 0), 
                intercepciones = data['statistics'].get('interceptionWon', 0),
                entradas = data['statistics'].get('totalTackle', 0),
                faltas_cometidas = data['statistics'].get('fouls', 0),
                faltas_recibidas = data['statistics'].get('wasFouled', 0),
                minutos = data['statistics'].get('minutesPlayed', 0),
                toques = data['statistics'].get('touches', 0),
                rating = data['statistics'].get('rating', 0),
                perdidas_de_balon = data['statistics'].get('possessionLostCtrl', 0),
                regates_totales = data['statistics'].get('totalContest', 0),
                regates_efectivos = data['statistics'].get('wonContest', 0),
                bloqueos = data['statistics'].get('outfielderBlock', 0),
                disparos_a_puerta = data['statistics'].get('onTargetScoringAttempt', 0),
                disparos_desviados = data['statistics'].get('shotOffTarget', 0),
                pases_clave = data['statistics'].get('keyPass', 0),
                grandes_oportunidades_creadas = data['statistics'].get('bigChanceCreated', 0),
                goles_esperados = round(data['statistics'].get('expectedGoals', 0), 5),
                asistencias_esperadas = round(data['statistics'].get('expectedAssists', 0), 5),
                entrada_de_ultimo_hombre = data['statistics'].get('lastManTackle', 0),
                duelos_aereos_ganados = data['statistics'].get('aerialWon', 0),
                duelos_aereos_perdidos = data['statistics'].get('aerialLost', 0),
                centros = data['statistics'].get('totalCross', 0),
                centros_acertados = data['statistics'].get('accurateCross', 0),
                disparos_bloqueados = data['statistics'].get('blockedScoringAttempt', 0),
                gran_oportunidad_fallada = data['statistics'].get('bigChanceMissed', 0),
                tarjeta_amarilla = tarjeta_amarilla,
                tarjeta_roja = tarjeta_roja,
                autogoles = data['statistics'].get('owngoal', 0)
            )
            
            update_estadistica_jugador_partido_stmt = insert_estadistica_jugador_partido_stmt.on_duplicate_key_update(
                jugador = insert_estadistica_jugador_partido_stmt.inserted.jugador, 
                partido = insert_estadistica_jugador_partido_stmt.inserted.partido,
                pases_totales = insert_estadistica_jugador_partido_stmt.inserted.pases_totales,
                pases_acertados = insert_estadistica_jugador_partido_stmt.inserted.pases_acertados,
                pases_largos_totales = insert_estadistica_jugador_partido_stmt.inserted.pases_largos_totales,
                pases_largos_acertados = insert_estadistica_jugador_partido_stmt.inserted.pases_largos_acertados,
                asistencias = insert_estadistica_jugador_partido_stmt.inserted.asistencias,
                despejes = insert_estadistica_jugador_partido_stmt.inserted.despejes,
                intercepciones = insert_estadistica_jugador_partido_stmt.inserted.intercepciones,  
                entradas = insert_estadistica_jugador_partido_stmt.inserted.entradas,
                faltas_cometidas = insert_estadistica_jugador_partido_stmt.inserted.faltas_cometidas,
                faltas_recibidas = insert_estadistica_jugador_partido_stmt.inserted.faltas_recibidas,
                minutos = insert_estadistica_jugador_partido_stmt.inserted.minutos,
                toques = insert_estadistica_jugador_partido_stmt.inserted.toques,
                rating = insert_estadistica_jugador_partido_stmt.inserted.rating,
                perdidas_de_balon = insert_estadistica_jugador_partido_stmt.inserted.perdidas_de_balon,
                regates_totales = insert_estadistica_jugador_partido_stmt.inserted.regates_totales,
                regates_efectivos = insert_estadistica_jugador_partido_stmt.inserted.regates_efectivos,
                bloqueos = insert_estadistica_jugador_partido_stmt.inserted.bloqueos,
                disparos_a_puerta = insert_estadistica_jugador_partido_stmt.inserted.disparos_a_puerta,
                disparos_desviados = insert_estadistica_jugador_partido_stmt.inserted.disparos_desviados,
                pases_clave = insert_estadistica_jugador_partido_stmt.inserted.pases_clave,
                grandes_oportunidades_creadas = insert_estadistica_jugador_partido_stmt.inserted.grandes_oportunidades_creadas,
                goles_esperados = insert_estadistica_jugador_partido_stmt.inserted.goles_esperados,
                asistencias_esperadas = insert_estadistica_jugador_partido_stmt.inserted.asistencias_esperadas,
                entrada_de_ultimo_hombre = insert_estadistica_jugador_partido_stmt.inserted.entrada_de_ultimo_hombre,
                duelos_aereos_ganados = insert_estadistica_jugador_partido_stmt.inserted.duelos_aereos_ganados,
                duelos_aereos_perdidos = insert_estadistica_jugador_partido_stmt.inserted.duelos_aereos_perdidos,
                centros = insert_estadistica_jugador_partido_stmt.inserted.centros,
                centros_acertados = insert_estadistica_jugador_partido_stmt.inserted.centros_acertados,
                disparos_bloqueados = insert_estadistica_jugador_partido_stmt.inserted.disparos_bloqueados,
                gran_oportunidad_fallada = insert_estadistica_jugador_partido_stmt.inserted.gran_oportunidad_fallada,
                tarjeta_amarilla = insert_estadistica_jugador_partido_stmt.inserted.tarjeta_amarilla,
                tarjeta_roja = insert_estadistica_jugador_partido_stmt.inserted.tarjeta_roja,
                autogoles = insert_estadistica_jugador_partido_stmt.inserted.autogoles
            )

            conn.execute(update_estadistica_jugador_partido_stmt)
    return

def insert_mapa_de_calor(engine, driver, id_sofascore):
    df_jugadores = pd.read_sql('SELECT id_jugador, nombre FROM jugador', engine)
    mapa_jugadores = dict(zip(df_jugadores['nombre'], df_jugadores['id_jugador']))
    df_partido = pd.read_sql('SELECT id_partido, url_sofascore FROM partido', engine)
    mapa_partidos = {
        extraer_id_sofascore(url): id_partido
        for id_partido, url in zip(df_partido['id_partido'], df_partido['url_sofascore'])
    }
    id_partido = mapa_partidos.get(str(id_sofascore))
    meta = MetaData()
    meta.reflect(bind=engine)
    tabla_mapa_de_calor = meta.tables['mapa_de_calor']
    tabla_jugador = meta.tables['jugador'] 
    with engine.begin() as conn:
        url = f"api/v1/event/{id_sofascore}/lineups"
        data = get_sofascore_api_data(driver, path=url)
        incidents = get_sofascore_api_data(driver, path=f"api/v1/event/{id_sofascore}/incidents")
        lista_amarillas = []
        lista_rojas = []
        lista_amarillas_rojas = []
        for i in range(len(incidents['incidents'])):
            try: 
                if incidents['incidents'][i]['incidentClass'] == 'yellow':
                    id_jugador = incidents['incidents'][i]['player']['id']
                    lista_amarillas.append(id_jugador)
                elif incidents['incidents'][i]['incidentClass'] == 'yellowRed':
                    id_jugador = incidents['incidents'][i]['player']['id']
                    lista_amarillas_rojas.append(id_jugador)
                elif incidents['incidents'][i]['incidentClass'] == 'red':
                    id_jugador = incidents['incidents'][i]['player']['id']
                    lista_rojas.append(id_jugador)
            except KeyError:
                print(",")
        for i in range(len(data['home']['players'])):
            posicion = data['home']['players'][i]['position']
            id_jugador_sofascore = data['home']['players'][i]['player']['id']
            if id_jugador_sofascore in lista_amarillas:
                tarjeta_amarilla = 1
                tarjeta_roja = 0
            elif id_jugador_sofascore in lista_rojas:
                tarjeta_amarilla = 0
                tarjeta_roja = 1
            elif id_jugador_sofascore in lista_amarillas_rojas:
                tarjeta_amarilla = 2
                tarjeta_roja = 1
            else: 
                tarjeta_amarilla = 0
                tarjeta_roja = 0
            
            nombre_jugador = data['home']['players'][i]['player']['name']
            id_jugador = mapa_jugadores.get(nombre_jugador, None)
            if id_jugador is None: 
                data_jugador = get_jugador(driver, id_jugador_sofascore)
                insert_jugador(engine, data_jugador, conn, tabla_jugador)
                df_jugador_actualizado = pd.read_sql('SELECT id_jugador, nombre FROM jugador', engine)
                mapa_jugadores.update(dict(zip(df_jugador_actualizado['nombre'], df_jugador_actualizado['id_jugador'])))
                id_jugador = mapa_jugadores.get(nombre_jugador) 
            
            
            
            print(f"Scrapeando a {nombre_jugador} {id_jugador_sofascore}")
            try:
                minutos_jugados = data['home']['players'][i]['statistics']['minutesPlayed']
                print(f"{nombre_jugador} jugo {minutos_jugados} minutos en el partido")
            except KeyError:
                print(f"{nombre_jugador} no jugo en el partido")
                minutos_jugados = 0
            
            if minutos_jugados > 2:
                insert_estadistica_partido_jugador(data['home']['players'][i], engine, id_sofascore, posicion, tarjeta_amarilla, tarjeta_roja, nombre_jugador)
                print(f"Estadistica insertada de {nombre_jugador} en el partido {id_partido}")
                new_url = f"api/v1/event/{id_sofascore}/player/{id_jugador_sofascore}/heatmap"
                print("Entrando a mapa de calor...")
                try: 
                    mapa_de_calor = get_sofascore_api_data(driver, path=new_url)
                    if len(mapa_de_calor['heatmap']) > 1: 
                        for i in range(len(mapa_de_calor['heatmap'])):
                            insert_mapa_de_calor_stmt = insert(tabla_mapa_de_calor).values(
                                jugador = id_jugador,
                                partido = id_partido,
                                x = mapa_de_calor['heatmap'][i]['x'],
                                y = mapa_de_calor['heatmap'][i]['y']
                            )

                            update_mapa_de_calor_stmt = insert_mapa_de_calor_stmt.on_duplicate_key_update(
                                jugador=insert_mapa_de_calor_stmt.inserted.jugador,
                                partido = insert_mapa_de_calor_stmt.inserted.partido,
                                x=insert_mapa_de_calor_stmt.inserted.x,
                                y=insert_mapa_de_calor_stmt.inserted.y
                            )
                            conn.execute(update_mapa_de_calor_stmt)
                        print(f"Mapa de calor de {nombre_jugador} insertado correctamente")
                    else: 
                        print("No hay info para este jugador, minutos insuficientes")
                except KeyError: 
                    print("No hay info")
                    
        
        
        for i in range(len(data['away']['players'])):
            posicion = data['away']['players'][i]['position']
            id_jugador_sofascore = data['away']['players'][i]['player']['id']
            if id_jugador_sofascore in lista_amarillas:
                tarjeta_amarilla = 1
                tarjeta_roja = 0
            elif id_jugador_sofascore in lista_rojas:
                tarjeta_amarilla = 0
                tarjeta_roja = 1
            elif id_jugador_sofascore in lista_amarillas_rojas:
                tarjeta_amarilla = 2
                tarjeta_roja = 1
            else: 
                tarjeta_amarilla = 0
                tarjeta_roja = 0
            
            nombre_jugador = data['away']['players'][i]['player']['name']
            id_jugador = mapa_jugadores.get(nombre_jugador, None)
            if id_jugador is None: 
                data_jugador = get_jugador(driver, id_jugador_sofascore)
                insert_jugador(engine, data_jugador, conn, tabla_jugador)
                df_jugador_actualizado = pd.read_sql('SELECT id_jugador, nombre FROM jugador', engine)
                mapa_jugadores.update(dict(zip(df_jugador_actualizado['nombre'], df_jugador_actualizado['id_jugador'])))
                id_jugador = mapa_jugadores.get(nombre_jugador) 
            
            
            print(f"Scrapeando a {nombre_jugador} {id_jugador_sofascore}")
            try:
                minutos_jugados = data['away']['players'][i]['statistics']['minutesPlayed']
                print(f"{nombre_jugador} jugo {minutos_jugados} minutos en el partido")
            except KeyError:
                print(f"{nombre_jugador} no jugo en el partido")
                minutos_jugados = 0
            
            if minutos_jugados >= 1:
                insert_estadistica_partido_jugador(data['away']['players'][i], engine, id_sofascore, posicion, tarjeta_amarilla, tarjeta_roja, nombre_jugador)
                print(f"Estadistica insertada de {nombre_jugador} en el partido {id_partido}")
                new_url = f"api/v1/event/{id_sofascore}/player/{id_jugador_sofascore}/heatmap"
                print("Entrando a mapa de calor...")
                try: 
                    mapa_de_calor = get_sofascore_api_data(driver, path=new_url)
                    if len(mapa_de_calor['heatmap']) > 1: 
                        for i in range(len(mapa_de_calor['heatmap'])):
                            insert_mapa_de_calor_stmt = insert(tabla_mapa_de_calor).values(
                                jugador = id_jugador,
                                partido = id_partido,
                                x = mapa_de_calor['heatmap'][i]['x'],
                                y = mapa_de_calor['heatmap'][i]['y']
                            )

                            update_mapa_de_calor_stmt = insert_mapa_de_calor_stmt.on_duplicate_key_update(
                                jugador=insert_mapa_de_calor_stmt.inserted.jugador,
                                partido = insert_mapa_de_calor_stmt.inserted.partido,
                                x=insert_mapa_de_calor_stmt.inserted.x,
                                y=insert_mapa_de_calor_stmt.inserted.y
                            )
                            conn.execute(update_mapa_de_calor_stmt)
                        print(f"Mapa de calor de {nombre_jugador} insertado correctamente")
                    else: 
                        print("No hay info para este jugador, minutos insuficientes")
                except KeyError: 
                    print("No hay info")       
    return 


def insert_mapa_de_disparos(engine, driver, id_sofascore):
    df_jugadores = pd.read_sql('SELECT id_jugador, nombre FROM jugador', engine)
    mapa_jugadores = dict(zip(df_jugadores['nombre'], df_jugadores['id_jugador']))
    df_partido = pd.read_sql('SELECT id_partido, url_sofascore FROM partido', engine)
    mapa_partidos = {
        extraer_id_sofascore(url): id_partido
        for id_partido, url in zip(df_partido['id_partido'], df_partido['url_sofascore'])
    }
    id_partido = mapa_partidos.get(str(id_sofascore))
    meta = MetaData()
    meta.reflect(bind=engine)
    tabla_mapa_de_disparos = meta.tables['mapa_de_disparos']
    with engine.begin() as conn:
        url = f"api/v1/event/{id_sofascore}/shotmap"
        print(f"Scrapeando a partido {id_sofascore}...")
        data = get_sofascore_api_data(driver, path=url)
        for i in range(len(data['shotmap'])):
            try: 
                xg = round(data['shotmap'][i]['xg'], 5)
            except KeyError: 
                print("xg no disponible")
                xg = None
            nombre_jugador = data['shotmap'][i]['player']['name']
            insert_mapa_de_disparos_stmt = insert(tabla_mapa_de_disparos).values(
                pitch_x = data['shotmap'][i]['playerCoordinates']['x'],
                pitch_y = data['shotmap'][i]['playerCoordinates']['y'],
                xg = xg,
                minuto = data['shotmap'][i]['time'],
                goal_mouth_y = data['shotmap'][i]['goalMouthCoordinates']['y'],
                goal_mouth_z = data['shotmap'][i]['goalMouthCoordinates']['z'],
                es_local = data['shotmap'][i]['isHome'],
                resultado = data['shotmap'][i]['shotType'],
                parte_del_cuerpo = data['shotmap'][i]['bodyPart'],
                jugador = mapa_jugadores.get(nombre_jugador),
                partido = id_partido
            )

            update_mapa_de_disparos_stmt = insert_mapa_de_disparos_stmt.on_duplicate_key_update(
                pitch_x = insert_mapa_de_disparos_stmt.inserted.pitch_x,
                pitch_y = insert_mapa_de_disparos_stmt.inserted.pitch_y,
                xg = insert_mapa_de_disparos_stmt.inserted.xg,
                minuto = insert_mapa_de_disparos_stmt.inserted.minuto,
                goal_mouth_y = insert_mapa_de_disparos_stmt.inserted.goal_mouth_y,
                goal_mouth_z = insert_mapa_de_disparos_stmt.inserted.goal_mouth_z,
                es_local = insert_mapa_de_disparos_stmt.inserted.es_local,
                resultado = insert_mapa_de_disparos_stmt.inserted.resultado,
                parte_del_cuerpo = insert_mapa_de_disparos_stmt.inserted.parte_del_cuerpo,
                jugador = insert_mapa_de_disparos_stmt.inserted.jugador,
                partido = insert_mapa_de_disparos_stmt.inserted.partido   
            )

            conn.execute(update_mapa_de_disparos_stmt)

    print(f"Mapa de diparos para {id_partido} insertado correctamente")
    return 


def buscar_jugador(nombre_fbref, lista_jugadores, mapa_jugadores, mapa_alias, df_jugadores):
    if nombre_fbref in mapa_jugadores:
        return nombre_fbref

    if nombre_fbref in mapa_alias:
        id_jugador = mapa_alias[nombre_fbref]
        return df_jugadores.loc[df_jugadores['id_jugador'] == id_jugador, 'nombre'].values[0]


    match, score, idx = process.extractOne(nombre_fbref, lista_jugadores)
    if score > 92:  
        return match
    
    return None

def insert_estadistica_jugador(engine,  ids_para_scrapear_temp, db_url):
    engine = create_engine(db_url)
    df_jugadores = pd.read_sql('SELECT id_jugador, nombre FROM jugador', engine)
    mapa_jugadores = dict(zip(df_jugadores['nombre'], df_jugadores['id_jugador'])) 
    df_alias = pd.read_sql('SELECT jugador, alias FROM alias_jugador', engine)
    mapa_alias = dict(zip(df_alias['alias'], df_alias['jugador']))
    lista_alias = df_alias['alias'].tolist()
    lista_nombres = df_jugadores['nombre'].tolist()
  
    meta = MetaData()
    meta.reflect(bind=engine)
    ligas = [
        'Premier League', 
        'La Liga', 
        'Serie A',
        'Bundesliga',
        'Ligue 1'
    ]
    tabla_estadistica_jugador = meta.tables['estadistica_jugador']
    tabla_estadistica_portero = meta.tables['estadistica_portero']
    with engine.begin() as conn:
        for i in range(len(ids_para_scrapear_temp)):
            
            iteracion = ids_para_scrapear_temp[i]
            print(f"Scrapeando temporada {iteracion-1}")
            df_all_players_stats = get_all_players_stats_fbref(id_liga_fbref=iteracion-1)      
            df_pochoclo = df_all_players_stats[0]
            df_porteros = df_all_players_stats[1]
            df_pochoclo.columns.values[17] = "shooting_FK"
            df_pochoclo.columns.values[34] = "pases_cmp"
            df_pochoclo.columns.values[35] = "pases_intentados"
            df_pochoclo.columns.values[36] = "pases_porcentaje_efectividad"
            df_pochoclo.columns.values[39] = "pases_cortos_cmp"
            df_pochoclo.columns.values[40] = "pases_cortos_intentados"
            df_pochoclo.columns.values[41] = "pases_cortos_porcentaje_efectividad"
            df_pochoclo.columns.values[42] = "pases_medios_cmp"
            df_pochoclo.columns.values[43] = "pases_medios_intentados"
            df_pochoclo.columns.values[44] = "pases_medios_porcentaje_efectividad"
            df_pochoclo.columns.values[45] = "pases_largos_cmp"
            df_pochoclo.columns.values[46] = "pases_largos_intentados"
            df_pochoclo.columns.values[47] = "pases_largos_porcentaje_efectividad"
            df_pochoclo.columns.values[66] = "pases_intentados"
            df_pochoclo.columns.values[69] = "passing_FK"
            df_pochoclo.columns.values[115] = "entradas"
            df_pochoclo.columns.values[120] = "entradas_regates"
            df_pochoclo = df_pochoclo.loc[:,~df_pochoclo.columns.duplicated()].copy()
            df_porteros_chulo = df_porteros[['Player', 'MP', 'Min', 'GA', 'GA90', 'SoTA', 'Saves', 'Save%', 'CS', 'PKatt', 'PKsv', 'Att (GK)', 'Opp', 'Stp', '#OPA']]
            df_jugadores_no_porteros = df_pochoclo[df_pochoclo['Pos'] != 'GK']
            df_jugadores_no_porteros['passing_FK'] = pd.to_numeric(df_jugadores_no_porteros['passing_FK'], errors='coerce')
            df_jugadores_no_porteros['shooting_FK'] = pd.to_numeric(df_jugadores_no_porteros['shooting_FK'], errors='coerce')
            df_jugadores_no_porteros['FK'] = df_jugadores_no_porteros['passing_FK'] + df_jugadores_no_porteros['shooting_FK']
            df_chulo = df_jugadores_no_porteros[['Player', 'passing_FK','shooting_FK','pases_cmp','entradas','pases_porcentaje_efectividad','Gls', 'CrdY', 'CrdR', '2CrdY',
                                      'MP', 'Ast', 'Min', 'Sh', 
                                      'SoT', 'Sh/90', 'G/Sh', 'Dist', 
                                      'PK', 'GCA','PrgP', 'PrgC', 
                                      'Succ', 'Touches', 'Int', 
                                      'Blocks', 'Clr', 'Won', 'npxG']]
            df_chulo.fillna(0, inplace=True)
            df_porteros_chulo.fillna(0, inplace=True)

            for i in range(len(df_chulo)):
                nombre_fbref = df_chulo['Player'].iloc[i]
                nombre_match = buscar_jugador(nombre_fbref, lista_nombres, mapa_jugadores, mapa_alias, df_jugadores)
                fk = df_chulo['passing_FK'].iloc[i] + df_chulo['shooting_FK'].iloc[i]
                if nombre_match:
                    jugador_id = mapa_jugadores[nombre_match]
                    insert_estadistica_jugador_stmt = insert(tabla_estadistica_jugador).values(
                        npxg = df_chulo['npxG'].iloc[i],
                        goles = df_chulo['Gls'].iloc[i],
                        tarjetas_amarillas = df_chulo['CrdY'].iloc[i],
                        tarjetas_rojas = df_chulo['CrdR'].iloc[i],
                        tarjetas_amarilla_roja = df_chulo['2CrdY'].iloc[i],
                        partidos_jugados = df_chulo['MP'].iloc[i],
                        asistencias = df_chulo['Ast'].iloc[i],
                        pases = df_chulo['pases_cmp'].iloc[i],
                        minutos_jugados = df_chulo['Min'].iloc[i],
                        disparos = df_chulo['Sh'].iloc[i],
                        disparos_a_puerta = df_chulo['SoT'].iloc[i],
                        disparos_por_90 = df_chulo['Sh/90'].iloc[i],
                        goles_por_disparo = df_chulo['G/Sh'].iloc[i],
                        distancia_promedio_de_disparos = df_chulo['Dist'].iloc[i],
                        tiros_libres = fk,
                        penales = df_chulo['PK'].iloc[i],
                        acciones_creadas = df_chulo['GCA'].iloc[i],
                        porcentaje_de_efectividad_de_pases = df_chulo['pases_porcentaje_efectividad'].iloc[i],
                        pases_progresivos = df_chulo['PrgP'].iloc[i],
                        acarreos_progresivos = df_chulo['PrgC'].iloc[i],
                        regates_efectivos = df_chulo['Succ'].iloc[i],
                        toques_de_balon = df_chulo['Touches'].iloc[i],
                        entradas = df_chulo['entradas'].iloc[i],
                        intercepciones = df_chulo['Int'].iloc[i],
                        bloqueos = df_chulo['Blocks'].iloc[i],
                        despejes = df_chulo['Clr'].iloc[i],
                        duelos_aereos_ganados = df_chulo['Won'].iloc[i],
                        jugador = jugador_id,
                        temporada = iteracion
                    )

                    update_estadistica_jugador_stmt = insert_estadistica_jugador_stmt.on_duplicate_key_update(
                        npxg = insert_estadistica_jugador_stmt.inserted.npxg,
                        goles = insert_estadistica_jugador_stmt.inserted.goles, 
                        tarjetas_amarillas = insert_estadistica_jugador_stmt.inserted.tarjetas_amarillas, 
                        tarjetas_rojas = insert_estadistica_jugador_stmt.inserted.tarjetas_rojas, 
                        tarjetas_amarilla_roja = insert_estadistica_jugador_stmt.inserted.tarjetas_amarilla_roja, 
                        partidos_jugados = insert_estadistica_jugador_stmt.inserted.partidos_jugados, 
                        asistencias = insert_estadistica_jugador_stmt.inserted.asistencias, 
                        pases = insert_estadistica_jugador_stmt.inserted.pases, 
                        minutos_jugados = insert_estadistica_jugador_stmt.inserted.minutos_jugados, 
                        disparos = insert_estadistica_jugador_stmt.inserted.disparos, 
                        disparos_a_puerta = insert_estadistica_jugador_stmt.inserted.disparos_a_puerta, 
                        disparos_por_90 = insert_estadistica_jugador_stmt.inserted.disparos_por_90, 
                        goles_por_disparo = insert_estadistica_jugador_stmt.inserted.goles_por_disparo, 
                        distancia_promedio_de_disparos = insert_estadistica_jugador_stmt.inserted.distancia_promedio_de_disparos, 
                        tiros_libres = insert_estadistica_jugador_stmt.inserted.tiros_libres, 
                        penales = insert_estadistica_jugador_stmt.inserted.penales, 
                        acciones_creadas = insert_estadistica_jugador_stmt.inserted.acciones_creadas, 
                        porcentaje_de_efectividad_de_pases = insert_estadistica_jugador_stmt.inserted.porcentaje_de_efectividad_de_pases, 
                        pases_progresivos = insert_estadistica_jugador_stmt.inserted.pases_progresivos, 
                        acarreos_progresivos = insert_estadistica_jugador_stmt.inserted.acarreos_progresivos, 
                        regates_efectivos = insert_estadistica_jugador_stmt.inserted.regates_efectivos,   
                        toques_de_balon = insert_estadistica_jugador_stmt.inserted.toques_de_balon,   
                        entradas = insert_estadistica_jugador_stmt.inserted.entradas,   
                        intercepciones = insert_estadistica_jugador_stmt.inserted.intercepciones,   
                        bloqueos = insert_estadistica_jugador_stmt.inserted.bloqueos,     
                        despejes = insert_estadistica_jugador_stmt.inserted.despejes,   
                        duelos_aereos_ganados = insert_estadistica_jugador_stmt.inserted.duelos_aereos_ganados,   
                        jugador = insert_estadistica_jugador_stmt.inserted.jugador,   
                        temporada = insert_estadistica_jugador_stmt.inserted.temporada
                    )
                    print(f"Jugador {df_chulo['Player'].iloc[i]} insertado correctamente como {nombre_match}")
                    conn.execute(update_estadistica_jugador_stmt)
       
                else:
                    jugador_id = None
                    print(f"No se pudo agregar el nombre de {nombre_fbref}")
                    
                
            for i in range(len(df_porteros_chulo)):
                nombre_fbref = df_porteros_chulo['Player'].iloc[i]
                nombre_match = buscar_jugador(nombre_fbref, lista_nombres, mapa_jugadores, mapa_alias, df_jugadores)
                
                if nombre_match:
                    jugador_id = mapa_jugadores[nombre_match]
                    insert_estadistica_portero_stmt = insert(tabla_estadistica_portero).values(
                        partidos_jugados = df_porteros_chulo['MP'].iloc[i],
                        minutos = df_porteros_chulo['Min'].iloc[i],
                        goles_recibidos = df_porteros_chulo['GA'].iloc[i],
                        goles_recibidos_por_90 = df_porteros_chulo['GA90'].iloc[i],
                        tiros_a_puerta_recibidos = df_porteros_chulo['SoTA'].iloc[i],
                        atajadas = df_porteros_chulo['Saves'].iloc[i],
                        porcentaje_de_atajadas = df_porteros_chulo['Save%'].iloc[i],
                        porterias_imbatidas = df_porteros_chulo['CS'].iloc[i],
                        penales_en_contra = df_porteros_chulo['PKatt'].iloc[i],
                        penales_atajados = df_porteros_chulo['PKsv'].iloc[i],
                        pases = df_porteros_chulo['Att (GK)'].iloc[i],
                        centros_recibidos = df_porteros_chulo['Opp'].iloc[i],
                        centros_interceptados = df_porteros_chulo['Stp'].iloc[i],
                        acciones_fuera_del_area = df_porteros_chulo['#OPA'].iloc[i],
                        jugador = jugador_id,
                        temporada = iteracion
                    )

                    update_estadistica_portero_stmt = insert_estadistica_portero_stmt.on_duplicate_key_update(
                        partidos_jugados = insert_estadistica_portero_stmt.inserted.partidos_jugados,
                        minutos = insert_estadistica_portero_stmt.inserted.minutos,
                        goles_recibidos = insert_estadistica_portero_stmt.inserted.goles_recibidos,
                        goles_recibidos_por_90 = insert_estadistica_portero_stmt.inserted.goles_recibidos_por_90,
                        tiros_a_puerta_recibidos = insert_estadistica_portero_stmt.inserted.tiros_a_puerta_recibidos,
                        atajadas = insert_estadistica_portero_stmt.inserted.atajadas,
                        porcentaje_de_atajadas = insert_estadistica_portero_stmt.inserted.porcentaje_de_atajadas,
                        porterias_imbatidas = insert_estadistica_portero_stmt.inserted.porterias_imbatidas,
                        penales_en_contra =insert_estadistica_portero_stmt.inserted.penales_en_contra,
                        penales_atajados = insert_estadistica_portero_stmt.inserted.penales_atajados,
                        pases = insert_estadistica_portero_stmt.inserted.pases,
                        centros_recibidos = insert_estadistica_portero_stmt.inserted.centros_recibidos,
                        centros_interceptados = insert_estadistica_portero_stmt.inserted.centros_interceptados,
                        acciones_fuera_del_area = insert_estadistica_portero_stmt.inserted.acciones_fuera_del_area,
                        jugador = insert_estadistica_portero_stmt.inserted.jugador,
                        temporada = insert_estadistica_portero_stmt.inserted.temporada,
                    )

                    print(f"Jugador {df_porteros_chulo['Player'].iloc[i]} insertado como {nombre_match}")
                    conn.execute(update_estadistica_portero_stmt)
                else:
                    jugador_id = None
                    print(f"No se pudo agregar correctamente a {nombre_fbref}")
                

        print(f"Liga insertada correctamente")
    return 

def update_standings_evolution_graph(liga_a_scrapear, engine,  temporada):
    df_equipo = pd.read_sql('SELECT id_equipo, nombre FROM equipo', engine)
    df_temporada = pd.read_sql('SELECT id_temporada, nombre FROM temporada', engine)
    mapa_equipo = dict(zip(df_equipo['nombre'], df_equipo['id_equipo']))
    mapa_temporada = dict(zip(df_temporada['nombre'], df_temporada['id_temporada']))
    meta = MetaData()
    meta.reflect(bind=engine)
    tabla_evoluciones = meta.tables['evolucion_posiciones'] 
    driver = init_driver()
    with engine.begin() as conn:
        print(f"Iniiciando scrapeo a {liga_a_scrapear}")
        data = get_sofascore_api_data(driver, path=f"api/v1/unique-tournament/{ligas_ids[liga_a_scrapear - 1]}/season/{temporadas_ids[liga_a_scrapear -1]}/standings/total")
        if temporada == 76986:
            id_temporada = 1
        elif temporada == 77559:
            id_temporada = 2
        elif temporada == 76457:
            id_temporada = 3
        elif temporada == 77333:
            id_temporada = 4
        else:
            id_temporada = 5
        torneo = data['standings'][0]
        for row in torneo['rows']:
            nombre_equipo_sofa = row['team']['name']
            print(f"Scrapeando a {nombre_equipo_sofa}...")
            id_equipo = mapa_equipo.get(nombre_equipo_sofa)
            id_sofa_equipo = row['team']['id']
            try:
                data_evolucion = get_sofascore_api_data(driver, path=f"api/v1/unique-tournament/{ligas_ids[liga_a_scrapear - 1]}/season/{temporadas_ids[liga_a_scrapear -1]}/team/{id_sofa_equipo}/team-performance-graph-data")
                jornadas = []
                for jornada in range(len(data_evolucion['graphData'])):
                    jornadas.append(data_evolucion['graphData'][jornada]['position'])
                
                jornadas_restates = 38 - len(jornadas)
                for i in range(jornadas_restates):
                    jornadas.append(0)

                
                insert_evolucion_equipos_stmt = insert(tabla_evoluciones).values(
                    equipo = id_equipo,
                    temporada = id_temporada,                   
                    jornada1 = jornadas[0],
                    jornada2 = jornadas[1],
                    jornada3 = jornadas[2],
                    jornada4 = jornadas[3],
                    jornada5 = jornadas[4],
                    jornada6 = jornadas[5],
                    jornada7 = jornadas[6],
                    jornada8 = jornadas[7],
                    jornada9 = jornadas[8],
                    jornada10 = jornadas[9],
                    jornada11 = jornadas[10],
                    jornada12 = jornadas[11],
                    jornada13 = jornadas[12],
                    jornada14 = jornadas[13],
                    jornada15 = jornadas[14],
                    jornada16 = jornadas[15],
                    jornada17 = jornadas[16],
                    jornada18 = jornadas[17],
                    jornada19 = jornadas[18],
                    jornada20 = jornadas[19],
                    jornada21 = jornadas[20],
                    jornada22 = jornadas[21],
                    jornada23 = jornadas[22],
                    jornada24 = jornadas[23],
                    jornada25 = jornadas[24],
                    jornada26 = jornadas[25],
                    jornada27 = jornadas[26],
                    jornada28 = jornadas[27],
                    jornada29 = jornadas[28],
                    jornada30 = jornadas[29],
                    jornada31 = jornadas[30],
                    jornada32 = jornadas[31],
                    jornada33 = jornadas[32],
                    jornada34 = jornadas[33],
                    jornada35 = jornadas[34],
                    jornada36 = jornadas[35],
                    jornada37 = jornadas[36],
                    jornada38 = jornadas[37]                        
                )
                
                update_evolucion_equipos_stmt = insert_evolucion_equipos_stmt.on_duplicate_key_update(
                    equipo = insert_evolucion_equipos_stmt.inserted.equipo,
                    temporada = insert_evolucion_equipos_stmt.inserted.temporada,
                    jornada1 = insert_evolucion_equipos_stmt.inserted.jornada1,
                    jornada2 = insert_evolucion_equipos_stmt.inserted.jornada2,
                    jornada3 = insert_evolucion_equipos_stmt.inserted.jornada3,
                    jornada4 = insert_evolucion_equipos_stmt.inserted.jornada4,
                    jornada5 = insert_evolucion_equipos_stmt.inserted.jornada5,
                    jornada6 = insert_evolucion_equipos_stmt.inserted.jornada6,
                    jornada7 = insert_evolucion_equipos_stmt.inserted.jornada7,
                    jornada8 = insert_evolucion_equipos_stmt.inserted.jornada8,
                    jornada9 = insert_evolucion_equipos_stmt.inserted.jornada9,
                    jornada10 = insert_evolucion_equipos_stmt.inserted.jornada10,
                    jornada11 = insert_evolucion_equipos_stmt.inserted.jornada11,
                    jornada12 = insert_evolucion_equipos_stmt.inserted.jornada12,
                    jornada13 = insert_evolucion_equipos_stmt.inserted.jornada13,
                    jornada14 = insert_evolucion_equipos_stmt.inserted.jornada14,
                    jornada15 = insert_evolucion_equipos_stmt.inserted.jornada15,
                    jornada16 = insert_evolucion_equipos_stmt.inserted.jornada16,
                    jornada17 = insert_evolucion_equipos_stmt.inserted.jornada17,
                    jornada18 = insert_evolucion_equipos_stmt.inserted.jornada18,
                    jornada19 = insert_evolucion_equipos_stmt.inserted.jornada19,
                    jornada20 = insert_evolucion_equipos_stmt.inserted.jornada20,
                    jornada21 = insert_evolucion_equipos_stmt.inserted.jornada21,
                    jornada22 = insert_evolucion_equipos_stmt.inserted.jornada22,
                    jornada23 = insert_evolucion_equipos_stmt.inserted.jornada23,
                    jornada24 = insert_evolucion_equipos_stmt.inserted.jornada24,
                    jornada25 = insert_evolucion_equipos_stmt.inserted.jornada25,
                    jornada26 = insert_evolucion_equipos_stmt.inserted.jornada26,
                    jornada27 = insert_evolucion_equipos_stmt.inserted.jornada27,
                    jornada28 = insert_evolucion_equipos_stmt.inserted.jornada28,
                    jornada29 = insert_evolucion_equipos_stmt.inserted.jornada29,
                    jornada30 = insert_evolucion_equipos_stmt.inserted.jornada30,
                    jornada31 = insert_evolucion_equipos_stmt.inserted.jornada31,
                    jornada32 = insert_evolucion_equipos_stmt.inserted.jornada32,
                    jornada33 = insert_evolucion_equipos_stmt.inserted.jornada33,
                    jornada34 = insert_evolucion_equipos_stmt.inserted.jornada34,
                    jornada35 = insert_evolucion_equipos_stmt.inserted.jornada35,
                    jornada36 = insert_evolucion_equipos_stmt.inserted.jornada36,
                    jornada37 = insert_evolucion_equipos_stmt.inserted.jornada37,
                    jornada38 = insert_evolucion_equipos_stmt.inserted.jornada38

                )
                conn.execute(update_evolucion_equipos_stmt)
            except KeyError:
                print("upsi dupsi no hay info")
                insert_evolucion_equipos_stmt = insert(tabla_evoluciones).values(
                    equipo = id_equipo,
                    temporada = id_temporada,                   
                    jornada1 = 0,
                    jornada2 = 0,
                    jornada3 = 0,
                    jornada4 = 0,
                    jornada5 = 0,
                    jornada6 = 0,
                    jornada7 = 0,
                    jornada8 = 0,
                    jornada9 = 0,
                    jornada10 = 0,
                    jornada11 = 0,
                    jornada12 = 0,
                    jornada13 = 0,
                    jornada14 = 0,
                    jornada15 = 0,
                    jornada16 = 0,
                    jornada17 = 0,
                    jornada18 = 0,
                    jornada19 = 0,
                    jornada20 = 0,
                    jornada21 = 0,
                    jornada22 = 0,
                    jornada23 = 0,
                    jornada24 = 0,
                    jornada25 = 0,
                    jornada26 = 0,
                    jornada27 = 0,
                    jornada28 = 0,
                    jornada29 = 0,
                    jornada30 = 0,
                    jornada31 = 0,
                    jornada32 = 0,
                    jornada34 = 0,
                    jornada35 = 0,
                    jornada36 = 0,
                    jornada37 = 0,
                    jornada38 = 0                        
                )
                
                update_evolucion_equipos_stmt = insert_evolucion_equipos_stmt.on_duplicate_key_update(
                    equipo = insert_evolucion_equipos_stmt.inserted.equipo,
                    temporada = insert_evolucion_equipos_stmt.inserted.temporada,
                    jornada1 = insert_evolucion_equipos_stmt.inserted.jornada1,
                    jornada2 = insert_evolucion_equipos_stmt.inserted.jornada2,
                    jornada3 = insert_evolucion_equipos_stmt.inserted.jornada3,
                    jornada4 = insert_evolucion_equipos_stmt.inserted.jornada4,
                    jornada5 = insert_evolucion_equipos_stmt.inserted.jornada5,
                    jornada6 = insert_evolucion_equipos_stmt.inserted.jornada6,
                    jornada7 = insert_evolucion_equipos_stmt.inserted.jornada7,
                    jornada8 = insert_evolucion_equipos_stmt.inserted.jornada8,
                    jornada9 = insert_evolucion_equipos_stmt.inserted.jornada9,
                    jornada10 = insert_evolucion_equipos_stmt.inserted.jornada10,
                    jornada11 = insert_evolucion_equipos_stmt.inserted.jornada11,
                    jornada12 = insert_evolucion_equipos_stmt.inserted.jornada12,
                    jornada13 = insert_evolucion_equipos_stmt.inserted.jornada13,
                    jornada14 = insert_evolucion_equipos_stmt.inserted.jornada14,
                    jornada15 = insert_evolucion_equipos_stmt.inserted.jornada15,
                    jornada16 = insert_evolucion_equipos_stmt.inserted.jornada16,
                    jornada17 = insert_evolucion_equipos_stmt.inserted.jornada17,
                    jornada18 = insert_evolucion_equipos_stmt.inserted.jornada18,
                    jornada19 = insert_evolucion_equipos_stmt.inserted.jornada19,
                    jornada20 = insert_evolucion_equipos_stmt.inserted.jornada20,
                    jornada21 = insert_evolucion_equipos_stmt.inserted.jornada21,
                    jornada22 = insert_evolucion_equipos_stmt.inserted.jornada22,
                    jornada23 = insert_evolucion_equipos_stmt.inserted.jornada23,
                    jornada24 = insert_evolucion_equipos_stmt.inserted.jornada24,
                    jornada25 = insert_evolucion_equipos_stmt.inserted.jornada25,
                    jornada26 = insert_evolucion_equipos_stmt.inserted.jornada26,
                    jornada27 = insert_evolucion_equipos_stmt.inserted.jornada27,
                    jornada28 = insert_evolucion_equipos_stmt.inserted.jornada28,
                    jornada29 = insert_evolucion_equipos_stmt.inserted.jornada29,
                    jornada30 = insert_evolucion_equipos_stmt.inserted.jornada30,
                    jornada31 = insert_evolucion_equipos_stmt.inserted.jornada31,
                    jornada32 = insert_evolucion_equipos_stmt.inserted.jornada32,
                    jornada33 = insert_evolucion_equipos_stmt.inserted.jornada33,
                    jornada34 = insert_evolucion_equipos_stmt.inserted.jornada34,
                    jornada35 = insert_evolucion_equipos_stmt.inserted.jornada35,
                    jornada36 = insert_evolucion_equipos_stmt.inserted.jornada36,
                    jornada37 = insert_evolucion_equipos_stmt.inserted.jornada37,
                    jornada38 = insert_evolucion_equipos_stmt.inserted.jornada38

                )
                conn.execute(update_evolucion_equipos_stmt)
    return

def ultimos_partidos(engine, temporada):
    df_partidos = pd.read_sql(f'SELECT * FROM partido WHERE temporada = {temporada}', engine)
    df_equipo = pd.read_sql('SELECT id_equipo, nombre FROM equipo', engine)
    mapa_equipo = dict(zip(df_equipo['nombre'], df_equipo['id_equipo']))
    meta = MetaData()
    meta.reflect(bind=engine)
    tabla_ultimos_enfrentamientos = meta.tables['ultimos_enfrentamientos'] 
    driver = init_driver()
    with engine.begin() as conn:
        for x in range(153):
            id_partido_sofascore = df_partidos.iloc[x]['url_sofascore'].split('/')[6].split('#')[0]
            data = get_sofascore_api_data(driver, path=f'api/v1/event/{id_partido_sofascore}/h2h/events')
            eventos_conservados = [
                evento for evento in data['events'] 
                if evento['status']['code'] == 100
            ]

            data['events'] = eventos_conservados
            
            if len(data['events']) < 5:
                for i in range(len(data['events'])):
                    fecha = data['events'][i]['startTimestamp']
                    fecha = datetime.utcfromtimestamp(fecha).strftime('%Y-%m-%d')
                    equipo_local = data['events'][i]['homeTeam']['name']
                    equipo_visitante = data['events'][i]['awayTeam']['name']
                    try:
                        goles_local = data['events'][i]['homeScore']['display']
                        goles_visitante = data['events'][i]['awayScore']['display']
                    except KeyError:
                        goles_local = 0
                        goles_visitante = 0
                    id_equipo_local = mapa_equipo.get(equipo_local)
                    id_equipo_visitante = mapa_equipo.get(equipo_visitante)
                    print(f"Partido: {df_partidos.iloc[x]['id_partido']}, {equipo_local} {goles_local} - {goles_visitante} {equipo_visitante}, {fecha}")
                    print("Insertando...")
                    insert_evolucion_equipos_stmt = insert(tabla_ultimos_enfrentamientos).values(
                        partido = df_partidos.iloc[x]['id_partido'],
                        fecha = fecha,
                        equipo_local = id_equipo_local,
                        equipo_visitante = id_equipo_visitante,
                        goles_local = goles_local,
                        goles_visitante = goles_visitante
                    )
                    conn.execute(insert_evolucion_equipos_stmt)
            else:
                for i in range(5):
                    fecha = data['events'][i]['startTimestamp']
                    fecha = datetime.utcfromtimestamp(fecha).strftime('%Y-%m-%d')
                    equipo_local = data['events'][i]['homeTeam']['name']
                    equipo_visitante = data['events'][i]['awayTeam']['name']
                    try:
                        goles_local = data['events'][i]['homeScore']['display']
                        goles_visitante = data['events'][i]['awayScore']['display']
                    except KeyError:
                        goles_local = 0
                        goles_visitante = 0
                    id_equipo_local = mapa_equipo.get(equipo_local)
                    id_equipo_visitante = mapa_equipo.get(equipo_visitante)
                    print(f"Partido: {df_partidos.iloc[x]['id_partido']}, {equipo_local} {goles_local} - {goles_visitante} {equipo_visitante}, {fecha}")
                    print("Insertando...")
                    insert_evolucion_equipos_stmt = insert(tabla_ultimos_enfrentamientos).values(
                        partido = df_partidos.iloc[x]['id_partido'],
                        fecha = fecha,
                        equipo_local = id_equipo_local,
                        equipo_visitante = id_equipo_visitante,
                        goles_local = goles_local,
                        goles_visitante = goles_visitante
                    )
                    conn.execute(insert_evolucion_equipos_stmt)
            
    return 



def update_fecha_partidos(engine, temporada, jornada):
    df_partidos = pd.read_sql(f"SELECT * FROM partido WHERE temporada = {temporada} AND jornada = {jornada}", engine)    
    meta = MetaData()
    meta.reflect(bind=engine)
    table_partidos = meta.tables['partido'] 
    driver = init_driver()
    with engine.begin() as conn:
        for i in range(len(df_partidos)):
            event_id = df_partidos.iloc[i]['url_sofascore'].split(':')[2]
            data = get_sofascore_api_data(driver, path=f'api/v1/event/{event_id}')
            try: 

                fecha = data['event']['startTimestamp']
                fecha = datetime.utcfromtimestamp(fecha).strftime('%Y-%m-%d')
                if fecha != df_partidos.iloc[i]['fecha']:
                    insert_partido_stmt = insert(table_partidos).values(
                        id_partido = df_partidos.iloc[i]['id_partido'],
                        fecha = fecha 
                        
                    )

                    update_partido_stmt = insert_partido_stmt.on_duplicate_key_update(
                        fecha = insert_partido_stmt.inserted.fecha
                    )

                    conn.execute(update_partido_stmt)
                    print(f"Fecha actualizada del partido {df_partidos.iloc[i]['id_partido']}")
                else:
                    print("Fecha igual")
            except KeyError:
                print("Patatas")
            
    return


def prematch_odds(engine, partido):
    meta = MetaData()
    meta.reflect(bind=engine)
    table_partidos = meta.tables['partido'] 
    driver = init_driver()
    with engine.begin() as conn:    
        event_id = partido['url_sofascore'].split(':')[2]
        try:  
            data = get_sofascore_api_data(driver, path=f'api/v1/event/{event_id}/odds/1/all')
            numerador_local = data['markets'][0]['choices'][0]['fractionalValue'].split('/')[0]
            denominador_local = data['markets'][0]['choices'][0]['fractionalValue'].split('/')[1]
            momio_local = (int(numerador_local) / int(denominador_local)) + 1
            numerador_empate = data['markets'][0]['choices'][1]['fractionalValue'].split('/')[0]
            denominador_empate = data['markets'][0]['choices'][1]['fractionalValue'].split('/')[1]
            momio_empate = (int(numerador_empate) / int(denominador_empate)) + 1
            numerador_visitante = data['markets'][0]['choices'][2]['fractionalValue'].split('/')[0]
            denominador_visitante = data['markets'][0]['choices'][2]['fractionalValue'].split('/')[1]
            momio_visitante = (int(numerador_visitante) / int(denominador_visitante)) + 1


            insert_odds_stmt = insert(table_partidos).values(
                id_partido = partido['id_partido'],
                momio_local = momio_local,
                momio_empate = momio_empate,
                momio_visitante = momio_visitante
            )

            update_odds_stmt = insert_odds_stmt.on_duplicate_key_update(
                momio_local = insert_odds_stmt.inserted.momio_local,
                momio_empate = insert_odds_stmt.inserted.momio_empate,
                momio_visitante = insert_odds_stmt.inserted.momio_visitante
            )
            conn.execute(update_odds_stmt)
            print(f"Momios insertados para partido {partido['id_partido']}")
        except KeyError: 
            print("Momios no disponibles")

    return

def postmatch_odss(engine, partido):
    meta = MetaData()
    meta.reflect(bind=engine)
    table_partidos = meta.tables['partido'] 
    driver = init_driver()
    with engine.begin() as conn:
        event_id = partido['url_sofascore'].split(':')[2]
        try: 
            odd_ganador = 0
            data = get_sofascore_api_data(driver, path=f'api/v1/event/{event_id}/odds/1/all')
            for i in range(3):
                resultado = data['markets'][0]['choices'][i]['winning']
                if resultado == 1:
                    odd_ganador = data['markets'][0]['choices'][i]['name']

            insert_odds_stmt = insert(table_partidos).values(
                id_partido = partido['id_partido'],
                momio_ganador = odd_ganador
                
            )

            update_odds_stmt = insert_odds_stmt.on_duplicate_key_update(
                momio_ganador = insert_odds_stmt.inserted.momio_ganador
            )
            conn.execute(update_odds_stmt)
            print(f"Momios insertados para partido {partido['id_partido']}")
        except KeyError: 
            print("Momios no disponibles")
    return


def pending_odds(engine, partidos):
    for i in range(len(partidos)):
        prematch_odds(engine, partidos.iloc[i])
        postmatch_odss(engine, partidos.iloc[i])

    return


def insert_confirmed_lineups(engine, partido):
    meta = MetaData()
    meta.reflect(bind=engine)
    df_alineaciones = pd.read_sql(f'SELECT id_alineacion FROM alineaciones WHERE partido = {partido['id_partido']}', engine) 
    df_jugadores = pd.read_sql('SELECT id_jugador, nombre FROM jugador', engine)
    mapa_jugadores = dict(zip(df_jugadores['nombre'], df_jugadores['id_jugador']))
    table_partidos = meta.tables['alineaciones'] 
    driver = init_driver()
    with engine.begin() as conn:    
        event_id = partido['url_sofascore'].split(':')[2]
        try: 
            data = get_sofascore_api_data(driver, path=f'api/v1/event/{event_id}/lineups')
            nombre_jugador1 = data['home']['players'][0]['player']['name']
            nombre_jugador2 = data['home']['players'][1]['player']['name']
            nombre_jugador3 = data['home']['players'][2]['player']['name']
            nombre_jugador4 = data['home']['players'][3]['player']['name']
            nombre_jugador5 = data['home']['players'][4]['player']['name']
            nombre_jugador6 = data['home']['players'][5]['player']['name']
            nombre_jugador7 = data['home']['players'][6]['player']['name']
            nombre_jugador8 = data['home']['players'][7]['player']['name']
            nombre_jugador9 = data['home']['players'][8]['player']['name']
            nombre_jugador10 = data['home']['players'][9]['player']['name']
            nombre_jugador11 = data['home']['players'][10]['player']['name']
            nombre_jugador12 = data['away']['players'][0]['player']['name']
            nombre_jugador13 = data['away']['players'][1]['player']['name']
            nombre_jugador14 = data['away']['players'][2]['player']['name']
            nombre_jugador15 = data['away']['players'][3]['player']['name']
            nombre_jugador16 = data['away']['players'][4]['player']['name']
            nombre_jugador17 = data['away']['players'][5]['player']['name']
            nombre_jugador18 = data['away']['players'][6]['player']['name']
            nombre_jugador19 = data['away']['players'][7]['player']['name']
            nombre_jugador20 = data['away']['players'][8]['player']['name']
            nombre_jugador21 = data['away']['players'][9]['player']['name']
            nombre_jugador22 = data['away']['players'][10]['player']['name']

            insert_alineaciones_stmt = insert(table_partidos).values(
                #id_alineacion = int(df_alineaciones['id_alineacion']),
                partido = partido['id_partido'],
                formacion_local = data['home']['formation'],
                formacion_visitante = data['away']['formation'],
                jugador1 = mapa_jugadores.get(nombre_jugador1, None),
                jugador2 = mapa_jugadores.get(nombre_jugador2, None),
                jugador3 = mapa_jugadores.get(nombre_jugador3, None),
                jugador4 = mapa_jugadores.get(nombre_jugador4, None),
                jugador5 = mapa_jugadores.get(nombre_jugador5, None),
                jugador6 = mapa_jugadores.get(nombre_jugador6, None),
                jugador7 = mapa_jugadores.get(nombre_jugador7, None),
                jugador8 = mapa_jugadores.get(nombre_jugador8, None),
                jugador9 = mapa_jugadores.get(nombre_jugador9, None),
                jugador10 = mapa_jugadores.get(nombre_jugador10, None),
                jugador11 = mapa_jugadores.get(nombre_jugador11, None),
                jugador12 = mapa_jugadores.get(nombre_jugador12, None),
                jugador13 = mapa_jugadores.get(nombre_jugador13, None),
                jugador14 = mapa_jugadores.get(nombre_jugador14, None),
                jugador15 = mapa_jugadores.get(nombre_jugador15, None),
                jugador16 = mapa_jugadores.get(nombre_jugador16, None),
                jugador17 = mapa_jugadores.get(nombre_jugador17, None),
                jugador18 = mapa_jugadores.get(nombre_jugador18, None),
                jugador19 = mapa_jugadores.get(nombre_jugador19, None),
                jugador20 = mapa_jugadores.get(nombre_jugador20, None),
                jugador21 = mapa_jugadores.get(nombre_jugador21, None),
                jugador22 = mapa_jugadores.get(nombre_jugador22, None),
                esConfirmada = 1 
            )

            update_alineaciones_stmt = insert_alineaciones_stmt.on_duplicate_key_update(
                partido = insert_alineaciones_stmt.inserted.partido,
                formacion_local = insert_alineaciones_stmt.inserted.formacion_local,
                formacion_visitante = insert_alineaciones_stmt.inserted.formacion_visitante,
                jugador1 = insert_alineaciones_stmt.inserted.jugador1,
                jugador2 = insert_alineaciones_stmt.inserted.jugador2,
                jugador3 = insert_alineaciones_stmt.inserted.jugador3,
                jugador4 = insert_alineaciones_stmt.inserted.jugador4,
                jugador5 = insert_alineaciones_stmt.inserted.jugador5,
                jugador6 = insert_alineaciones_stmt.inserted.jugador6,
                jugador7 = insert_alineaciones_stmt.inserted.jugador7,
                jugador8 = insert_alineaciones_stmt.inserted.jugador8,
                jugador9 = insert_alineaciones_stmt.inserted.jugador9,
                jugador10 = insert_alineaciones_stmt.inserted.jugador10,
                jugador11 = insert_alineaciones_stmt.inserted.jugador11,
                jugador12 = insert_alineaciones_stmt.inserted.jugador12,
                jugador13 = insert_alineaciones_stmt.inserted.jugador13,
                jugador14 = insert_alineaciones_stmt.inserted.jugador14,
                jugador15 = insert_alineaciones_stmt.inserted.jugador15,
                jugador16 = insert_alineaciones_stmt.inserted.jugador16,
                jugador17 = insert_alineaciones_stmt.inserted.jugador17,
                jugador18 = insert_alineaciones_stmt.inserted.jugador18,
                jugador19 = insert_alineaciones_stmt.inserted.jugador19,
                jugador20 = insert_alineaciones_stmt.inserted.jugador20,
                jugador21 = insert_alineaciones_stmt.inserted.jugador21,
                jugador22 = insert_alineaciones_stmt.inserted.jugador22,
                esConfirmada = insert_alineaciones_stmt.inserted.esConfirmada
            )
            conn.execute(update_alineaciones_stmt)
            print(f"Alineaciones confirmadas insertados para partido {partido['id_partido']}")
        except KeyError: 
            print("Alineaciones no disponibles")
    return


def insert_predicted_lineups(engine, partido):
    meta = MetaData()
    meta.reflect(bind=engine)
    df_jugadores = pd.read_sql('SELECT id_jugador, nombre FROM jugador', engine)
    mapa_jugadores = dict(zip(df_jugadores['nombre'], df_jugadores['id_jugador']))
    table_partidos = meta.tables['alineaciones'] 
    driver = init_driver()
    with engine.begin() as conn:    
        event_id = partido['url_sofascore'].split(':')[2]
        try: 
            data = get_sofascore_api_data(driver, path=f'api/v1/event/{event_id}/lineups')
            nombre_jugador1 = data['home']['players'][0]['player']['name']
            nombre_jugador2 = data['home']['players'][1]['player']['name']
            nombre_jugador3 = data['home']['players'][2]['player']['name']
            nombre_jugador4 = data['home']['players'][3]['player']['name']
            nombre_jugador5 = data['home']['players'][4]['player']['name']
            nombre_jugador6 = data['home']['players'][5]['player']['name']
            nombre_jugador7 = data['home']['players'][6]['player']['name']
            nombre_jugador8 = data['home']['players'][7]['player']['name']
            nombre_jugador9 = data['home']['players'][8]['player']['name']
            nombre_jugador10 = data['home']['players'][9]['player']['name']
            nombre_jugador11 = data['home']['players'][10]['player']['name']
            nombre_jugador12 = data['away']['players'][0]['player']['name']
            nombre_jugador13 = data['away']['players'][1]['player']['name']
            nombre_jugador14 = data['away']['players'][2]['player']['name']
            nombre_jugador15 = data['away']['players'][3]['player']['name']
            nombre_jugador16 = data['away']['players'][4]['player']['name']
            nombre_jugador17 = data['away']['players'][5]['player']['name']
            nombre_jugador18 = data['away']['players'][6]['player']['name']
            nombre_jugador19 = data['away']['players'][7]['player']['name']
            nombre_jugador20 = data['away']['players'][8]['player']['name']
            nombre_jugador21 = data['away']['players'][9]['player']['name']
            nombre_jugador22 = data['away']['players'][10]['player']['name']

            insert_alineaciones_stmt = insert(table_partidos).values(
                
                partido = partido['id_partido'],
                formacion_local = data['home']['formation'],
                formacion_visitante = data['away']['formation'],
                jugador1 = mapa_jugadores.get(nombre_jugador1, None),
                jugador2 = mapa_jugadores.get(nombre_jugador2, None),
                jugador3 = mapa_jugadores.get(nombre_jugador3, None),
                jugador4 = mapa_jugadores.get(nombre_jugador4, None),
                jugador5 = mapa_jugadores.get(nombre_jugador5, None),
                jugador6 = mapa_jugadores.get(nombre_jugador6, None),
                jugador7 = mapa_jugadores.get(nombre_jugador7, None),
                jugador8 = mapa_jugadores.get(nombre_jugador8, None),
                jugador9 = mapa_jugadores.get(nombre_jugador9, None),
                jugador10 = mapa_jugadores.get(nombre_jugador10, None),
                jugador11 = mapa_jugadores.get(nombre_jugador11, None),
                jugador12 = mapa_jugadores.get(nombre_jugador12, None),
                jugador13 = mapa_jugadores.get(nombre_jugador13, None),
                jugador14 = mapa_jugadores.get(nombre_jugador14, None),
                jugador15 = mapa_jugadores.get(nombre_jugador15, None),
                jugador16 = mapa_jugadores.get(nombre_jugador16, None),
                jugador17 = mapa_jugadores.get(nombre_jugador17, None),
                jugador18 = mapa_jugadores.get(nombre_jugador18, None),
                jugador19 = mapa_jugadores.get(nombre_jugador19, None),
                jugador20 = mapa_jugadores.get(nombre_jugador20, None),
                jugador21 = mapa_jugadores.get(nombre_jugador21, None),
                jugador22 = mapa_jugadores.get(nombre_jugador22, None),
                esConfirmada = 0 
            )

            update_alineaciones_stmt = insert_alineaciones_stmt.on_duplicate_key_update(
                partido = insert_alineaciones_stmt.inserted.partido,
                formacion_local = insert_alineaciones_stmt.inserted.formacion_local,
                formacion_visitante = insert_alineaciones_stmt.inserted.formacion_visitante,
                jugador1 = insert_alineaciones_stmt.inserted.jugador1,
                jugador2 = insert_alineaciones_stmt.inserted.jugador2,
                jugador3 = insert_alineaciones_stmt.inserted.jugador3,
                jugador4 = insert_alineaciones_stmt.inserted.jugador4,
                jugador5 = insert_alineaciones_stmt.inserted.jugador5,
                jugador6 = insert_alineaciones_stmt.inserted.jugador6,
                jugador7 = insert_alineaciones_stmt.inserted.jugador7,
                jugador8 = insert_alineaciones_stmt.inserted.jugador8,
                jugador9 = insert_alineaciones_stmt.inserted.jugador9,
                jugador10 = insert_alineaciones_stmt.inserted.jugador10,
                jugador11 = insert_alineaciones_stmt.inserted.jugador11,
                jugador12 = insert_alineaciones_stmt.inserted.jugador12,
                jugador13 = insert_alineaciones_stmt.inserted.jugador13,
                jugador14 = insert_alineaciones_stmt.inserted.jugador14,
                jugador15 = insert_alineaciones_stmt.inserted.jugador15,
                jugador16 = insert_alineaciones_stmt.inserted.jugador16,
                jugador17 = insert_alineaciones_stmt.inserted.jugador17,
                jugador18 = insert_alineaciones_stmt.inserted.jugador18,
                jugador19 = insert_alineaciones_stmt.inserted.jugador19,
                jugador20 = insert_alineaciones_stmt.inserted.jugador20,
                jugador21 = insert_alineaciones_stmt.inserted.jugador21,
                jugador22 = insert_alineaciones_stmt.inserted.jugador22,
                esConfirmada = insert_alineaciones_stmt.inserted.esConfirmada
            )
            conn.execute(update_alineaciones_stmt)
            print(f"Alineaciones posibles insertados para partido {partido['id_partido']}")
        except KeyError: 
            print("Alineaciones no disponibles")
    return


def prematch_ref(engine, partido):
    df_arbitros = pd.read_sql('SELECT id_arbitro, nombre FROM arbitro', engine)
    mapa_arbitros = dict(zip(df_arbitros['nombre'], df_arbitros['id_arbitro']))
    meta = MetaData()
    meta.reflect(bind=engine)
    table_partidos = meta.tables['partido'] 
    tabla_arbitro = meta.tables['arbitro']
    driver = init_driver()
    with engine.begin() as conn:    
        event_id = partido['url_sofascore'].split(':')[2]
        try: 
            data = get_sofascore_api_data(driver, path=f'api/v1/event/{event_id}')
            arbitro_data = data['event']['referee']
            arbitro_nombre = data['event']['referee']['name']
            id_arbitro = mapa_arbitros.get(arbitro_nombre, None)
            if id_arbitro is None:
                print(f"Insertando a {arbitro_nombre}...")
                insert_arbitro(engine, arbitro_data, tabla_arbitro, conn)
                print(f"{arbitro_nombre} insertado correctamente")
                df_arbitro_actualizado = pd.read_sql('SELECT id_arbitro, nombre FROM arbitro', engine)
                mapa_arbitros.update(dict(zip(df_arbitro_actualizado['nombre'], df_arbitro_actualizado['id_arbitro'])))
                id_arbitro = mapa_arbitros.get(arbitro_nombre)

            insert_arbitro_stmt = insert(table_partidos).values(
                id_partido = partido['id_partido'],
                arbitro = id_arbitro
                
            )

            update_arbitro_stmt = insert_arbitro_stmt.on_duplicate_key_update(
                arbitro = insert_arbitro_stmt.inserted.arbitro
                
            )
            
            conn.execute(update_arbitro_stmt)
            print(f"Arbitro insertados para partido {partido['id_partido']}")
        except KeyError: 
            print("Arbitro no disponibles")

    return


def extract_img_url(engine, df, driver):
    meta = MetaData()
    meta.reflect(bind=engine)
    arreglo_url = []
    for i in range(len(df)):
        url_img = df.iloc[i]['url_imagen']
        nombre_equipo = df.iloc[i]['nombre']
        cloudinary_public_id = f"teams/team_{nombre_equipo}"
        print(f"Procesando imagen para el equipo {nombre_equipo}...")
        new_cloudinary_url = upload_image_from_url(driver, url_img, cloudinary_public_id)
        final_image_url = new_cloudinary_url if new_cloudinary_url else "URL_DE_IMAGEN_POR_DEFECTO.PNG"
        print(f"Resultado para {nombre_equipo}: {final_image_url}")
        
        arreglo_url.append(final_image_url)
        
    return arreglo_url


def transfer_to_database(engine, arreglo_url, df):
    meta = MetaData()
    meta.reflect(bind=engine)
    tabla_entrenador = meta.tables['jugador']

    with engine.begin() as conn: 
        for i in range(len(arreglo_url)):
            insert_url_stmt = insert(tabla_entrenador).values(
                id_jugador = df.iloc[i]['id_jugador'],
                url_imagen = arreglo_url[i]
            )

            update_url_stmt = insert_url_stmt.on_duplicate_key_update(
                url_imagen = insert_url_stmt.inserted.url_imagen
            )

            conn.execute(update_url_stmt)
            print("Url actualizada exitosamente")

    return 



def procesar_puntos_partido(engine, id_partido):

    """
    Procesa todos los puntos fantasy de un partido completo en lote,
    evitando consultas N+1 y usando vectorización.
    """
    meta = MetaData()
    meta.reflect(bind=engine)
    tabla_puntos = meta.tables['puntos_jugador_jornada']

    # --- 1. OBTENER DATOS (3 Consultas en total) ---

    # Obtener info del partido (1 consulta)
    df_partido = pd.read_sql(f'SELECT jornada FROM partido WHERE id_partido = {id_partido}', engine)
    
    if df_partido.empty:
        print(f"Partido {id_partido} no encontrado. Saltando.")
        return
    jornada = df_partido['jornada'].iloc[0]

    # Obtener TODOS los jugadores de campo con sus stats (1 consulta)
    sql_jugadores = f"""
        SELECT j.id_jugador, j.posicion, s.* FROM estadistica_jugador_partido s
        JOIN jugador j ON s.jugador = j.id_jugador
        WHERE s.partido = {id_partido}
    """
    df_jugadores = pd.read_sql(sql_jugadores, engine)
    print(f"Hubo {len(df_jugadores)} jugadores de campo en el partido")

    # Obtener TODOS los porteros con sus stats (1 consulta)
    sql_porteros = f"""
        SELECT j.id_jugador, j.posicion, s.* FROM estadistica_jugador_portero s
        JOIN jugador j ON s.jugador = j.id_jugador
        WHERE s.partido = {id_partido}
    """
    df_porteros = pd.read_sql(sql_porteros, engine)
    print(f"Hubo {len(df_porteros)} porteros en el partido")

    # --- 2. CALCULAR MVP (Una sola vez, en memoria) ---
    
    # Concatenar ratings de ambos dataframes
    ratings_jug = df_jugadores[['id_jugador', 'rating']] if not df_jugadores.empty else pd.DataFrame(columns=['id_jugador', 'rating'])
    ratings_por = df_porteros[['id_jugador', 'rating']] if not df_porteros.empty else pd.DataFrame(columns=['id_jugador', 'rating'])
    all_ratings = pd.concat([ratings_jug, ratings_por], ignore_index=True)

    id_mvp = None
    if not all_ratings.empty:
        # Encontrar el id_jugador con el rating máximo
        id_mvp = all_ratings.loc[all_ratings['rating'].idxmax()]['id_jugador']
        print(f"El mvp del partido fue: {id_mvp}")

    # Lista para guardar todos los datos a insertar
    data_para_insertar = []

    # --- 3. PROCESAR JUGADORES (Vectorizado) ---
    if not df_jugadores.empty:
        # Empezar con una Serie de ceros
        puntos = pd.Series(0, index=df_jugadores.index, dtype=int)

        # Minutos
        minutos_jug = df_jugadores['minutos'].fillna(0)
        puntos += (minutos_jug > 59) * 2
        puntos += ((minutos_jug > 0) & (minutos_jug <= 59)) * 1

        # Goles (usando np.select para lógica condicional)
        goles_jug = df_jugadores['goles'].fillna(0)
        condiciones_goles = [
            (df_jugadores['posicion'] == 'D'),
            (df_jugadores['posicion'] == 'M')
        ]
        puntos_por_gol = [
            goles_jug * 6, # Puntos para Defensas
            goles_jug * 5  # Puntos para Medios
        ]
        # El default es 4 puntos (para delanteros u otros)
        puntos += np.select(condiciones_goles, puntos_por_gol, default=goles_jug * 4)

        # Asistencias
        puntos += df_jugadores['asistencias'].fillna(0) * 3

        # Tarjetas
        puntos -= df_jugadores['tarjeta_amarilla'].fillna(0) * 1
        puntos -= df_jugadores['tarjeta_roja'].fillna(0) * 3

        # Recuperaciones
        recuperaciones = df_jugadores['intercepciones'].fillna(0) + df_jugadores['despejes'].fillna(0)
        puntos += (recuperaciones // 3) * 1  # Suma 1 por cada 3 recuperaciones

        # Autogoles
        puntos -= df_jugadores['autogoles'].fillna(0) * 3

        # MVP
        if id_mvp is not None:
            puntos += (df_jugadores['id_jugador'] == id_mvp) * 3

        # Asignar puntos finales al DataFrame
        df_jugadores['puntos_fantasy'] = puntos

        # Preparar datos para inserción en lote
        for row in df_jugadores.itertuples():
            data_para_insertar.append({
                'id_jugador': int(row.id_jugador),
                'id_partido': int(id_partido),
                'jornada': int(jornada),
                'puntos_fantasy': int(row.puntos_fantasy)
            })

    # --- 4. PROCESAR PORTEROS (Vectorizado) ---
    if not df_porteros.empty:
        puntos_p = pd.Series(0, index=df_porteros.index, dtype=int)

        # Minutos
        minutos_por = df_porteros['minutos'].fillna(0)
        puntos_p += (minutos_por > 59) * 2
        puntos_p += ((minutos_por > 0) & (minutos_por <= 59)) * 1

        # Goles
        puntos_p += df_porteros['goles'].fillna(0) * 10

        # Asistencias
        puntos_p += df_porteros['asistencias'].fillna(0) * 3
        
        # Penales atajados
        puntos_p += df_porteros['penales_atajados'].fillna(0) * 6

        # Atajadas
        puntos_p += (df_porteros['atajadas'].fillna(0) // 3) * 1 # Suma 1 por cada 3 atajadas

        # Tarjetas
        puntos_p -= df_porteros['tarjeta_amarilla'].fillna(0) * 1
        puntos_p -= df_porteros['tarjeta_roja'].fillna(0) * 3

        # MVP
        if id_mvp is not None:
            puntos_p += (df_porteros['id_jugador'] == id_mvp) * 3

        # Asignar al DF
        df_porteros['puntos_fantasy'] = puntos_p

        # Añadir a la lista de inserción
        for row in df_porteros.itertuples():
            data_para_insertar.append({
                'id_jugador': int(row.id_jugador),
                'id_partido': int(id_partido),
                'jornada': int(jornada),
                'puntos_fantasy': int(row.puntos_fantasy)
            })

    # --- 5. INSERTAR EN LOTE (Una sola transacción) ---
    if data_para_insertar:
        stmt = insert(tabla_puntos)
        
        # Define la parte ON DUPLICATE KEY UPDATE
        update_stmt = stmt.on_duplicate_key_update(
            jornada = stmt.inserted.jornada,
            puntos_fantasy = stmt.inserted.puntos_fantasy
        )
        
        # Ejecuta la inserción en lote
        with engine.begin() as conn:
            conn.execute(update_stmt, data_para_insertar)
            
        print(f"Partido {id_partido}: Insertados/Actualizados {len(data_para_insertar)} jugadores.")
    else:
        print(f"Partido {id_partido}: No se encontraron datos de jugadores para procesar.")

    return

def actualizar_valor_mercado_fantasy(engine):
    """
    Actualiza el valor de mercado de TODOS los jugadores fantasy
    basado en la fórmula: V = Vbase + (W1*Rendimiento) + (W2*Popularidad)
    """
    
    # --- 1. Definir Constantes ---
    W1_RENDIMIENTO = 0.75
    W2_POPULARIDAD = 0.25
    DIVISOR_VALOR_BASE = 15000000.0  # Usamos float para asegurar división decimal

    meta = MetaData()
    meta.reflect(bind=engine)
    tabla_valor_fantasy = meta.tables['valor_jugador_fantasy']

    # --- 2. Crear la consulta SQL para obtener TODOS los datos ---
    # Esta consulta une las 3 tablas necesarias:
    # 1. jugador (para Vbase = valor_mercado / DIVISOR)
    # 2. valor_jugador_fantasy (para Popularidad)
    # 3. puntos_jugador_jornada (para calcular AVG(puntos_fantasy) como Rendimiento)
    
    sql_query = text(f"""
        SELECT
            j.id_jugador,
            j.valor_mercado,
            COALESCE(v.popularidad, 0) as popularidad,
            COALESCE(r.rendimiento, 0) as rendimiento
        FROM
            jugador j
        LEFT JOIN
            valor_jugador_fantasy v ON j.id_jugador = v.id_jugador
        LEFT JOIN
            (
                SELECT
                    id_jugador,
                    AVG(puntos_fantasy) as rendimiento
                FROM
                    puntos_jugador_jornada
                GROUP BY
                    id_jugador
            ) r ON j.id_jugador = r.id_jugador
    """)

    # --- 3. Ejecutar consulta y cargar en Pandas ---
    df_data = pd.read_sql(sql_query, engine)
    
    if df_data.empty:
        print("No se encontraron jugadores para actualizar.")
        return
 
    # --- 4. Limpiar datos (por si acaso) ---
    # COALESCE en SQL ya debería haber hecho esto, pero es una buena práctica
    df_data['valor_mercado'] = df_data['valor_mercado'].fillna(0)
    df_data['popularidad'] = df_data['popularidad'].fillna(0)
    df_data['rendimiento'] = df_data['rendimiento'].fillna(0)

    # --- 5. Calcular el nuevo valor (Vectorizado) ---
    
    # Vbase = Valor inicial del jugador
    v_base = df_data['valor_mercado'] / DIVISOR_VALOR_BASE
    
    # Rendimiento ponderado (W1 * Rendimiento)
    rendimiento_pond = W1_RENDIMIENTO * df_data['rendimiento']
    
    # Popularidad ponderada (W2 * Popularidad)
    popularidad_pond = W2_POPULARIDAD * df_data['popularidad']
    
    # Vjugador = Vbase + (W1 * Rendimiento) + (W2 * Popularidad)
    v_jugador_nuevo = v_base + rendimiento_pond + popularidad_pond
    
    # Asignamos el valor nuevo al DataFrame
    # Redondeamos a 2 decimales, como lo pide tu tabla DECIMAL(5,2)
    df_data['valor_actual_nuevo'] = round(v_jugador_nuevo, 2)

    # --- 6. Preparar datos para la inserción en lote ---
    data_para_insertar = []
    for row in df_data.itertuples():
        # ¡Importante! Convertimos a float, no a int, 
        # para no perder los decimales de tu columna DECIMAL(5,2)
        data_para_insertar.append({
            'id_jugador': int(row.id_jugador),
            'valor_actual': float(row.valor_actual_nuevo)
            # No necesitamos pasar 'popularidad' aquí, 
            # ya que solo estamos actualizando 'valor_actual'
        }) 

    # --- 7. Ejecutar la actualización en lote ---
    if data_para_insertar:
        stmt = insert(tabla_valor_fantasy)
        
        # Define la parte ON DUPLICATE KEY UPDATE
        # Esto solo actualizará 'valor_actual' si el 'id_jugador' ya existe
        update_stmt = stmt.on_duplicate_key_update(
            valor_actual = stmt.inserted.valor_actual
        )
        
        # Ejecuta la inserción en lote
        with engine.begin() as conn:
            conn.execute(update_stmt, data_para_insertar)
            
        print(f"Valor fantasy actualizado para {len(data_para_insertar)} jugadores.")
    else:
        print("Fallo mi loco: No hubo datos para insertar.")
    
    return


def actualizar_porcentaje_popularidad(engine):
    
    df_plantillas_fantasy = pd.read_sql("SELECT id_jugador FROM plantilla_fantasy", engine)

    if df_plantillas_fantasy.empty:
        print("No hay datos en plantilla_fantasy.")
        return

    meta = MetaData()
    meta.reflect(bind=engine)
    tabla_valor_fantasy = meta.tables['valor_jugador_fantasy']

    total_equipos = len(df_plantillas_fantasy) / 15.0

    if total_equipos == 0:
        print("No se detectaron equipos, división por cero.")
        return

    
    conteo_jugadores = df_plantillas_fantasy['id_jugador'].value_counts()

    
    df_popularidad = (conteo_jugadores / total_equipos) * 100
    
    data_para_actualizar = []
    
    # Iteramos sobre la Serie (id_jugador es 'index', popularidad es 'valor')
    for id_jugador, popularidad in df_popularidad.items():
        data_para_actualizar.append({
            'id_jugador': int(id_jugador),
            'valor_actual': 0.0, 
            'popularidad': round(float(popularidad), 2) 
        })

    if data_para_actualizar:
        with engine.begin() as conn:
            
            conn.execute(text("UPDATE valor_jugador_fantasy SET popularidad = 0.00"))
            
            stmt = insert(tabla_valor_fantasy)
            
            update_stmt = stmt.on_duplicate_key_update(
                popularidad = stmt.inserted.popularidad
            )

            conn.execute(update_stmt, data_para_actualizar)
            
        print(f"Popularidad actualizada para {len(data_para_actualizar)} jugadores.")
    else:
        print("No se calcularon datos de popularidad.")
        
    return


def update_fantasy_team_points(engine):
    """
    Calcula los puntos de la última jornada para CADA equipo fantasy y 
    actualiza (suma) sus 'puntos_totales' en la base de datos.

    Esta función asume que 'puntos_totales' es un acumulado histórico, 
    por lo que los puntos de la jornada calculados se AÑADIRÁN al total.
    """
    
    print(" Iniciando cálculo de puntos de la jornada fantasy...")

    sql_gameweek_points = """
    WITH 
    -- CTE 1: Equipo real más reciente de cada jugador
    LatestPlayerTeam AS (
        SELECT
            pe.jugador,
            pe.equipo,
            pe.temporada,
            ROW_NUMBER() OVER(PARTITION BY pe.jugador ORDER BY pe.id_plantilla DESC) as rn
        FROM
            plantilla_equipos pe
    ),
    -- CTE 2: Último partido completado de cada equipo real
    LatestTeamMatch AS (
        SELECT
            team_id,
            temporada_id,
            match_id,
            ROW_NUMBER() OVER(PARTITION BY team_id, temporada_id ORDER BY match_fecha DESC, match_id DESC) as rn
        FROM (
            -- Partidos como local
            SELECT p.equipo_local as team_id, p.temporada as temporada_id, p.id_partido as match_id, p.fecha as match_fecha
            FROM partido p
            WHERE p.fecha <= CURDATE()
            UNION ALL
            -- Partidos como visitante
            SELECT p.equipo_visitante as team_id, p.temporada as temporada_id, p.id_partido as match_id, p.fecha as match_fecha
            FROM partido p
            WHERE p.fecha <= CURDATE()
        ) AS all_team_matches
    ),
    -- CTE 3: Puntos de la última jornada para CADA jugador en CADA plantilla fantasy
    FantasyPlayerPoints AS (
        SELECT
            pf.id_equipo_fantasy,
            -- Lógica de puntos: 
            -- 1. Solo cuentan si 'es_titular' = 1
            -- 2. Si 'es_capitan' = 1, los puntos se multiplican x2 (1 + 1)
            -- 3. Si no es capitán ('es_capitan' = 0), se multiplican x1 (1 + 0)
            CASE
                WHEN pf.es_titular = 1 THEN COALESCE(pjj.puntos_fantasy, 0) * (1 + pf.es_capitan)
                ELSE 0
            END AS calculated_points
        FROM
            plantilla_fantasy pf
        JOIN
            jugador j ON pf.id_jugador = j.id_jugador
        -- Encontrar el equipo/temporada real más reciente del jugador
        LEFT JOIN
            LatestPlayerTeam lpt ON j.id_jugador = lpt.jugador AND lpt.rn = 1
        -- Encontrar el último partido de ese equipo real
        LEFT JOIN
            LatestTeamMatch ltm ON lpt.equipo = ltm.team_id AND lpt.temporada = ltm.temporada_id AND ltm.rn = 1
        -- Encontrar los puntos del jugador para ESE partido específico
        LEFT JOIN
            puntos_jugador_jornada pjj ON pf.id_jugador = pjj.id_jugador AND ltm.match_id = pjj.id_partido
    )
    -- Consulta final: Sumar todos los puntos de los jugadores por cada equipo fantasy
    SELECT
        id_equipo_fantasy,
        SUM(calculated_points) AS total_jornada_points
    FROM
        FantasyPlayerPoints
    GROUP BY
        id_equipo_fantasy;
    """
    
    meta = MetaData()
    meta.reflect(bind=engine)
    tabla_equipo_fantasy = meta.tables['equipo_fantasy']
    
    count_updated = 0
    
    # Usamos una transacción para asegurar que todas las actualizaciones
    # se completen con éxito.
    with engine.begin() as conn:
        # 1. Ejecutar la consulta masiva para obtener los puntos de la jornada de todos
        try:
            df_puntos_jornada = pd.read_sql(text(sql_gameweek_points), conn)
        except Exception as e:
            print(f"Error al calcular los puntos de la jornada: {e}")
            return # Salir de la función si la consulta falla

        if df_puntos_jornada.empty:
            print("No se encontraron puntos de jornada para actualizar.")
            return

        print(f"Se calcularon puntos para {len(df_puntos_jornada)} equipos.")

        # 2. Preparar los datos para una actualización masiva (bulk update)
        # Esto es mucho más eficiente que un bucle 'for' con N consultas
        update_data = [
            {
                'id_val': int(row.id_equipo_fantasy), 
                'puntos_val': int(row.total_jornada_points)
            }
            for row in df_puntos_jornada.itertuples()
            if row.total_jornada_points > 0 # Solo actualizar si hay puntos que sumar
        ]

        if not update_data:
            print("No hay equipos con nuevos puntos para actualizar.")
            return

        # 3. Construir y ejecutar la actualización masiva
        # Sumamos los nuevos puntos a los puntos totales existentes
        update_stmt = tabla_equipo_fantasy.update(). \
            where(tabla_equipo_fantasy.c.id_equipo_fantasy == text(':id_val')). \
            values(puntos_totales = tabla_equipo_fantasy.c.puntos_totales + text(':puntos_val'))

        conn.execute(update_stmt, update_data)
        count_updated = len(update_data)

    print(f"✅ Actualización completada. Se sumaron puntos a {count_updated} equipos fantasy.")
    return

def calcular_puntos_predicciones(engine, id_partido):
    """
    Calcula y actualiza los puntos de predicción para todos los usuarios
    que participaron en un partido específico.
    """
    
    print(f"--- Iniciando cálculo de puntos para el partido {id_partido} ---")

    # --- 1. OBTENER LA "HOJA DE RESPUESTAS" (Resultados Reales) ---
    
    # Primero, obtenemos el resultado (quién ganó) y los IDs de los equipos
    sql_match_results = text(f"""
        SELECT
            p.equipo_local,
            p.equipo_visitante,
            e.goles_local,
            e.goles_visitante
        FROM partido p
        JOIN estadisticas_partido e ON p.id_partido = e.partido
        WHERE p.id_partido = :id_partido
    """)
    
    # Segundo, obtenemos el primer gol (jugador y equipo)
    sql_first_goal = text(f"""
        SELECT
            jugador,
            es_local
        FROM mapa_de_disparos
        WHERE
            partido = :id_partido AND resultado = 'Goal'
        ORDER BY
            minuto ASC
        LIMIT 1
    """)
    
    actual_winner = None
    actual_first_team_id = None
    actual_first_player_id = None

    with engine.connect() as conn:
        df_match = pd.read_sql(sql_match_results, conn, params={"id_partido": id_partido})
        df_first_goal = pd.read_sql(sql_first_goal, conn, params={"id_partido": id_partido})

    # --- 2. PROCESAR LA "HOJA DE RESPUESTAS" ---

    if df_match.empty:
        print(f"Error: No se encontraron datos de partido/estadísticas para {id_partido}. Saltando.")
        return

    # Determinar ganador real
    goles_local = df_match['goles_local'].iloc[0]
    goles_visitante = df_match['goles_visitante'].iloc[0]
    
    if goles_local > goles_visitante:
        actual_winner = 'L'
    elif goles_visitante > goles_local:
        actual_winner = 'V'
    else:
        actual_winner = 'E'

    # Determinar primer gol (si hubo)
    if df_first_goal.empty:
        # Partido fue 0-0
        print("Partido sin goles (0-0).")
        actual_first_team_id = None
        actual_first_player_id = None
    else:
        # Hubo goles
        actual_first_player_id = int(df_first_goal['jugador'].iloc[0])
        es_local = df_first_goal['es_local'].iloc[0]
        
        if es_local:
            actual_first_team_id = int(df_match['equipo_local'].iloc[0])
        else:
            actual_first_team_id = int(df_match['equipo_visitante'].iloc[0])

    print(f"Resultados Reales: Ganador={actual_winner}, 1er Equipo={actual_first_team_id}, 1er Jugador={actual_first_player_id}")

    # --- 3. OBTENER TODAS LAS PREDICCIONES DE USUARIOS ---
    
    sql_predictions = text("""
        SELECT id_prediccion, id_usuario, resultado_predicho, 
               primer_equipo_anotar_id, primer_jugador_anotar_id
        FROM prediccion_usuario
        WHERE id_partido = :id_partido
    """)
    
    with engine.connect() as conn:
        df_predictions = pd.read_sql(sql_predictions, conn, params={"id_partido": id_partido})

    if df_predictions.empty:
        print("No se encontraron predicciones de usuarios para este partido. Saltando.")
        return

    print(f"Se encontraron {len(df_predictions)} predicciones para calificar.")

    # --- 4. CALIFICAR CADA PREDICCIÓN ---
    
    data_para_actualizar = []
    
    for row in df_predictions.itertuples():
        puntos = 0
        
        # +3 por adivinar ganador
        if row.resultado_predicho == actual_winner:
            puntos += 3
            
        # +2 por primer equipo en anotar
        # Esto funciona aunque sea 0-0, ya que (None == None) es True
        if row.primer_equipo_anotar_id == actual_first_team_id:
            puntos += 2
            
        # +5 por primer jugador en anotar
        # Esto también funciona para 0-0
        if row.primer_jugador_anotar_id == actual_first_player_id:
            puntos += 5
            
        # Preparamos los datos para la actualización masiva
        data_para_actualizar.append({
            'p_id': row.id_prediccion,
            'puntos': puntos
        })

    # --- 5. ACTUALIZAR PUNTOS EN LA BD (Bulk Update) ---
    
    if data_para_actualizar:
        meta = MetaData()
        meta.reflect(bind=engine)
        tabla_predicciones = meta.tables['prediccion_usuario']
        
        # Preparamos un "bulk update"
        stmt = update(tabla_predicciones).where(
            tabla_predicciones.c.id_prediccion == bindparam('p_id')
        ).values(
            puntos_obtenidos = bindparam('puntos')
        )
        
        with engine.begin() as conn:
            conn.execute(stmt, data_para_actualizar)
            
        print(f"¡Éxito! Se actualizaron {len(data_para_actualizar)} predicciones.")
    
    return
