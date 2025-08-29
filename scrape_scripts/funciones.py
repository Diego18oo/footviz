import pandas as pd
import LanusStats as ls
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.mysql import insert
from scrape_sofa import get_sofascore_api_data, init_driver, get_all_teams_stats_fbref, get_all_urls_fbref, get_all_players_stats_fbref
#from scrape import links_totales 
from chat import  get_match_report_links_selenium
from datetime import datetime, timedelta
from rapidfuzz import process, fuzz


fbref = ls.Fbref()


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

def get_links_totales(engine, driver):
    
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    yesterday = yesterday.strftime('%Y-%m-%d')
    df_partidos_ayer = pd.read_sql(f"SELECT * FROM partido WHERE fecha = '{yesterday}'", engine)
    ids_para_scrapear_temporada = df_partidos_ayer['temporada'].unique() 
    links_totales = []
    for i in range(len(ids_para_scrapear_temporada)):
        iteracion = ids_para_scrapear_temporada[i]
        url = f"https://fbref.com/en/comps/{liga_fbref_fixtures[iteracion-1]}"
        lista_links = get_all_urls_fbref(driver, liga_url=url)
        links_totales = links_totales + lista_links
    return links_totales

def insert_tabla_posiciones(liga_a_scrapear, engine, temporada, liga):
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
        data_fbref = get_all_teams_stats_fbref(driver, id_liga_fbref)
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
                pases_progresivos = data_equipo['PrgP']
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
                pases_progresivos=insert_stmt.inserted.pases_progresivos

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
    arbitro = data['event']['referee']
    return arbitro

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
        fecha = verificar_fecha_partido(driver, url_sofascore, jornada, temporada)
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
        links_totales = get_links_totales(engine, driver) 
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
            entradas_visitante = tackles['away']
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
        )

        conn.execute(update_estadistica_partido_stmt)

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
    if dob_timestamp is not None:
        fecnac = datetime.utcfromtimestamp(dob_timestamp).strftime('%Y-%m-%d')
    else: 
        fecnac = None
    insert_jugador_stmt = insert(tabla_jugador).values(
        nombre = data_jugador['player']['name'],
        dorsal = data_jugador['player'].get('jerseyNumber', None),
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
    return 

def insert_update_plantilla_equipos(engine, driver, ligas, temporadas):
    engine = create_engine('mysql+pymysql://root@localhost/footviz')
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
                        fecha_inicio = fecha_inicio,
                        jugador = id_jugador,
                        equipo = mapa_equipos.get(nombre_equipo, None),
                        temporada = i +1
                    )

                    update_plantilla_equipos_stmt = insert_plantilla_equipos_stmt.on_duplicate_key_update(
                        fecha_inicio=insert_plantilla_equipos_stmt.inserted.fecha_inicio,
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

def insert_mapa_de_calor(engine, driver, id_sofascore):
    engine = create_engine('mysql+pymysql://root@localhost/footviz')
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
        for i in range(len(data['home']['players'])):
            id_jugador_sofascore = data['home']['players'][i]['player']['id']
            nombre_jugador = data['home']['players'][i]['player']['name']
            print(f"Scrapeando a {nombre_jugador} {id_jugador_sofascore}")
            try:
                minutos_jugados = data['home']['players'][i]['statistics']['minutesPlayed']
                print(f"{nombre_jugador} jugo {minutos_jugados} minutos en el partido")
            except KeyError:
                print(f"{nombre_jugador} no jugo en el partido")
                minutos_jugados = 0
            id_jugador = mapa_jugadores.get(nombre_jugador, None)
            if id_jugador is None: 
                data_jugador = get_jugador(driver, id_jugador_sofascore)
                insert_jugador(engine, data_jugador, conn, tabla_jugador)
                df_jugador_actualizado = pd.read_sql('SELECT id_jugador, nombre FROM jugador', engine)
                mapa_jugadores.update(dict(zip(df_jugador_actualizado['nombre'], df_jugador_actualizado['id_jugador'])))
                id_jugador = mapa_jugadores.get(nombre_jugador) 
            if minutos_jugados > 2:
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
            id_jugador_sofascore = data['away']['players'][i]['player']['id']
            nombre_jugador = data['away']['players'][i]['player']['name']
            try:
                minutos_jugados = data['away']['players'][i]['statistics']['minutesPlayed']
                print(f"{nombre_jugador} jugo {minutos_jugados} minutos en el partido")
            except KeyError:
                print(f"{nombre_jugador} no jugo en el partido")
                minutos_jugados = 0       
            id_jugador = mapa_jugadores.get(nombre_jugador, None)
            if id_jugador is None: 
                data_jugador = get_jugador(driver, id_jugador_sofascore)
                insert_jugador(engine, data_jugador, conn, tabla_jugador)
                df_jugador_actualizado = pd.read_sql('SELECT id_jugador, nombre FROM jugador', engine)
                mapa_jugadores.update(dict(zip(df_jugador_actualizado['nombre'], df_jugador_actualizado['id_jugador'])))
                id_jugador = mapa_jugadores.get(nombre_jugador) 
            if minutos_jugados > 2:
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
    engine = create_engine('mysql+pymysql://root@localhost/footviz')
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

def insert_estadistica_jugador(engine,  ids_para_scrapear_temp):
    engine = create_engine('mysql+pymysql://root@localhost/footviz')
    df_jugadores = pd.read_sql('SELECT id_jugador, nombre FROM jugador', engine)
    mapa_jugadores = dict(zip(df_jugadores['nombre'], df_jugadores['id_jugador'])) 
    df_alias = pd.read_sql('SELECT id_jugador, alias FROM alias_jugador', engine)
    mapa_alias = dict(zip(df_alias['alias'], df_alias['id_jugador']))
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
                                      'Blocks', 'Clr', 'Won']]
            df_chulo.fillna(0, inplace=True)
            df_porteros_chulo.fillna(0, inplace=True)

            for i in range(len(df_chulo)):
                nombre_fbref = df_chulo['Player'].iloc[i]
                nombre_match = buscar_jugador(nombre_fbref, lista_nombres, mapa_jugadores, mapa_alias, df_jugadores)
                fk = df_chulo['passing_FK'].iloc[i] + df_chulo['shooting_FK'].iloc[i]
                if nombre_match:
                    jugador_id = mapa_jugadores[nombre_match]
                else:
                    jugador_id = None
                    print(f"No se pudo agregar el nombre de {nombre_fbref}")
                insert_estadistica_jugador_stmt = insert(tabla_estadistica_jugador).values(
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
       
            for i in range(len(df_porteros_chulo)):
                nombre_fbref = df_porteros_chulo['Player'].iloc[i]
                nombre_match = buscar_jugador(nombre_fbref, lista_nombres, mapa_jugadores, mapa_alias, df_jugadores)
                
                if nombre_match:
                    jugador_id = mapa_jugadores[nombre_match]
                else:
                    jugador_id = None
                    print(f"No se pudo agregar correctamente a {nombre_fbref}")
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

        print(f"Liga {ligas[iteracion-1]} insertada correctamente")
    return 