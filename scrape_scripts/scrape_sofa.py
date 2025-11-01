from bs4 import BeautifulSoup, Comment
from selenium import webdriver
import random

from selenium.webdriver.chrome.options import Options
import cloudscraper
import json
import time
import pandas as pd

def init_driver(): 
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0")
    options.add_argument("accept-language=en-US,en;q=0.9")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    driver = webdriver.Chrome(options=options)
    return driver

def get_sofascore_api_data(driver, path: str) -> dict:
    """
    Realiza una solicitud a la API de SofaScore usando un driver de Selenium ya inicializado.
    """
    base_url = "https://api.sofascore.com/"
    full_url = f"{base_url}{path}"

    driver.get(full_url)
    time.sleep(3)  

    soup = BeautifulSoup(driver.page_source, "html.parser")
    
    try:
        return json.loads(soup.text)
    except json.JSONDecodeError:
        return {"error": "No se pudo decodificar el JSON"}
    

def get_all_players_stats_fbref(id_liga_fbref):
    stats_list = [
    'keepers',
    'keepersadv',
    'shooting',
    'passing',
    'passing_types',
    'gca',
    'defense',
    'possession',
    'playingtime',
    'misc'
    ]
    liga_fbref = [
    '9/Premier-League',
    '12/La-Liga',
    '11/Serie-A',
    '20/Bundesliga',
    '13/Ligue-1'
    ]

    base_url = "https://fbref.com/en/comps/"
    id_parts = liga_fbref[id_liga_fbref].split('/')

    data = pd.DataFrame()
    gk_data = pd.DataFrame()

    scraper = cloudscraper.create_scraper()

    for stat in stats_list:
        full_url = f"{base_url}{id_parts[0]}/{stat}/{id_parts[1]}-Stats"
        print(f"Scraping: {full_url}")

        response = scraper.get(full_url)
        time.sleep(5)
        soup = BeautifulSoup(response.text, "lxml")

        comments = soup.find_all(string=lambda text: isinstance(text, Comment))
        player_tables = []
        for comment in comments:
            if "table" in comment:
                comment_soup = BeautifulSoup(comment, "lxml")
                player_table = comment_soup.find("table", {"id": lambda x: x and x.startswith("stats_")})
                if player_table:
                    player_tables.append(player_table)

        if not player_tables:
            print(f"No se encontró la tabla de jugadores para {stat}")
            continue

        placeholder = pd.concat([pd.read_html(str(tbl))[0] for tbl in player_tables], axis=0, ignore_index=True)

        if stat in ['keepers', 'keepersadv']:
            gk_data = pd.concat([gk_data, placeholder], axis=1) if not gk_data.empty else placeholder
        else:
            data = pd.concat([data, placeholder], axis=1) if not data.empty else placeholder

    print("Scraping finalizado")
    data.columns = data.columns.droplevel(0)
    gk_data.columns = gk_data.columns.droplevel(0)
    player_col = data.loc[:, 'Player']
    if isinstance(player_col, pd.DataFrame):
        player_col = player_col.iloc[:, 0]

    mask = player_col.notna() & (player_col.astype(str).str.strip() != '') & (player_col != 'Player')
    data = data[mask].copy()
    data['Player'] = player_col[mask].values

    data.reset_index(drop=True, inplace=True)

    player_col = gk_data.loc[:, 'Player']
    if isinstance(player_col, pd.DataFrame):
        player_col = player_col.iloc[:, 0]
    mask = player_col.notna() & (player_col.astype(str).str.strip() != '') & (player_col != 'Player')
    gk_data = gk_data[mask].copy()

    gk_data['Player'] = player_col[mask].values

    gk_data = gk_data.loc[:, ~gk_data.columns.duplicated(keep='first')]

    gk_data.reset_index(drop=True, inplace=True)

    return data, gk_data








def get_all_teams_stats_fbref(id_liga_fbref):
    base_url = "https://fbref.com/en/comps/"
    full_url = f"{base_url}{id_liga_fbref}-Stats"

    scraper = cloudscraper.create_scraper()  
    response = scraper.get(full_url)
    
    soup = BeautifulSoup(response.text, "lxml")

    tables = soup.find_all("table")

    if not tables:
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))
        for comment in comments:
            if "table" in comment:
                comment_soup = BeautifulSoup(comment, "lxml")
                tables += comment_soup.find_all("table")

    df_total = [pd.read_html(str(table))[0] for table in tables if table]
    tabla_ofensiva =  df_total[8]
    tabla_ofensiva_en_contra = df_total[9]
    tabla_pass = df_total[12]
    tabla_gk = df_total[6]
    tabla_ofensiva_en_contra.columns = tabla_ofensiva_en_contra.columns.droplevel(0)
    cols = tabla_ofensiva_en_contra.columns.tolist()
    cols[15] = 'xgAgainst'
    tabla_ofensiva_en_contra.columns = cols
    tabla_gk.columns = tabla_gk.columns.droplevel(0)
    gk = tabla_gk['Att'].iloc[:, 1]

    tabla_misc = df_total[22]
    tabla_def = df_total[16]
    tabla_posesion = df_total[18]
    tabla_gca = df_total[14]
    tabla_pases_progres = df_total[10]
    tabla_pases_progres.columns = tabla_pases_progres.columns.droplevel(0)
    prg_pas = tabla_pases_progres['PrgP']
    xg_en_contra = tabla_ofensiva_en_contra['xgAgainst']
    tabla_pass.columns = tabla_pass.columns.droplevel(0)
    pas = tabla_pass[['FK','CK','Att','Cmp']]
    tabla_gca.columns = tabla_gca.columns.droplevel(0)
    gca = tabla_gca['GCA']
    tabla_posesion.columns = tabla_posesion.columns.droplevel(0)
    pos = tabla_posesion[['Att', 'PrgC']]
    tabla_misc.columns = tabla_misc.columns.droplevel(0)
    misc = tabla_misc[['Fls', 'CrdY', 'CrdR', '2CrdY', 'Won']]
    tabla_ofensiva.columns = tabla_ofensiva.columns.droplevel(0)
    off = tabla_ofensiva[['Squad', 'xG', 'Sh', 'SoT', 'FK', 'PK']]
    tabla_def.columns = tabla_def.columns.droplevel(0)
    defn1 = tabla_def['Tkl'].iloc[:,0]
    defn2 = tabla_def[['Blocks', 'Int', 'Clr']]
    df_concat = pd.concat([off,misc,pas, gk, defn1, defn2,pos, gca, prg_pas, xg_en_contra], axis=1)
    cols = df_concat.columns.tolist()
    cols[13] = 'PasesIntentados'
    cols[15] = 'goalKick'
    cols[20] = 'dribblesAtt'
    cols[4] = 'FKaPuerta'
    df_concat.columns = cols

    return df_concat

def get_all_urls_fbref( liga_url, max_retries=5):
    links_fbref = []
    scraper = cloudscraper.create_scraper()  
    base_url = "https://fbref.com"

    retries = 0
    delay = 5  

    while retries < max_retries:
        response = scraper.get(liga_url)

        if response.status_code == 429:
            print(f"[429] Demasiadas solicitudes. Esperando {delay} segundos antes de reintentar...")
            time.sleep(delay)
            delay *= 2  
            retries += 1
            continue

        if response.status_code != 200:
            print(f"Error {response.status_code} al acceder a {liga_url}")
            return []

        soup = BeautifulSoup(response.text, "lxml")
        for row in soup.select("table tbody tr"):
            for a in row.find_all("a", href=lambda href: href and "/en/matches/" in href):
                match_link = base_url + a["href"]
                links_fbref.append(match_link)

        links_fbref = list(dict.fromkeys(links_fbref))
        print(f"Se extrajeron {len(links_fbref)} links de {liga_url}")

        time.sleep(random.uniform(3, 7))
        return links_fbref

    print("Se alcanzó el número máximo de reintentos. No se pudieron obtener los links.")
    return []