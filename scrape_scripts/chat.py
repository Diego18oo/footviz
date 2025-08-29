from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from datetime import datetime
import time
import pandas as pd
import requests
import json


def get_match_report_links_selenium(url):
    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)

    driver.get(url)
    time.sleep(5)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    links = []
    for row in soup.select("table tbody tr"):
        report_link = row.find("a", string="Match Report")
        date_cell = row.find("td", {"data-stat": "date"})
        if report_link and date_cell:
            try:
                date_obj = datetime.strptime(date_cell.text.strip(), "%Y-%m-%d")
                links.append((date_obj, "https://fbref.com" + report_link["href"]))
            except ValueError:
                pass

    links.sort(key=lambda x: x[0])
    return links



def get_prematch_links_selenium(url):
    options = Options()
    options.add_argument("--headless")  
    options.add_argument("--disable-blink-features=AutomationControlled")
    driver = webdriver.Chrome(options=options)

    driver.get(url)
    time.sleep(15)  

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/en/stathead/matchup/teams/"):
            links.append("https://fbref.com" + href)
    return list(set(links))

def get_premier_league_team_ids():
    url = "https://fbref.com/en/comps/9/Premier-League-Stats"
    options = Options()
    options.add_argument("--headless")  
    options.add_argument("--disable-blink-features=AutomationControlled")
    driver = webdriver.Chrome(options=options)

    driver.get(url)
    time.sleep(5)  

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    team_ids = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/en/squads/"):
            team_id = href.split("/")[3]
            team_ids.add(team_id)

    return list(team_ids)







def get_sofascore_api_data(path: str) -> dict:
    """
    Realiza una solicitud a la API de SofaScore usando Selenium para evitar bloqueos (como el error 403).

    Args:
        path (str): Ruta relativa del endpoint de SofaScore (por ejemplo, 'api/v1/event/14025013').

    Returns:
        dict: Respuesta JSON como diccionario.
    """
    base_url = "https://api.sofascore.com/"
    full_url = f"{base_url}{path}"

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0")
    options.add_argument("accept-language=en-US,en;q=0.9")

    driver = webdriver.Chrome(options=options)
    driver.get(full_url)
    time.sleep(3) 

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    try:
        return json.loads(soup.text)
    except json.JSONDecodeError:
        return {"error": "No se pudo decodificar el JSON"}



if __name__ == "__main__":
    url = "https://fbref.com/en/comps/9/2024-2025/schedule/2024-2025-Premier-League-Scores-and-Fixtures"
    match_links = get_match_report_links_selenium(url)
    print(len(match_links), "partidos encontrados")
    for date, link in match_links[:5]:
        print(date, link)


        
