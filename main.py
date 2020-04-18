import xlrd
import requests
import json
import logging

CAMINHO_ARQUIVO = "acompanhamento_climatico/planilhas/cidade.xlsx"
API_FORECAST = "0680857673815820f1dda21bf4f6c83b"
API_GEO_KEY = "Nwx8FN6bij4DBcvQZESnljVdTYnL02wv"

logging.basicConfig(
    filename="acompanhamento_climatico/logs/acompanhamento_climatico.log",
    level=logging.ERROR,
    format="%(levelname)s - %(asctime)s  - %(message)s",
)
logger = logging.getLogger(__name__)


class PrevisaoTempo:
    def __init__(self, previsao_tempo):
        self.previsao_tempo = previsao_tempo


def procurar_coordenadas_regiao(nome_cidade: str, uf: str) -> int:
    try:
        url_find_coordinates = f"http://www.mapquestapi.com/geocoding/v1/address?key={API_GEO_KEY}&location={nome_cidade},{uf}"
        response = requests.request("GET", url_find_coordinates)
        if response.status_code == 200:
            response_dict = json.loads(response.text)
            return response_dict['results'][0]['locations'][0]['latLng']
    except Exception as e:
        logger.error(e)


def previsao_from_xls(caminho_arquivo: str, coluna_cidade: int, coluna_uf: int) -> list:
    """Abre a planilha com os nomes de cidades e estados"""
    planilha_cidades = xlrd.open_workbook(caminho_arquivo).sheet_by_index(0)
    cidade_uf = []
    for row in range(1, planilha_cidades.nrows):
        cidade_uf.append({'cidade': planilha_cidades.cell_value(row, coluna_cidade), 'uf': planilha_cidades.cell_value(row, coluna_uf)})
    previsao_cidades = []
    for row in cidade_uf:
        previsao_cidade = {'cidade': row['cidade'], 'uf': row['uf']}
        lat_long = procurar_coordenadas_regiao(row['cidade'], row['uf'])
        previsao_cidade.update(requisitar_previsao_tempo(**lat_long))  
        previsao_cidades.append(previsao_cidade)
    return previsao_cidades


def requisitar_previsao_tempo(lat: float, lng: float) -> dict:
    """Requisita a previsao do tempo"""
    try:
        url_forecast = f"http://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lng}&lang=pt_br&appid=0680857673815820f1dda21bf4f6c83b"
        response = requests.request("GET", url_forecast)
        if response.status_code == 200:
            response_dict = json.loads(response.text)
            return response_dict
    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    previsao_cidades = previsao_from_xls(CAMINHO_ARQUIVO, 0, 1)
    print('a')


