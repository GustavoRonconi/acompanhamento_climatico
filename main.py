import xlrd
import requests
import json
import logging
from datetime import datetime
import sqlite3
import pytz
import sys

ARQUIVO_CIDADES = "planilhas/cidade.xlsx"
API_FORECAST = "0680857673815820f1dda21bf4f6c83b"
API_GEO_KEY = "Nwx8FN6bij4DBcvQZESnljVdTYnL02wv"
DATABASE = "climatologia.db"

logging.basicConfig(
    filename="logs/acompanhamento_climatico.log",
    level=logging.INFO,
    format="%(levelname)s - %(asctime)s  - %(message)s",
)
logger = logging.getLogger(__name__)
local_tz = pytz.timezone("America/Sao_Paulo")


def progress(count, total, status=""):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = "=" * filled_len + "-" * (bar_len - filled_len)

    sys.stdout.write("[%s] %s%s ...%s\r" % (bar, percents, "%", status))
    sys.stdout.flush()


def procurar_coordenadas_regiao(nome_cidade: str, uf: str) -> int:
    try:
        url_find_coordinates = f"http://www.mapquestapi.com/geocoding/v1/address?key={API_GEO_KEY}&location={nome_cidade},{uf}"
        response = requests.request("GET", url_find_coordinates)
        if response.status_code == 200:
            response_dict = json.loads(response.text)
            return response_dict["results"][0]["locations"][0]["latLng"]
    except Exception as e:
        logger.error(e)


def previsao_from_xls(caminho_arquivo: str, coluna_cidade: int, coluna_uf: int) -> list:
    """Abre a planilha com os nomes de cidades e estados"""
    planilha_cidades = xlrd.open_workbook(caminho_arquivo).sheet_by_index(0)
    cidade_uf = []
    for row in range(1, planilha_cidades.nrows):
        cidade_uf.append(
            {
                "cidade": planilha_cidades.cell_value(row, coluna_cidade),
                "uf": planilha_cidades.cell_value(row, coluna_uf),
            }
        )
    previsao_cidades = []
    for row in cidade_uf:
        previsao_cidade = {"cidade": row["cidade"], "uf": row["uf"]}
        lat_long = procurar_coordenadas_regiao(row["cidade"], row["uf"])
        previsao_cidade.update(requisitar_previsao_tempo(**lat_long))
        previsao_cidades.append(previsao_cidade)
    return previsao_cidades


def requisitar_previsao_tempo(lat: float, lng: float) -> dict:
    """Requisita a previsao do tempo"""
    try:
        url_forecast = f"http://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lng}&lang=pt_br&appid=0680857673815820f1dda21bf4f6c83b&units=metric"
        response = requests.request("GET", url_forecast)
        if response.status_code == 200:
            response_dict = json.loads(response.text)
            return response_dict
    except Exception as e:
        logger.error(e)


def timestamp_to_datetime(timestamp: int) -> str:
    dt_object = datetime.fromtimestamp(timestamp)
    dt_object = dt_object.astimezone(local_tz)
    return dt_object.strftime("%Y/%m/%d %H:%M:%S")


def insert_on_table(table: str, dict_insert: dict) -> int:
    try:
        conn = sqlite3.connect(DATABASE)
        cursor_execute = conn.cursor()
        str_columns = ",".join(list(dict_insert.keys()))
        values = tuple(dict_insert.values())
        cursor_execute.execute(
            f"INSERT INTO {table}({str_columns}) VALUES {str(values)}"
        )
        conn.commit()
        cursor_execute.close()
        conn.close()
        return cursor_execute.lastrowid
    except Exception as e:
        logger.error(e)


def serialize_and_insert_climatologia(previsao_cidade: dict) -> int:
    mapper_climatologia = {
        "cidade": "cidade",
        "uf": "uf",
        "sunrise": "nascer_do_sol",
        "sunset": "por_do_sol",
        "temp": "temperatura",
        "dt": "data_referencia",
        "description": "descricao_clima",
        "humidity": "umidade",
    }

    def _check_in_mapper_climatologia(value: str) -> bool:
        if value in mapper_climatologia.keys():
            return True
        return False

    dict_insert = {}
    for key, value in previsao_cidade.items():
        if _check_in_mapper_climatologia(key):
            dict_insert.update({mapper_climatologia[key]: value})
        elif key == "current":
            for k_current, v_current in value.items():
                if _check_in_mapper_climatologia(k_current):
                    if k_current in ["dt", "sunrise", "sunset"]:
                        v_current = timestamp_to_datetime(v_current)
                    dict_insert.update({mapper_climatologia[k_current]: v_current})
                elif k_current == "weather":
                    for k_weather, v_weather in v_current[0].items():
                        if _check_in_mapper_climatologia(k_weather):
                            dict_insert.update(
                                {mapper_climatologia[k_weather]: v_weather}
                            )

    return insert_on_table("climatologia_atual", dict_insert)


def serializa_and_insert_historico(previsao_cidade: dict, id_climatologia: int) -> bool:
    mapper_historico = {
        "cidade": "cidade",
        "uf": "uf",
        "temp": "temperatura",
        "description": "descricao_clima",
        "humidity": "umidade",
    }

    def serializa_historico(
        previsao_cidade: dict,
        mapper_historico: dict,
        periodo: str,
        id_climatologia: int,
    ) -> bool:
        def check_mapper_historico(key, mapper):
            if key in mapper.keys():
                return True
            return False

        dict_insert_historico_horario = {}
        dict_insert_historico_diario = {}
        for key, value in previsao_cidade.items():
            if check_mapper_historico(key, mapper_historico):
                dict_insert_historico_horario.update({mapper_historico[key]: value})
                dict_insert_historico_diario.update({mapper_historico[key]: value})
            elif key == "hourly" and periodo == "hourly":
                for row in value:
                    for key_hourly, value_hourly in row.items():
                        if check_mapper_historico(key_hourly, mapper_historico):
                            if key_hourly in ["dt", "sunrise", "sunset"]:
                                value_hourly = timestamp_to_datetime(value_hourly)
                            dict_insert_historico_horario.update(
                                {mapper_historico[key_hourly]: value_hourly}
                            )
                        elif key_hourly == "weather":
                            for k_weather, v_weather in value_hourly[0].items():
                                if check_mapper_historico(k_weather, mapper_historico):
                                    dict_insert_historico_horario.update(
                                        {mapper_historico[k_weather]: v_weather}
                                    )
                        elif key_hourly == "rain":
                            for k_rain, v_rain in value_hourly.items():
                                if check_mapper_historico(k_rain, mapper_historico):
                                    dict_insert_historico_horario.update(
                                        {mapper_historico[k_rain]: v_rain}
                                    )
                    dict_insert_historico_horario.update(
                        {"id_climatologia": id_climatologia}
                    )
                    insert_on_table(
                        "historico_horario", dict_insert_historico_horario,
                    )

            elif key == "daily" and periodo == "daily":
                for row in value:
                    for key_daily, value_daily in row.items():
                        if check_mapper_historico(key_daily, mapper_historico):
                            if key_daily in ["dt", "sunrise", "sunset"]:
                                value_daily = timestamp_to_datetime(value_daily)
                            if key_daily == "temp":
                                value_daily = value_daily["max"]
                            dict_insert_historico_diario.update(
                                {mapper_historico[key_daily]: value_daily}
                            )
                        elif key_daily == "weather":
                            for k_weather, v_weather in value_daily[0].items():
                                if check_mapper_historico(k_weather, mapper_historico):
                                    dict_insert_historico_diario.update(
                                        {mapper_historico[k_weather]: v_weather}
                                    )
                    dict_insert_historico_diario.update(
                        {"id_climatologia": id_climatologia}
                    )
                    insert_on_table(
                        "historico_diario", dict_insert_historico_diario,
                    )

        return True

    try:
        mapper_historico_horario = mapper_historico.copy()
        mapper_historico_horario.update({"dt": "hora_previsao", "1h": "chuva"})
        dict_insert_historico_horario = serializa_historico(
            previsao_cidade, mapper_historico_horario, "hourly", id_climatologia
        )

        mapper_historico_diario = mapper_historico.copy()
        mapper_historico_diario.update(
            {
                "dt": "dia_previsao",
                "rain": "chuva",
                "sunrise": "nascer_do_sol",
                "sunset": "por_do_sol",
            }
        )
        dict_insert_historico_diario = serializa_historico(
            previsao_cidade, mapper_historico_diario, "daily", id_climatologia
        )

        return True
    except Exception as e:
        logger.error(e)


def persist_previsao_cidades(previsao_cidades: list) -> bool:
    for index, row in enumerate(previsao_cidades):
        id_climatologia = serialize_and_insert_climatologia(row)
        if serializa_and_insert_historico(row, id_climatologia):
            logger.info(f"INFORMAÇÕES DE CLIMATOLOGIA ATUALIZADAS: {id_climatologia}")
        progress(index, len(previsao_cidades), status="PROCESSANDO CLIMATOLOGIA")


if __name__ == "__main__":
    print("Processando o arquivo de cidades...")
    previsao_cidades = previsao_from_xls(ARQUIVO_CIDADES, 0, 1)
    persist_previsa_cidades = persist_previsao_cidades(previsao_cidades)

