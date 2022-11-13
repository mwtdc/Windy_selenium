import datetime
import logging
import pathlib
import sys
import urllib.parse
from time import sleep

import pandas as pd
import pymysql
import requests
import yaml
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

START_TIME = datetime.datetime.now()
# warnings.filterwarnings('ignore')
print(f'Старт парсинга Windy {START_TIME}')

# Проверяем запущен .py файл или .exe и в зависимости от этого получаем пути
if getattr(sys, 'frozen', False):
    GECKO_PATH = (f'{pathlib.Path(sys.executable).parent.absolute()}/'
                  f'geckodriver.exe')
    FIREFOX_PATH = (f'{pathlib.Path(sys.executable).parent.absolute()}/'
                    f'FirefoxPortable/App/Firefox64/firefox.exe')
elif __file__:
    GECKO_PATH = (f'{pathlib.Path(__file__).parent.absolute()}/'
                  f'geckodriver.exe')
    FIREFOX_PATH = (f'{pathlib.Path(__file__).parent.absolute()}/'
                    f'FirefoxPortable/App/Firefox64/firefox.exe')

# Настройки для драйвера Firefox
# (скрытый режим и установка драйвера(закоменчена),
# теперь берется geckodriver.exe из этой же папки и portable версия firefox
# (чтобы работало даже на чистой системе))

options = Options()
options.headless = True
options.binary_location = FIREFOX_PATH
serv = Service(GECKO_PATH)
browser = webdriver.Firefox(options=options, service=serv)

# Загружаем yaml файл с настройками

with open(
    f'{pathlib.Path(__file__).parent.absolute()}/settings.yaml', 'r'
) as yaml_file:
    settings = yaml.safe_load(yaml_file)
    telegram_settings = pd.DataFrame(settings['telegram'])
    sql_settings = pd.DataFrame(settings['sql_db'])
    pyodbc_settings = pd.DataFrame(settings['pyodbc_db'])


# Функция отправки уведомлений в telegram на любое количество каналов
#  (указать данные в yaml файле настроек)


def telegram(i, text):
    msg = urllib.parse.quote(str(text))
    bot_token = str(telegram_settings.bot_token[i])
    channel_id = str(telegram_settings.channel_id[i])
    requests.post(f'https://api.telegram.org/bot{bot_token}/'
                  f'sendMessage?chat_id={channel_id}&text={msg}')


# Функция коннекта к базе Mysql
# (для выбора базы задать порядковый номер числом !!! начинается с 0 !!!!!)


def connection(i):
    host_yaml = str(sql_settings.host[i])
    user_yaml = str(sql_settings.user[i])
    port_yaml = int(sql_settings.port[i])
    password_yaml = str(sql_settings.password[i])
    database_yaml = str(sql_settings.database[i])
    return pymysql.connect(host=host_yaml, user=user_yaml, port=port_yaml,
                           password=password_yaml, database=database_yaml)

# Загрузка списка ГТП и координат из базы


connection_geo = connection(0)
with connection_geo.cursor() as cursor:
    sql = 'select gtp,lat,lng from visualcrossing.ses_gtp;'
    cursor.execute(sql)
    ses_dataframe = pd.DataFrame(
        cursor.fetchall(), columns=['gtp', 'lat', 'lng']
        )
    connection_geo.close()
logging.info(f'Список ГТП и координаты станций'
             f' загружены из базы visualcrossing.ses_gtp')


# Загрузка прогнозов погоды по станциям

dataframe_3 = pd.DataFrame()
dataframe_2 = pd.DataFrame()

parameters = ['solarpower', 'clouds', 'lclouds', 'mclouds', 'hclouds',
              'uvindex', 'temp', 'rh', '', 'gust', 'gustAccu',
              'rain', 'rainAccu', 'snowAccu', 'snowcover', 'visibility',
              'pressure', 'thunder', 'cloudtop', 'awr_0_40', 'awd_0_40']
XPATH = '/html/body/div[1]/div[1]/div[4]/div[2]/div[2]/span/big'
UV_XPATH = '/html/body/div[1]/div[1]/div[4]/div[3]/div[2]/span/big'
# TIME_XPATH = '/html/body/div[4]/div[1]/div[2]/div[1]'
BUTTON = 'fg-red.size-xs.inlined.clickable'
g = 0

for ses in range(len(ses_dataframe.index)):
    gtp = str(ses_dataframe.gtp[ses])
    lat = str(ses_dataframe.lat[ses]).replace(',', '.')
    lng = str(ses_dataframe.lng[ses]).replace(',', '.')
    print(gtp)
    for hour in ('09', '12', '15', '18', '21'):
        for col in parameters:
            URL = (f'https://www.windy.com/{lat}/{lng}/meteogram?{col}'
                   f',20221011{hour},{lat},{lng},8,m:eTEahrj')

            def get_url(url):
                try:
                    browser.get(url)
                    sleep(1)
                    WebDriverWait(browser, 10).until(
                        ec.presence_of_element_located(
                            (By.CLASS_NAME, BUTTON)))
                    click_button = browser.find_element(
                        By.CLASS_NAME, BUTTON)
                    browser.execute_script(
                        'arguments[0].click();',
                        click_button)
                    # element = browser.find_element(By.XPATH, TIME_XPATH)
                    # browser.execute_script("arguments[0].setAttribute('value',arguments[1])", element, hour)
                    sleep(1)
                    if col in ('uvindex', 'awr_0_40', 'awd_0_40'):
                        data = browser.find_element(By.XPATH, UV_XPATH).text
                    else:
                        data = browser.find_element(By.XPATH, XPATH).text
                    print(data)
                    return data
                except Exception:
                    print(f'Не найден элемент {col}')
                    return None
            # print(URL)
            data = get_url(URL)
            if data is None:
                while data is None:
                    data = get_url(URL)

            dataframe_2.at[0, 'gtp'] = gtp
            dataframe_2.at[0, 'hour'] = hour
            if col == '':
                dataframe_2.at[0, 'wind_direction'] = data.split().pop(0)
                dataframe_2.at[0, 'wind'] = data.split().pop(1)
            else:
                dataframe_2.at[0, col] = data.split().pop(0)
        dataframe_3 = dataframe_3.append(dataframe_2, ignore_index=True)
        print(dataframe_3)
print(dataframe_3)
print(f'Время выполнения: {datetime.datetime.now() - START_TIME}')
