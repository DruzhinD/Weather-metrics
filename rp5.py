import pandas as pd
import re
from SqlServer_db import SqlServer
from datetime import datetime

from selenium import webdriver #для доступа к rp5
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By #перечисление

import os, time
import gzip #для работы с компрессией gz

class Rp5Observer():
    def __init__(self) -> None:
        #путь, по которому будет храниться конфигурация и прочая информация
        self.data_directory: str = 'data'
        self.config_path: str = f'{self.data_directory}\\config.json'
        self.driver_path: str = f'{self.data_directory}\\driver\\chromedriver.exe'
        self.driver_version = '126.0.6478.55'
    
    def download_weather_csv(self, from_date: datetime, to_date: datetime, location: str) -> str:
        """Загрузка архива с погодой \n
        True - если удалось загрузить, иначе False"""

        #инициализация chromedriver
        chrome_service = Service(executable_path=f'{os.getcwd()}\\{self.driver_path}')
        chrome_options = webdriver.ChromeOptions()

        #устанавливаем путь по умолчанию для загрузки файлов
        prefs = { 'download.default_directory': f'{os.getcwd()}\\{self.data_directory}'}
        chrome_options.add_experimental_option('prefs', prefs)

        #запуск
        driver = webdriver.Chrome(options=chrome_options, service=chrome_service)
        # driver.minimize_window()

        sleep_time = 0.15

        try:
            #переход на главную страницу
            driver.get('https://rp5.am/Погода_в_мире')
            time.sleep(sleep_time)

            #ввод запроса в поиске
            driver.find_element(By.XPATH, '//*[@id="searchStr"]').send_keys(location)

            #нажимаем на кнопку поиска
            driver.find_element(By.XPATH, '//*[@id="searchButton"]').click()
            time.sleep(sleep_time)

            #нажимаем на первый результат из появившихся
            el = driver.find_element(By.CLASS_NAME, 'resBorder')
            el.find_element(By.TAG_NAME, 'a').click()
            time.sleep(sleep_time)
            
            #нажимаем на ссылку на архив
            driver.find_element(By.CLASS_NAME, 'ArchiveStrLink').click()
            time.sleep(sleep_time)

            #нажимаем на раздел "скачать архив погоды"
            driver.find_element(By.XPATH, "// div[contains(text(), 'Скачать архив погоды')]").click()
            time.sleep(sleep_time)
            #ставим чекбокс в csv
            driver.find_element(By.XPATH, '//*[@id="toFileMenu"]/form/table[2]/tbody/tr[2]/td[3]/label/span').click()
            time.sleep(sleep_time)
            #ставим чекбокс в utf-8
            driver.find_element(By.XPATH, '//*[@id="toFileMenu"]/form/table[2]/tbody/tr[3]/td[3]/label/span').click()
            time.sleep(sleep_time)

            #вводим диапазон дат
            #дата ОТ
            el = driver.find_element(By.NAME, 'ArchDate1')
            el.clear()
            el.send_keys(from_date.strftime('%d.%m.%Y'))
            time.sleep(sleep_time)
            #дата ДО
            el2 = driver.find_element(By.NAME, 'ArchDate2')
            el2.clear()
            el2.send_keys(to_date.strftime('%d.%m.%Y'))
            time.sleep(sleep_time)


            #нажимаем на кнопку формирования архива
            download_el = driver.find_element(By.CLASS_NAME, 'download')
            download_el.find_element(By.CLASS_NAME, 'archButton').click()
            time.sleep(sleep_time)

            #нажимаем на кнопку скачивания
            button_tag = driver.find_element(By.ID, 'f_result')
            time.sleep(sleep_time)
            link = button_tag.find_element(By.TAG_NAME, 'a').get_attribute('href')
            time.sleep(sleep_time)
            # tag_with_link = driver.find_element(By.XPATH, "// span[contains(text(), 'Скачать')]")
            # time.sleep(sleep_time)
            # link = tag_with_link.get_attribute('href')
            # time.sleep(sleep_time)
            
            driver.get(link)
            
            #время ожидания загрузки
            await_time = (to_date - from_date).days * 0.1
            if await_time < 1:
                await_time = 1
            time.sleep(await_time)

            driver.close()

        except Exception as ex:
            raise Exception('Во время работы Chrome произошла ошибка')
        
        #работа с файлом .gz
        #получение имени .gz
        gz_name = self.__get_last_downloaded_file_path()
        #получение имени .csv
        csv_name = self.__extract_from_gz(gz_name)
        #Возврат относительного пути к csv
        return f'{self.data_directory}\\{csv_name}'


    def __get_last_downloaded_file_path(self) -> str:
        """Получение имени последнего загруженного файла (.gz)"""

        files = os.listdir(self.data_directory)
        files.sort(key=lambda x: os.path.getmtime(os.path.join(self.data_directory, x)), reverse=True)

        if files:
            for file in files:
                if file.endswith('.csv.gz'): return file
        else:
            raise FileNotFoundError('Не удалось найти файл .gz. Возможно файл не был загружен.')


    def __extract_from_gz(self, gz_name) -> str:
        """Распаковка компрессии .csv.gz и удаление компрессии \n
        Возврат: имя .csv"""

        chunk_size = 8192 #8kb       
        gz_path = f'{self.data_directory}\\{gz_name}'
        csv_path = f'{self.data_directory}\\{gz_name[:-3]}'
        #чтение сжатого файла csv.gz
        with gzip.open(gz_path, 'rb') as f_in:
            #запись в файл .csv
            with open(csv_path, 'wb') as f_out:
                while True:
                    chunk = f_in.read(chunk_size)
                    if not chunk:
                        break
                    f_out.write(chunk)
        
        #удаление сжатого файла .gz
        os.remove(gz_path)

        #имя файла .csv
        return gz_name[:-3]


    def weather_csv_to_dataframe(self, csv_path: str) -> list[pd.DataFrame, str]:
        """читает данные о погоде из csv и возвращает dataframe и название города"""

        print('чтение csv rp5') #отладка
        with open(csv_path, 'r', encoding='utf-8') as file:
            #считываем название города
            raw_city = file.readline()
            not_need_len = len('# Метеостанция')
            
            #обрезаем строку от слова Метеостанция до слеша
            if raw_city.find('/') != -1:
                city = raw_city[not_need_len+1:raw_city.find('/')].strip()
            #если слеш не был найден, то обрезаем до скобки
            else:
                city = raw_city[not_need_len+1:raw_city.find('(')].strip()

            #считываем первые незначащие строки
            for i in range(6-1):
                file.readline()
            #читаем заголовки
            headers = file.readline().strip().split(';')
            for i in range(len(headers)):
                headers[i] = headers[i].replace('"', '')
            #пустой df с заголовками
            dataframe = pd.DataFrame(columns=headers)

            while True:
                line = file.readline()[:-2] #не включает \n и последний разделитель ;
                if line == "": break;
                splited_line = []
                coma2 = 0
                new_line = line
                while True:
                    coma2 = new_line[1:].find('"')
                    if coma2 == -1:
                        break
                    else:
                        splited_line.append(new_line[1: coma2+1])
                        new_line = new_line[coma2+3:]

                dataframe.loc[len(dataframe.index)] = splited_line     
        # dataframe = pd.read_csv(csv_path, sep=';', header=7-1, encoding='utf-8', index_col=None) #cp1252

        print('Завершено чтение из файла') #отладка

        #inplace: False - возвращает DataFrame (конструктор), True - изменяет текущий dataframe (модификатор)
        #удаление ненужных полей
        dataframe.drop(['P', 'Pa', 'ff10', 'ff3', 'WW', 'W1', 'W2', 'Cl', 'Nh', 'H', 'Cm', 'Ch', 'E', 'Tg', "E'", 'sss', 'tR'], #, 'E', 'Tg', "E'", 'sss', 'tR'
                        axis=1, inplace=True)
        rename_dict = {
            dataframe.columns[0]: 'LocalTime',
            'T': 'Temperature',
            'Po': 'Pressure',
            'U': 'Humidity',
            'DD': 'WindDirectionID',
            'Ff': 'WindSpeed',
            'N': 'Cloudiness',
            'Tn': 'TemperatureMin',
            'Tx': 'TemperatureMax',
            'VV': 'HorizontalVisibility',
            'Td': 'TemperatureDewPoint',
            'RRR': 'Precipitation'
        }
        #переименование полей
        dataframe.rename(columns=rename_dict, inplace=True)
        
        #приведение столбцов в числовой вид или изменение значений числового типа
        #приведение поля cloudiness к числовому типу
        cloudiness = dataframe['Cloudiness']
        pattern = '\d+'
        for i in range(cloudiness.size):
            matches = re.findall(pattern, cloudiness.iloc[i].lower()) #поиск чисел
            if len(matches) == 1:
                cloudiness.iloc[i] = float(matches[0]) / 100
            elif len(matches) == 2:
                avg = sum(float(x) / (2*100) for x in matches)
                cloudiness.iloc[i] = avg
            elif cloudiness.iloc[i].lower().find('облаков нет') != -1:
                cloudiness.iloc[i] = 0
            else:
                cloudiness.iloc[i] = 1
        dataframe['Cloudiness'] = cloudiness
        del cloudiness

        #приведение поля Precipitation
        precipitation = dataframe['Precipitation']
        for i in range(precipitation.size):
            item = precipitation.iloc[i].lower()
            if item == 'следы осадков':
                precipitation.iloc[i] = 1
            elif item == 'осадков нет':
                precipitation.iloc[i] = 0
        dataframe['Precipitation'] = precipitation
        del precipitation

        #приведение поля WindDirection
        wind = dataframe['WindDirectionID']
        pattern = f'север|восток|юг|запад|штиль'
        for i in range(wind.size):
            item = wind.iloc[i].lower()
            matches = re.findall(pattern, item)
            direction = ''.join(x[0] for x in matches).upper()
            wind.iloc[i] = direction
        dataframe['WindDirection'] = wind   
        del wind

        #приведение поля Humidity
        humidity = dataframe['Humidity']
        for i in range(humidity.size):
            humidity.iloc[i] = float(humidity.iloc[i]) / 100
        dataframe['Humidity'] = humidity
        del humidity
        
        #приведение поля HorizontalView
        hor = dataframe['HorizontalVisibility']
        for i in range(hor.size):
            item = hor.iloc[i]
            try:
                hor.iloc[i] = float(item) * 1000
            except Exception as ex:
                # print(f'Возникло исключение при привидении HorizontalView') 
                hor.iloc[i] = 0
        dataframe['HorizontalView'] = hor
        del hor
        
        #конвертация типов данных
        #числовые типы
        numeric_cols = [x for x in rename_dict.values()]
        numeric_cols.remove('LocalTime')
        numeric_cols.remove('WindDirectionID')
        dataframe[numeric_cols] = dataframe[numeric_cols].apply(pd.to_numeric)
        #дата
        for i in range(dataframe.shape[0]):
            dt = datetime.strptime(dataframe.at[i, 'LocalTime'], '%d.%m.%Y %H:%M')
            dataframe.at[i, 'LocalTime'] = dt
        
        #строка
        dataframe = dataframe.convert_dtypes()

        print('Завершено приведение данных в записях к виду для дальнейшней обработки.') #отладка
        return [dataframe, city]


    def normalize_rp5_metrics_dataframe(self, df: pd.DataFrame, city: str, db: SqlServer):
        """Приводит датафрейм метрик в вид для базы данных"""

        #получаем id города и добавляем соответствующий столбце в dataframe
        city_df = db.manual_query("SELECT ID FROM Cities WHERE City = '{0}'".format(city))
        city_id = city_df.iat[0, 0]
        lst = [city_id] * df.shape[0]
        df['CityID'] = lst
        del city_df
        #преобразуем направления ветра к числовым значениям
        wind_df = db.get_table_data("WindDirections")
        for i in range(df.shape[0]):
            old = df.at[i, 'WindDirection']
            for j in range(wind_df.shape[0]):
                if old == str(wind_df.at[j, 'Direction']):
                    df.at[i, 'WindDirection'] = str(wind_df.at[j, 'ID'])
                    break 
        df['WindDirection'] = df['WindDirection'].apply(pd.to_numeric)
        del wind_df

        #меняем порядок столбцов
        new_df = df[['CityID', 'LocalTime', 'Temperature', 'TemperatureMax', 'TemperatureMin', 'Pressure',
                'Humidity', 'WindDirection', 'WindSpeed', 'Cloudiness', 'HorizontalVisibility', 'TemperatureDewPoint', 'Precipitation']]
        del df

        print('завершена нормализация датафрейма') #отладка
        return new_df