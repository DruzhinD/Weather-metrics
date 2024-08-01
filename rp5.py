import pandas as pd
import math
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
        driver.maximize_window()

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
            # driver.find_element(By.CLASS_NAME, 'ArchiveStrLink').click()
            driver.find_element(By.XPATH, "//a[contains(@href, 'Архив')]").click()
            time.sleep(sleep_time)
            
            #нажимаем на раздел "скачать архив погоды"
            driver.find_element(By.XPATH, "// div[contains(text(), 'Скачать архив погоды')]").click()
            #обновляем страницу, чтобы информация отобразилась корректно
            driver.refresh()
            time.sleep(1)

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
            time.sleep(2) #ожидание появления кнопки
            link = button_tag.find_element(By.TAG_NAME, 'a').get_attribute('href')
            time.sleep(sleep_time)
            
            driver.get(link)
            
            #время ожидания загрузки
            await_time = (to_date - from_date).days * 0.05
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
            
            #удаляем заголовок, состоящий из пустой строки, если такой имеется
            try:
                empty_id = headers.index('')
                headers.remove(headers[empty_id])
            except ValueError:
                pass

            for i in range(len(headers)):
                headers[i] = headers[i].replace('"', '')
            
            #df из csv файла
            csv_dataframe = pd.DataFrame(columns=headers)

            #пустой df с заголовками
            # dataframe = pd.DataFrame(columns=headers)

            #заполнение dataframe из csv файла
            while True:
                line = file.readline()[:-2] #не включает \n и последний разделитель ;
                if line == "": break
                splited_line = []
                
                #разделение полей по "", т.е. каждое поле находится в отдельных ""
                while True:
                    #id кавычки
                    coma = line[1:].find('"')
                    if coma == -1:
                        break
                    else:
                        #добавляем поле в список, не включая кавычки
                        splited_line.append(line[1: coma+1])
                        #обрезание строки, исключая только что добавленное в список поле
                        line = line[coma+3:]

                csv_dataframe.loc[len(csv_dataframe.index)] = splited_line     
        # dataframe = pd.read_csv(csv_path, sep=';', header=7-1, encoding='utf-8', index_col=None) #cp1252
        print('Завершено чтение из файла') #отладка
        
        dataframe = pd.DataFrame()
        #заполняем возвращаемый dataframe
        #вставка полей LocalTime, Temperature, Pressure
        dataframe.insert(0, 'LocalTime', csv_dataframe.iloc[:, 0])
        dataframe.insert(1, 'Temperature', csv_dataframe.iloc[:, 1])
        dataframe.insert(2, 'Pressure', csv_dataframe.iloc[:, 2])

        #строка, в которой хранится 1 запись из dataframe
        first_row = csv_dataframe.iloc[0]
        #id поля, в котором в последний раз были найдены нужные данные, чтобы в дальнейшнем начинать поиск с указанной позиции
        last_found_id = 0

        #ищем поле, соответствующее влажности формат: 000|00|00 в скопированной строке
        for i in range(last_found_id, first_row.size):
            value = str(first_row.iloc[i])
            matches = re.findall('^\d{1,3}$', value)
            if len(matches) > 0:
                #вставляем Humidity (влажность) в dataframe
                dataframe.insert(3, 'Humidity', csv_dataframe.iloc[:, i])
                last_found_id = i
                break

        #ищем поле, указывающее направление ветра (содержит либо Ветер либо Штиль)
        for i in range(last_found_id, first_row.size):
            value = str(first_row.iloc[i])
            matches = re.findall('^Ветер|^Штиль', value)
            if len(matches) > 0:
                #вставляем WindDirection (направление ветра) в dataframe
                dataframe.insert(4, 'WindDirectionID', csv_dataframe.iloc[:, i])
                #вставляем WindSpeed (скорость ветра), т.к. это поле идет следующим
                dataframe.insert(5, 'WindSpeed', csv_dataframe.iloc[:, i+1])
                last_found_id = i+1
                break
        
        #если в dataframe есть поле RRR (осадки), то забираем еще и их иначе, в выходном dataframe будет пустое поле
        if 'RRR' in csv_dataframe.columns.values:
            dataframe.insert(6, 'Precipitation', csv_dataframe['RRR'])
        else:
            dataframe.insert(6, 'Precipitation', [None]*dataframe.shape[0])

        return self.__normalize_rp5_metrics_dataframe(dataframe, city)


    def __normalize_rp5_metrics_dataframe(self, df: pd.DataFrame, city: str) -> pd.DataFrame:
        """Приводит датафрейм в вид для дальнейшней обработки с корректными типами данных"""
        #преобразование всех столбцов в наиболее подходящий тип данных
        # csv_dataframe = csv_dataframe.convert_dtypes()
        
        #функция для приведения поля LocalTime
        def parse_datetime(x: str) -> datetime:
            dt = datetime.strptime(x, '%d.%m.%Y %H:%M')
            return dt
        #парсинг первого столбца с датами и конвертация в datetime
        df['LocalTime'] = df['LocalTime'].apply(parse_datetime)

        #вставляем поле с городом в столбец с индексом 1
        df.insert(1, 'CityID', [city]*df.shape[0])

        df['Temperature'] = df['Temperature'].apply(pd.to_numeric)
        #выбираем все данные, в которых Temperature не равняется nan
        for i, value in enumerate(df['Temperature']):
            if math.isnan(value):
                df.drop([i], inplace=True)
        """сделать так, что записи с NaN не участвовали в выборке"""
        df['Pressure'] = df['Pressure'].apply(pd.to_numeric)

        #функция для приведения поля Humidity
        def to_float_from_percentage(x: str) -> float:
            try:
                x = float(x)
                return x / 100
            except:
                return None       
        #приведение поля с влажностью Humidity к числу 0 < x < 1
        df['Humidity'] = df['Humidity'].apply(to_float_from_percentage) #преобразование типа данных к числовому

        #функция для приведения поля WindDirection
        def parse_wind_directions(field_value: str) -> str:
            pattern = f'север|восток|юг|запад|штиль'
            #находим все совпадения
            matches = re.findall(pattern, field_value.lower())
            if len(matches) > 0:
                direction = ''.join(x[0] for x in matches).upper()
            else:
                direction = 'С'
            return direction
        #приведение поля WindDirection
        df['WindDirectionID'] = df['WindDirectionID'].apply(parse_wind_directions)
    
        df['WindSpeed'] = df['WindSpeed'].apply(pd.to_numeric)

        #приводим поле осадков к требуемому виду
        def parse_precipitation(x: str) -> float:
            try:
                return float(x)
            except Exception as ex:
                if x != None:
                    item = x.lower()
                    if item.find('осадк') != -1:
                        return 1
                    else:
                        return 0
                else:
                    return None
            
        df['Precipitation'] = df['Precipitation'].apply(parse_precipitation)
        return df
                

    def normalize_metrics_to_database(self, df: pd.DataFrame, db: SqlServer) -> pd.DataFrame:
        """Приводит датафрейм метрик в вид для базы данных"""
        
        #получаем id городов
        city_df = db.get_table_data('Cities')
        city_list = city_df['City'].values
        city_id_list = city_df['ID'].values
        #заменяем названия городов на их id
        df['CityID'].replace(city_list, city_id_list, inplace=True)
        
        #получаем id направлений сторон света
        direction_df = db.get_table_data('WindDirections')
        #формируем 2 списка значений со сторонами света
        direction_list = direction_df['Direction'].values
        direction_id_list = direction_df['ID'].values
        #заменяем стороны света на их id
        df['WindDirectionID'].replace(direction_list, direction_id_list, inplace=True)
        print(df.head(15))

        return df