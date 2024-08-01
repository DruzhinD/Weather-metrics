from SqlServer_db import SqlServer
import pandas
from datetime import datetime


class WeatherSqlServer(SqlServer):
    def __init__(self, database: str, server) -> None:
        super().__init__(database, server)


    def select_all_metrics(self) -> pandas.DataFrame:
        """Позволяет получить все метрики из базы данных"""

        fields = ['LOCALTIME', 'Temperature', 'TemperatureMax', 'TemperatureMin', 'Pressure', 'Humidity',
                   'WindSpeed', 'Сloudiness', 'HorizontalVisibility', 'TemperatureDewPoint', 'Precipitation']
        for i in range(len(fields)):
            fields[i] = 'Metrics.' + fields[i]
        fields.append('WindDirections.Direction')
        fields.append('Cities.City')
        query =  """
        SELECT {0} FROM Metrics
        JOIN Cities ON Metrics.CityID = Cities.ID
        JOIN WindDirections ON Metrics.WindDirectionID = WindDirections.ID
        """.format(', '.join(fields))

        return self.manual_query(query)


    def select_metrics(self, start: datetime, finish: datetime, city: str = None):
        """Получение метрик по дате и времени \n
        ----
        ## Параметры
        start: datetime
            Начальный период выборки
        finish: datetime
            Конечный период выборки
        city: str, optional
            Город выборки
        """

        fields = ['LOCALTIME', 'Temperature', 'Pressure', 'Humidity',
                   'WindSpeed', 'Precipitation']
        for i in range(len(fields)):
            fields[i] = 'Metrics.' + fields[i]
        fields.append('WindDirections.Direction')
        fields.append('Cities.City')
        query =  """
        SELECT {0} FROM Metrics
        JOIN Cities ON Metrics.CityID = Cities.ID
        JOIN WindDirections ON Metrics.WindDirectionID = WindDirections.ID
        WHERE Metrics.LocalTime BETWEEN '{1}' AND '{2}'
        ORDER BY Metrics.LocalTime
        """.format(', '.join(fields), start.strftime('%Y-%d-%m'), finish.strftime('%Y-%d-%m'))
        
        #если город был передан в качестве параметра, то вставляем это условие в запрос
        if city != None:
            index = query.find('ORDER')
            query = query[:index] + f"AND Cities.City = '{city}'\n" + query[index:]

        df = self.manual_query(query)
        #удаляем ненужные столбцы
        # df.drop(columns=['ID', 'CityID', 'WindDirectionID'], inplace=True)
        return df