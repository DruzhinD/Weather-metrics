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


    def select_metrics_by_date(self, start: datetime, finish: datetime):
        """Получение метрик по дате и времени"""

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
        WHERE Metrics.LocalTime BETWEEN '{1}' AND '{2}'
        ORDER BY Metrics.LocalTime
        """.format(', '.join(fields), start.strftime('%Y-%d-%m'), finish.strftime('%Y-%d-%m'))

        return self.manual_query(query)