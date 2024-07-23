from WeatherSqlServer import WeatherSqlServer
from datetime import datetime
from rp5 import Rp5Observer

#загрузка метрик в бд
def main():
    #пока что работает только с г. Хабаровск
    rp5_observer = Rp5Observer()
    csv_path = rp5_observer.download_weather_csv(datetime(2024, 1, 1), datetime(2024, 1, 10), 'Хабаровск')
    csv_df, city = rp5_observer.weather_csv_to_dataframe(csv_path)
    print(csv_df.head(5))
    print('-'*20)
    data_base = WeatherSqlServer('WeatherDB', 'HOME-PC')
    new_df = rp5_observer.normalize_rp5_metrics_dataframe(csv_df, city, data_base)
    print(new_df.head(5))
    
    data_base.insert_data_to_table("Metrics", new_df, 200)

if __name__ == '__main__':
    main()