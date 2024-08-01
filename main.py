from WeatherSqlServer import WeatherSqlServer
from datetime import datetime
from rp5 import Rp5Observer

#загрузка метрик в бд
def main():
    #баг: поиск происходит корректно, однако выбор ссылки происходит неверный логически
    #скрипт берем ссылку на первый архив - архив погоды в Шереметьево, который не есть Москва
    city = 'Москва'
    start = datetime(2024, 1, 1)
    finish = datetime(2024, 5, 31)

    rp5_observer = Rp5Observer()
    csv_path = rp5_observer.download_weather_csv(start, finish, city)
    csv_df = rp5_observer.weather_csv_to_dataframe(csv_path)
    print(csv_df.head(5))
    print('-'*20)
    data_base = WeatherSqlServer('WeatherDB', 'DMITRIY-LAPTOP\\SQLEXPRESS01')
    new_df = rp5_observer.normalize_metrics_to_database(csv_df, data_base)
    print(new_df.head(5))
    
    print('-'*20)
    print(new_df.dtypes)
    data_base.insert_data_to_table("Metrics", new_df, 200)
    print('готово')
    

if __name__ == '__main__':
    main()