import pyodbc #импорт модуля для БД
import pandas, numpy as np
from pandas import DataFrame

class SqlServer():
    def __init__(self, database: str, server) -> None:
        #строка подключения
        #для того, чтобы понять, какой драйвер вводить - вбить в поиск ODBC -> fileDSN -> Add
        self.cnxn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                                   'Server='+server+';'
                                   'Database='+database+';'
                                   'Trusted_connection=yes;')


    def manual_query(self, query: str) -> DataFrame:
        """позволяет отправить произвольный запрос к БД"""
        return pandas.read_sql(query, self.cnxn)


    def get_table_data(self, table: str, *fields) -> DataFrame:
        """Вывод информации из таблицы"""
        # cursor = self.cnxn.cursor() #создание курсора выполнения
        
        if len(fields) == 0:
            query = 'SELECT * FROM {0}'.format(table)
        else:
            query = 'SELECT {0} FROM {1}'.format(', '.join(fields), table)
        
        return self.manual_query(query)


    def insert_data_to_table(self, table: str, data: DataFrame, batchsize: int):
        """добавление данных в указанную таблицу"""
        #достаем необходимые поля таблицы из БД
        query = f"SELECT TOP 0 * FROM {table}"
        table_df = pandas.read_sql(query, self.cnxn)
        table_fields = list(table_df.columns)
        table_fields.remove('ID')

        #активируем курсор
        cursor = self.cnxn.cursor()
        cursor.fast_executemany = True #активируем быстрое выполнение
        #формируем запрос
        insert_query = f" INSERT INTO {table} ({', '.join(table_fields)}) VALUES ({','.join(['?']*len(table_fields))})"
        
        # вставляем батчи (группа запросов/SQL операторов) с шагом batchsize
        for b in range(0, data.shape[0], batchsize):
            inserted = 0 #количество загруженных записей
            if (b+batchsize) > data.shape[0]:
                batch = data[b: data.shape[0]].values.tolist()
                inserted = data.shape[0] - b
            else:
                batch = data[b: b+batchsize].values.tolist()
                inserted = batchsize
            # #заменяем N/A на None
            for i in range(len(batch)):
                for j in range(len(batch[i])):
                    if isinstance(batch[i][j], pandas._libs.missing.NAType):
                        batch[i][j] = None
            #запускаем вставку батча
            cursor.executemany(insert_query, batch)
            self.cnxn.commit()
            print(f'Загружено в БД {inserted} записей')
        print("выполнено!")


    def run_stored_function(self, procedure: str) -> DataFrame:
        """Вызов хранимой процедуры и получение из неё результата"""
        query = 'EXECUTE {0}'.format(procedure)
        return self.manual_query(query)