import json
import os

import sqlalchemy as sq

import models


class BooksSales:
    def __init__(self, db_host, db_name, db_user, db_user_pass, db_schema) -> None:
        self._connect_to_db_postgresql(db_host, db_name, db_user, db_user_pass, db_schema)
        self._create_tables()
    
    def _connect_to_db_postgresql(self, db_host, db_name, db_user, db_user_pass, db_schema):
        # create engine
        DSN = f'postgresql://{db_user}:{db_user_pass}@{db_host}:5432/{db_name}'
        self.engine = sq.create_engine(DSN, connect_args={'options': f'-csearch_path={db_schema}'})
        # create session
        Session = sq.orm.sessionmaker(bind=self.engine)
        self.session = Session()

    def _create_tables(self):
        models.create_tables(self.engine)
    
    def _load_data(self, data):
        for item in data:
            model = models.models_by_tablename[item['model']]
            self.session.add(model(id=item['pk'], **item['fields']))
        self.session.commit()

    def load_data_from_json(self, file_path):
        with open(file_path, 'r') as json_file:
            data_from_file = json.load(json_file)
        self._load_data(data_from_file)

    def get_sales_by_publisher(self, publisher_info):
        try:
            publisher_q = self.session.query(models.Publisher).filter(models.Publisher.id == int(publisher_info))
        except:
            publisher_q = self.session.query(models.Publisher).filter(models.Publisher.name == publisher_info)
        publisher = publisher_q.one()
        sales = self.session.query(models.Book).join(models.Stock).join(models.Shop).join(models.Sale).filter(models.Book.id_publisher == publisher.id)
        sales_list = []
        col_len = [0] * 4
        for book in sales.all():
            for stock in book.stocks:
                for sale in stock.sales:
                    item = [book.title, stock.shop.name, sale.price, sale.date_sale.strftime('%d-%m-%Y')]
                    sales_list.append(item)
                    for i in range(4):
                        col_len[i] = max(col_len[i], len(str(item[i])))
        for item in sales_list:
            print(*[str(item[i]).ljust(col_len[i]) for i in range(4)], sep=' | ')

if __name__ == '__main__':
    # Для задания значения переменной окружения может быть использована команада
    # $Env:DB_NAME='<value>'  
    db_username = os.getenv('DB_USERNAME')
    db_pass = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')
    db_host = os.getenv('DB_HOST')
    db_schema = os.getenv('DB_SCHEMA')

    # print(f'username = {db_username}\npassword = {db_pass}\ndatabase = {db_name}\nhost={db_host}\nschema = {db_schema}')

    # подключиться к БД, создать модели и схему по ним
    books_sales = BooksSales(db_host, db_name, db_username, db_pass, db_schema)
    # загрузить тестовые данные
    books_sales.load_data_from_json('tests_data.json')
    # вывод фактов покупки книг заданного издателя
    books_sales.get_sales_by_publisher(input('Введите имя или идентификатор издателя для вывода фактов покупки его книг:\n'))