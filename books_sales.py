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
            print(item['model'])
            model = models.models_by_tablename[item['model']]
            print(type(model))
            self.session.add(model(id=item['pk'], **item['fields']))
        self.session.commit()

    def load_data_from_json(self, file_path):
        with open(file_path, 'r') as json_file:
            data_from_file = json.load(json_file)
        self._load_data(data_from_file)

    def get_sales_by_publisher(self, publisher_name):
        pass

if __name__ == '__main__':
    # Для задания значения переменной окружения может быть использована команада
    # $Env:DB_NAME='<value>'  
    db_username = os.getenv('DB_USERNAME')
    db_pass = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')
    db_host = os.getenv('DB_HOST')
    db_schema = os.getenv('DB_SCHEMA')

    print(f'username = {db_username}\npassword = {db_pass}\ndatabase = {db_name}\nhost={db_host}\nschema = {db_schema}')

    books_sales = BooksSales(db_host, db_name, db_username, db_pass, db_schema)
    books_sales.load_data_from_json('tests_data.json')