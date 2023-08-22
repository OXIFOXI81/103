from pprint import pprint
import sqlalchemy
import sqlalchemy as sq
from psycopg2.sql import SQL

from sqlalchemy.orm import declarative_base, relationship, sessionmaker
import json
import datetime

from sqlalchemy.sql.operators import or_

from models import Publisher, create_tables,Book, Shop, Stock, Sale


def Tables_fill_From_JSON(session):
    with open("tests_data.json", encoding='utf - 8', newline="") as f:
        json_a = json.load(f)
        for news in json_a:
            if news['model']=='publisher':
                pk=news['pk']
                name=news['fields']['name']
                obj = Publisher(publisher_id=pk,publisher_name=name)
            elif   news['model']=='book':
                pk=news['pk']
                name=news['fields']['title']
                idp=news['fields']['id_publisher']
                obj = Book(book_id=pk,title=name, publisher_id=idp)
            elif news['model'] == 'shop':
                pk = news['pk']
                name = news['fields']['name']
                obj = Shop(shop_id=pk, shop_name=name)
            elif news['model'] == 'stock':
                pk = news['pk']
                count = news['fields']['count']
                idb = news['fields']['id_book']
                ids = news['fields']['id_shop']
                obj=Stock(stock_id=pk,book_id=idb, shop_id=ids, count=count)
            else:
                pk = news['pk']
                count = news['fields']['count']
                ids = news['fields']['id_stock']
                date = news['fields']['date_sale']
                price= news['fields']['price']
                obj=Sale(id=pk,price=price, count=count, stock_id=ids, data_sale=date)
            session.add(obj)
            session.commit()


def Query_execute_json(session,publ_input):
    if publ_input.isdigit():
        publ_int=publ_input
        publ_str=""
    else:
        publ_int = 0
        publ_str = publ_input
    q = session.query(Book.title, Shop.shop_name, Sale.price, Sale.data_sale).join(Publisher).join(Stock).join(Shop).join(Sale).filter(or_(Publisher.publisher_name==publ_str,Publisher.publisher_id==publ_int))
    for s in q.all():
     print(f"{s.title:40} | {s.shop_name:15} | {s.price:10} | {s.data_sale}")

if __name__ == '__main__':
     DSN = "postgresql://postgres:post_oxana@localhost:5432/oxana_db"
     engine = sqlalchemy.create_engine(DSN)
     create_tables(engine)
     Session = sessionmaker(bind=engine)
     session = Session()
     Tables_fill_From_JSON(session)
     publ_input = input("Введите id издателя или его название: ")
     Query_execute_json(session,publ_input)


