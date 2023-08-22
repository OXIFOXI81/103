import sqlalchemy
import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()


class Publisher(Base):
    __tablename__ = "publisher"
    publisher_id = sq.Column(sq.Integer, primary_key=True)
    publisher_name = sq.Column(sq.String(length=40), unique=True)

class Book(Base):
    __tablename__ = "book"
    book_id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=60), nullable=False)
    publisher_id = sq.Column(sq.Integer, sq.ForeignKey("publisher.publisher_id"), nullable=False)

    publisher = relationship(Publisher, backref="books")

class Shop(Base):
    __tablename__ = "shop"

    shop_id = sq.Column(sq.Integer, primary_key=True)
    shop_name = sq.Column(sq.String(length=40), unique=True)

class Stock(Base):
         __tablename__ = "stock"
         stock_id = sq.Column(sq.Integer, primary_key=True)
         book_id = sq.Column(sq.Integer, sq.ForeignKey("book.book_id"), nullable=False)
         shop_id = sq.Column(sq.Integer, sq.ForeignKey("shop.shop_id"), nullable=False)
         count = sq.Column(sq.Integer, nullable=False)

         book = relationship(Book, backref="stocks")
         shop = relationship(Shop, backref="stocks")


class Sale(Base):
    __tablename__ = "sale"
    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Float, nullable=False)
    count = sq.Column(sq.Integer, nullable=False)
    stock_id = sq.Column(sq.Integer, sq.ForeignKey("stock.stock_id"), nullable=False)
    data_sale = sq.Column(sq.DATE, nullable=False)
    stock = relationship(Stock, backref="sales")

def create_tables(engine):
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)





