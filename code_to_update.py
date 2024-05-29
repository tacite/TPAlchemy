from sqlalchemy import Column, Integer, String, Boolean, create_engine
from sqlalchemy import ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import select
from statistics import mean, pstdev
from sqlalchemy.sql.expression import func
from sqlalchemy import inspect
from faker import Faker
from datetime import date
from typing import Self
from tqdm import tqdm
import csv

Base = declarative_base()


# Configuration de la base de données

class Book(Base):
    __tablename__ = "Book"
    id = Column(Integer, primary_key=True, autoincrement=True)
    isbn = Column(String, nullable=False)
    title = Column(String, nullable=False)
    author = Column(String, nullable=False)
    year_publication = Column(Integer, nullable=False)
    available = Column(Boolean)
    
    def __repr__(self) -> str:
        return f"{self.title}[{self.author}]({self.year_publication}) -> {"available" if self.available else "not available"}"
    
    
    def get_stats(self, session):
        request = select(Rating.book_rating).where(Rating.isnb == self.isbn)
        data = session.execute(request).all()
        values = [int(i._mapping['book_rating']) for i in data]
        print(f"l'le livre n°{self.isbn} a fais {len(values)} avis, ayant pour moyenne {mean(values)} et pour ecart_type {pstdev(values)}")

    @classmethod
    def get_random_books(cls,  number: int) -> (Self):
        request = select(Book).order_by(func.random()).limit(number)
        data = session.execute(request).all()
        return (i._mapping['Book'] for i in data)
    
    @classmethod
    def get_books_by_name(cls, session, substring: str) -> (Self):
        request = select(Book).filter(Book.title.contains(substring))
        data = session.execute(request).all()
        return(i._mapping['Book'] for i in data)

class User(Base):
    __tablename__ = "User"
    userId = Column(String, primary_key=True)
    location = Column(String, nullable=False)
    age = Column(String, nullable=True)
    loan_available = Column(Integer, nullable=False)
    
    def __repr__(self) -> str:
        return f'id: {self.userId} location: {self.location} age: {self.age}'
    
    def get_stats(self, session):
        request = select(Rating.book_rating).where(Rating.userId == self.userId)
        data = session.execute(request).all()
        values = [int(i._mapping['book_rating']) for i in data]
        if len(values) == 0:
            print(f"l'utilisateur {self.userId} n'as pas mis d'avis pour le moment")
        else:
            print(f"l'utilisateur {self.userId} a fais {len(values)} avis, ayant pour moyenne {mean(values)} et pour ecart_type {pstdev(values)} ")

    @classmethod
    def get_random_users(cls, number: int) -> (Self):
        request = select(User).order_by(func.random()).limit(number)
        data = session.execute(request).all()
        return (i._mapping['User'] for i in data)


class Rating(Base):
    __tablename__ = "Rating"
    id = Column(Integer, primary_key=True, autoincrement=True)
    userId = Column(String, nullable=False)
    isnb = Column(String, nullable=False)
    book_rating = Column(String, nullable=False)
    
class Author(Base):
    __tablename__ = "Author"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, ForeignKey("Book.author"), nullable=False)
    birth_year = Column(Integer, nullable=False)
    death_year = Column(Integer, nullable=False)
    nationality = Column(String, nullable=False)
    
    def get_books(self, session):
        print(f'----- {self.name} -----')
        request = select(Book).where(Book.author == self.name)
        values = session.execute(request).all()
        for book in values:
            print(book._mapping['Book'])
    
    @classmethod
    def get_random_authors(cls, number: int) -> (Self):
        request = select(Author).order_by(func.random()).limit