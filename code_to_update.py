from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import select
from statistics import mean, pstdev
import csv

Base = declarative_base()


# Configuration de la base de données
engine = create_engine('sqlite:///books.db')
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

class Book(Base):
    __tablename__ = "Book"
    isbn = Column(String, primary_key=True)
    title = Column(String)
    author = Column(String)
    year_publication = Column(Integer)
    
    def __repr__(self) -> str:
        return f"{self.title}[{self.author}]({self.year_publication})"
    
    @classmethod
    def get_stats(cls, session, isbn):
        request = select(Rating.book_rating).where(Rating.isnb == isbn)
        data = session.execute(request).all()
        values = [int(i._mapping['book_rating']) for i in data]
        print(f"l'le livre n°{isbn} a fais {len(values)} avis, ayant pour moyenne {mean(values)} et pour ecart_type {pstdev(values)}")

class User(Base):
    __tablename__ = "User"
    userId = Column(String, primary_key=True)
    location = Column(String)
    age = Column(String)
    
    @classmethod
    def get_stats(cls, session, userID):
        request = select(Rating.book_rating).where(Rating.userId == userID)
        data = session.execute(request).all()
        values = [int(i._mapping['book_rating']) for i in data]
        print(f"l'utilisateur {userID} a fais {len(values)} avis, ayant pour moyenne {mean(values)} et pour ecart_type {pstdev(values)} ")
        

class Rating(Base):
    __tablename__ = "Rating"
    id = Column(Integer, primary_key=True, autoincrement=True)
    userId = Column(String)
    isnb = Column(String)
    book_rating = Column(String)

csv_book_path = 'files/books.csv'
csv_users_path = 'files/users.csv'
csv_rating_path = 'files/ratings.csv'

def import_ratings_from_csv(csv_file_path):
    with open(csv_file_path, newline='', encoding='latin-1') as csvfile:
        csvreader = csv.DictReader(csvfile,quotechar='"', delimiter=';') # to Update???
        for row in csvreader:
            rating = Rating(
                userId=row['User-ID'], isnb=row['ISBN'], 
                book_rating=row['Book-Rating']
            )
            session.add(rating)
        session.commit()

def import_users_from_csv(csv_file_path):
    with open(csv_file_path, newline='', encoding='latin-1') as csvfile:
        csvreader = csv.DictReader(csvfile,quotechar='"', delimiter=';') # to Update???
        for row in csvreader:
            user = User(
                userId=row['User-ID'], location=row['Location'], 
                age=row['Age']
            )
            session.add(user)
        session.commit()

def import_books_from_csv(csv_file_path):
    with open(csv_file_path, newline='', encoding='latin-1') as csvfile:
        csvreader = csv.DictReader(csvfile,quotechar='"', delimiter=';') # to Update???
        for row in csvreader:
            book = Book(
                isbn=row['ISBN'], title=row['Book-Title'], 
                author=row['Book-Author'], year_publication=row['Year-Of-Publication']
            )
            session.add(book)
        session.commit()

# Chemin vers le fichier CSV
#if not inspect(engine).has_table('Book'):
 #   import_books_from_csv(csv_book_path)
#import_ratings_from_csv(csv_rating_path)
#import_users_from_csv(csv_users_path)
#data = session.query(Book).filter(Book.title.contains('The Hobbit')).all()
#for toto in data:
#    print(toto)
User.get_stats(session=session, userID="276747")
Book.get_stats(session=session, isbn="0345339703")
session.close()