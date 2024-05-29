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
        request = select(Author).order_by(func.random()).limit(number)
        data = session.execute(request).all()
        return (i._mapping['Author'] for i in data)
    
class Loan(Base):
    __tablename__ = 'Loan'
    id = Column(Integer, primary_key=True, autoincrement=True)
    book_isbn = Column(String, ForeignKey('Book.isbn'), nullable=False)
    userId = Column(String, ForeignKey('User.userId'), nullable=False)
    load_date = Column(String, nullable=False)
    return_date = Column(String, nullable=True)
    status = Column(String, nullable=False)
    
    def __repr__(self) -> str:
        return f"le loan n°{self.id} du livre {self.book_isbn} de l'user {self.userId} emprunté le {self.load_date}, rendu le {self.return_date} status: {self.status}"
        
    
    def end_loan(self, session):
        self.return_date = date.today().isoformat()
        self.status = "Returned"
        request_book = select(Book).where(Book.isbn == self.book_isbn)
        request_user = select(User).where(User.userId == self.userId)
        book = session.execute(request_book).first()._mapping['Book']
        user = session.execute(request_user).first()._mapping['User']
        book.available = True
        user.loan_available += 1
        session.commit()
    
    @classmethod
    def create_loan(cls, session, book: Book, user: User, load_date: date) -> Self:
        if user.loan_available > 0:
            if book.available:
                loan = Loan(book_isbn=book.isbn, userId = user.userId,
                            load_date=load_date.isoformat(), status="Loan")
                book.available = False
                user.loan_available -= 1
                session.add(loan)
                session.commit()
                return loan
            else:
                print(f"Error: the book {book.title} isn't available")
        else:
            print(f'Error: the user {user.userId} cannot make another loan')
        return 
            
    @classmethod
    def get_loans(cls, session) -> (Self):
        request = select(Loan)
        data = session.execute(request).all()
        return (i._mapping['Loan'] for i in data)
    

csv_book_path = 'files/books.csv'
csv_users_path = 'files/users.csv'
csv_rating_path = 'files/ratings.csv'




def import_ratings_from_csv(csv_file_path):
    print("Création de la table Rating")
    with open(csv_file_path, newline='', encoding='latin-1') as csvfile:
        lines = len(csvfile.readlines())  
    with open(csv_file_path, newline='', encoding='latin-1') as csvfile:
        csvreader = csv.DictReader(csvfile,quotechar='"', delimiter=';') # to Update???
        for row in tqdm(csvreader, total=lines):
            rating = Rating(
                userId=row['User-ID'], isnb=row['ISBN'], 
                book_rating=row['Book-Rating']
            )
            session.add(rating)
        session.commit()

def import_users_from_csv(csv_file_path):
    print('Création de la table User')
    with open(csv_file_path, newline='', encoding='latin-1') as csvfile:
        lines = len(csvfile.readlines())  
    with open(csv_file_path, newline='', encoding='latin-1') as csvfile:
        csvreader = csv.DictReader(csvfile,quotechar='"', delimiter=';') # to Update???
        for row in tqdm(csvreader, total=lines):
            user = User(
                userId=row['User-ID'], location=row['Location'], 
                age=row['Age'], loan_available = 5
            )
            session.add(user)
        session.commit()

def import_books_from_csv(csv_file_path):
    print("Création de la table Book")
    with open(csv_file_path, newline='', encoding='latin-1') as csvfile:
        lines = len(csvfile.readlines())  
    with open(csv_file_path, newline='', encoding='latin-1') as csvfile:
        csvreader = csv.DictReader(csvfile,quotechar='"', delimiter=';') # to Update???
        for row in tqdm(csvreader, total=lines):
            book = Book(
                isbn=row['ISBN'], title=row['Book-Title'],
                author=row['Book-Author'], year_publication=row['Year-Of-Publication'], available=True
            )
            session.add(book)
        session.commit()

def create_author_from_book():
    request = select(Book.author).distinct()
    data = session.execute(request).all()
    authors = [i._mapping['author'] for i in data]
    fake = Faker('en_US')
    print("Création de la table Author")
    for author in tqdm(authors, total=len(authors)):
        element = Author(name=author,
                         birth_year=fake.date_between(start_date=date.fromisoformat('1900-01-01'), end_date=date.fromisoformat('1940-01-01')).year,
                         death_year=fake.date_between(start_date=date.fromisoformat('1970-01-01'), end_date=date.fromisoformat('2001-01-01')).year,
                         nationality=fake.country())
        session.add(element)
    session.commit()
    
if __name__ == "__main__":
    engine = create_engine('sqlite:///books.db', echo=False)
    inspector = inspect(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    if inspector.get_table_names() == []:
        print("création des elements")
        Base.metadata.create_all(engine)
        import_users_from_csv(csv_users_path)
        import_books_from_csv(csv_book_path)
        import_ratings_from_csv(csv_rating_path)
        create_author_from_book()
    
    print("----- Print of the books containing 'The Hobbit' -----")
    books = Book.get_books_by_name(session, 'The Hobbit')
    for book in books:
        print(book)
    print('----------------')
    print('----- Print of one random book and his ratings data -----')
    book = next(Book.get_random_books(1))
    print(book)
    book.get_stats(session)
    print('----- Print of one random user and his ratings data -----')
    user = next(User.get_random_users(1))
    print(user)
    user.get_stats(session)
    loan = Loan.create_loan(session, book, user, date.today())
    print(loan)
    loan.end_loan(session=session)
    print(loan)
    print(book)
    
    print('----- Print of the books of 3 random Authors -----')
    authors = Author.get_random_authors(3)
    for author in authors:
        author.get_books(session)

#data = session.query(Book).filter(Book.title.contains('The Hobbit')).all()
#for toto in data:
#    print(toto)
#User.get_stats(session=session, userID="276747")
#Book.get_stats(session=session, isbn="0345339703")


#session.close()