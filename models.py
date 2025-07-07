import os
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker, scoped_session
from dotenv import load_dotenv

class Database:
    def __init__(self):
        load_dotenv()  # Ensure env variables are loaded regardless of import order
        
        self.database_url = os.getenv("DATABASE_URL")
        if not self.database_url:
            raise Exception("DATABASE_URL environment variable not set.")
        
        self.engine = create_engine(self.database_url)
        self.SessionLocal = scoped_session(sessionmaker(bind=self.engine))
        self.Base = declarative_base()

        self.define_models()
    
    def define_models(self):
        class FileRecord(self.Base):
            __tablename__ = 'files'
            id = Column(Integer, primary_key=True)
            file_name = Column(String, nullable=False)
        
        self.FileRecord = FileRecord
    
    def init_db(self):
        self.Base.metadata.create_all(bind=self.engine)
    
    def get_session(self):
        return self.SessionLocal()
