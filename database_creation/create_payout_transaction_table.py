import os
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, ForeignKey, Text, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv('SUPABASE_URL')

# Create SQLAlchemy engine and base
engine = create_engine(DATABASE_URL)
Base = declarative_base()

# Define Payout model
class Payout(Base):
    __tablename__ = 'payout'
    
    id = Column(BigInteger, primary_key=True)
    date = Column(Date, nullable=False)
    status = Column(String(50), nullable=False)
    total = Column(Float, nullable=False)
    bank_reference = Column(String(100), nullable=True)
    charges_total = Column(Float, nullable=True)
    refunds_total = Column(Float, nullable=True)
    fees_total = Column(Float, nullable=True)
    currency = Column(String(10), nullable=False)
    
    # Relationship with transactions
    transactions = relationship("PayoutTransaction", back_populates="payout")

# Define PayoutTransaction model
class PayoutTransaction(Base):
    __tablename__ = 'payout_transaction'
    
    id = Column(BigInteger, primary_key=True)
    payout_id = Column(BigInteger, ForeignKey('payout.id'), nullable=False)
    date = Column(Date, nullable=True)
    order_id = Column(BigInteger, nullable=True)
    order_name = Column(String(100), nullable=True)
    type = Column(String(50), nullable=False)
    amount = Column(Float, nullable=False)
    fee = Column(Float, nullable=False)
    net = Column(Float, nullable=False)
    currency = Column(String(10), nullable=False)
    
    # Relationship with payout
    payout = relationship("Payout", back_populates="transactions")

def create_tables():
    # Create tables
    Base.metadata.create_all(engine)
    print("Payout and PayoutTransaction tables created successfully")

if __name__ == "__main__":
    create_tables()
