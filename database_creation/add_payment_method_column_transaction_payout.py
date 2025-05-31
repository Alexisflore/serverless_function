import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get Supabase credentials
db_host = os.getenv('SUPABASE_HOST', 'db.nybxcfjjnkgxzgaeitlk.supabase.co')
db_password = os.getenv('SUPABASE_PASSWORD', '')
db_user = os.getenv('SUPABASE_USER', 'postgres')
db_name = os.getenv('SUPABASE_DB_NAME', 'postgres')
db_port = os.getenv('SUPABASE_PORT', '5432')

# Create proper PostgreSQL connection string
DATABASE_URL = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)

def add_payment_method_column():
    # Connect to the database
    with engine.connect() as connection:
        # Create a transaction
        with connection.begin():
            # Add payment_method_name column to payout_transaction table
            connection.execute(
                text("ALTER TABLE payout_transaction ADD COLUMN IF NOT EXISTS payment_method_name VARCHAR(100)")
            )
        
        print("Successfully added payment_method_name column to payout_transaction table")

if __name__ == "__main__":
    add_payment_method_column()
