import os
import json
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from create_payout_transaction_table import engine, Payout, PayoutTransaction

def import_payout_data(json_file_path):
    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Read JSON data
        with open(json_file_path, 'r') as f:
            payouts_data = json.load(f)
        
        print(f"Importing {len(payouts_data)} payouts...")
        
        # Process each payout
        for payout_data in payouts_data:
            # Create payout record
            payout = Payout(
                id=payout_data['id'],
                date=datetime.strptime(payout_data['date'], "%Y-%m-%d").date(),
                status=payout_data['status'],
                total=payout_data['summary']['total'],
                bank_reference=payout_data['summary']['bank_reference'],
                charges_total=payout_data['summary']['charges_total'],
                refunds_total=payout_data['summary']['refunds_total'],
                fees_total=payout_data['summary']['fees_total'],
                currency=payout_data['summary']['currency']
            )
            
            # Add payout to session
            session.add(payout)
            
            # Process transactions for this payout
            for transaction_data in payout_data['transactions']:
                # Convert date string to date object if available
                transaction_date = None
                if transaction_data['date']:
                    transaction_date = datetime.strptime(transaction_data['date'], "%Y-%m-%d").date()
                
                # Create transaction record
                transaction = PayoutTransaction(
                    id=transaction_data['id'],
                    payout_id=payout_data['id'],
                    date=transaction_date,
                    order_id=transaction_data['order_id'],
                    order_name=transaction_data['order_name'],
                    type=transaction_data['type'],
                    amount=transaction_data['amount'],
                    fee=transaction_data['fee'],
                    net=transaction_data['net'],
                    currency=transaction_data['currency']
                )
                
                # Add transaction to session
                session.add(transaction)
        
        # Commit all changes to database
        session.commit()
        print("Data import completed successfully")
        
    except Exception as e:
        # Rollback in case of error
        session.rollback()
        print(f"Error importing data: {str(e)}")
    
    finally:
        # Close session
        session.close()

if __name__ == "__main__":
    # Path to JSON file relative to script location
    current_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
    json_file_path = os.path.join(root_dir, 'versements_deposited_format_specifique.json')
    
    # Import data
    import_payout_data(json_file_path) 