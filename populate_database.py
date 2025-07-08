import os
import json
import requests
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
import random
import copy

from sqlalchemy import create_engine, Column, Integer, String, Text, Float, Date, DateTime, ForeignKey, JSON
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.sql import func

# --- Configuration ---
# Load from environment variables for security and flexibility

load_dotenv()  # Load environment variables from .env file
API_URL = "https://kgsearch.googleapis.com/v1/entities:search"
API_KEY = os.getenv("GOOGLE_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

# Static list of entities to track
ENTITIES = [
    "ClutchPoints", "Yahoo Sports", "The Sporting News", "Bleacher Report",
    "FanSided", "ESPN", "CBS Sports", "Sports Illustrated", "FOX Sports",
    "SB Nation", "The Athletic", "Essentially Sports"
]

# --- SQLAlchemy Setup ---
Base = declarative_base()

class Entity(Base):
    __tablename__ = 'entities'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)
    
    # Relationships
    results = relationship("KnowledgeGraphDailyResult", back_populates="entity", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Entity(name='{self.name}')>"

class KnowledgeGraphDailyResult(Base):
    __tablename__ = 'knowledge_graph_daily_results'
    id = Column(Integer, primary_key=True)
    entity_id = Column(Integer, ForeignKey('entities.id'), nullable=False)
    result_score = Column(Float)
    name = Column(String(255))
    description = Column(Text)
    article_body = Column(Text)
    raw_json = Column(JSON)
    date = Column(Date, nullable=False, default=date.today)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # Relationships
    entity = relationship("Entity", back_populates="results")

    def __repr__(self):
        return f"<KnowledgeGraphDailyResult(name='{self.name}', date='{self.date}')>"

# --- API Fetching ---
def fetch_entity_data(entity_name):
    """Fetches data for a given entity from the Google Knowledge Graph API."""
    if not API_KEY:
        print("Error: GOOGLE_API_KEY environment variable not set.")
        return None

    params = {'query': entity_name, 'key': API_KEY, 'limit': 3}
    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {entity_name}: {e}")
        return None

# --- Main Logic ---
def main():
    """Main function to populate the database."""

    # --- Configuration ---
    # Load from environment variables for security and flexibility
    
    if not API_KEY:
        print("Error: GOOGLE_API_KEY not found in .env file or environment variables.")
        return

    if not DATABASE_URL:
        print("Error: DATABASE_URL not found in .env file or environment variables.")
        print("Please set it in your .env file, e.g.:")
        print("DATABASE_URL='mysql+pymysql://user:password@host/dbname'")
        return

    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    today = date.today()

    for entity_name in ENTITIES:
        # Get or create the entity record
        entity = session.query(Entity).filter_by(name=entity_name).first()
        if not entity:
            entity = Entity(name=entity_name)
            session.add(entity)
            session.commit()
            print(f"Created new entity: {entity_name}")

        # Check if results for this entity and date already exist
        existing_result = session.query(KnowledgeGraphDailyResult).filter_by(
            entity_id=entity.id,
            date=today
        ).first()

        if existing_result:
            print(f"Data for {entity_name} on {today} already exists. Skipping.")
            continue

        print(f"Fetching data for {entity_name}...")
        api_data = fetch_entity_data(entity_name)

        if api_data and 'itemListElement' in api_data:
            for item in api_data['itemListElement']:
                result = item.get('result', {})
                detailed_desc = result.get('detailedDescription', {})
                
                new_result = KnowledgeGraphDailyResult(
                    entity_id=entity.id,
                    result_score=item.get('resultScore'),
                    name=result.get('name'),
                    description=result.get('description'),
                    article_body=detailed_desc.get('articleBody'),
                    raw_json=item,
                    date=today
                )
                session.add(new_result)
            
            session.commit()
            print(f"Successfully stored {len(api_data['itemListElement'])} results for {entity_name}.")
        else:
            print(f"No data retrieved for {entity_name}.")

    session.close()
    print("\nDatabase population script finished.")


def backfill_data(session, source_date_str, start_date_str, end_date_str):
    """
    Temporarily backfills data from a source date to a target date range
    with a slightly modified result_score.
    """
    print("--- Starting data backfill process ---")
    
    source_date = date.fromisoformat(source_date_str)
    start_date = date.fromisoformat(start_date_str)
    end_date = date.fromisoformat(end_date_str)

    # 1. Fetch all records from the source date
    source_results = session.query(KnowledgeGraphDailyResult).filter_by(date=source_date).all()
    
    if not source_results:
        print(f"No source data found for {source_date}. Aborting backfill.")
        return

    print(f"Found {len(source_results)} records for source date {source_date}.")

    # 2. Iterate through the target date range
    current_date = start_date
    while current_date <= end_date:
        print(f"\nProcessing for target date: {current_date}")
        
        # Check if data already exists for the target date to avoid duplicates
        existing_result = session.query(KnowledgeGraphDailyResult).filter_by(date=current_date).first()
        if existing_result:
            print(f"Data already exists for {current_date}. Skipping.")
            current_date += timedelta(days=1)
            continue
            
        # 3. For each source record, create a new record for the target date
        for source_result in source_results:
            new_raw_json = copy.deepcopy(source_result.raw_json)
            
            # Modify the score slightly (e.g., +/- up to 5%)
            original_score = source_result.result_score
            modifier = 1 + random.uniform(-0.05, 0.05)
            new_score = round(original_score * modifier, 4) if original_score else None
            
            # Update score in JSON object
            if new_score is not None:
                new_raw_json['resultScore'] = new_score

            new_result = KnowledgeGraphDailyResult(
                entity_id=source_result.entity_id,
                result_score=new_score,
                name=source_result.name,
                description=source_result.description,
                article_body=source_result.article_body,
                raw_json=new_raw_json,
                date=current_date
            )
            session.add(new_result)
        
        print(f"Prepared {len(source_results)} new records for {current_date}.")
        current_date += timedelta(days=1)

    session.commit()
    print("\n--- Data backfill process finished successfully. ---")


def run_backfill():
    """Connects to DB and runs the backfill function."""
    if not DATABASE_URL:
        print("Error: DATABASE_URL is not set.")
        return
        
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Copies data FROM '2024-06-28' TO the date range '2024-06-29'...'2024-07-07'
    backfill_data(session, '2025-07-08', '2025-06-29', '2025-07-07')
    
    session.close()


if __name__ == "__main__":
    # main()
    print("\nTo run the one-time data backfill, uncomment the line below and run the script:")
    run_backfill() 