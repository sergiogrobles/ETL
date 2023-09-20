import logging
from typing import Optional
from logging_functions.logging_decorators import timing_decorator
from decouple import config
from sqlalchemy import create_engine, exc
from sqlalchemy.pool import QueuePool
from settings.config_env import load_env

load_env()

@timing_decorator
def create_db_engine() -> Optional[create_engine]:
    """
    Create a database engine.

    Returns:
    - SQLAlchemy Engine object if successful.
    - None if DATABASE_URI is not set or there's an error in creating the engine.

    Note:
    - This function is timed using a decorator to log its execution time.
    """
    
    db_url = config('DATABASE_URI', default=None)

    if not db_url:
        logging.warning("Database URI is empty. Cannot create engine.")
        return None

    try:
        engine = create_engine(db_url, poolclass=QueuePool, pool_size=15, max_overflow=2)
        return engine
    except exc.ArgumentError as e:
        logging.error(f"Invalid database URI provided: {e}")
        return None