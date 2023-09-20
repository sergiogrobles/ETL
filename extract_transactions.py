import logging
from typing import Optional
from logging_functions.logging_decorators import timing_decorator
from sqlalchemy import (exc, MetaData, Table, Column, String, Date, Float, select)
import pandas as pd
from functools import lru_cache
from etl.tranform.profitability_kpi import merge_sales_expenses

logging.basicConfig(level=logging.INFO)

metadata = MetaData()



user_sales_itemized = Table(
    'user_sales_itemized',
    metadata,
    Column('user_id', String),
    Column('date', Date),
    Column('item', String),
    Column('net_sales', Float),
)

user_expenses_condensed = Table(
    'user_expenses_condensed',
    metadata,
    Column('user_id', String),
    Column('date', Date),
    Column('inventory_expense', Float),
    Column('interest_paid', Float), 
    Column('operating_expenses', Float), 
)

@timing_decorator
@lru_cache(maxsize=32)
def extract_user_sales_itemized(engine, user_id: str) -> Optional[pd.DataFrame]:

    """
    Fetch the itemized sales of a user from the database.

    Parameters:
    - engine: SQLAlchemy Engine object.
    - user_id (str): ID of the user for which to fetch the sales data.

    Returns:
    - A pandas DataFrame containing the user's itemized sales if successful.
    - None if an error occurs or if the engine isn't initialized.

    Note:
    - This function is timed using a decorator to log its execution time.
    """

    if engine is None:
        logging.warning("Database engine is not initialized.")
        return None

    select_stmt = select(user_sales_itemized).where(user_sales_itemized.c.user_id == user_id)
    
    try:
        with engine.connect() as connection:
            df = pd.read_sql(select_stmt, connection)

        # Convert 'date' to datetime format and month-year format
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%b %y') + "'"
    
    except exc.NoSuchTableError as e:
        logging.error(f"Table does not exist: {e}")
        return None
    except exc.SQLAlchemyError as e:
        logging.error(f"Error while executing SQL query: {e}")
        return None

    return df.to_dict('records')


@timing_decorator
@lru_cache(maxsize=32)
def extract_user_sales_expenses(engine, user_id: str) -> Optional[pd.DataFrame]:
    
    """
    Fetch the itemized sales of a user from the database.

    Parameters:
    - engine: SQLAlchemy Engine object.
    - user_id (str): ID of the user for which to fetch the sales data.

    Returns:
    - A pandas DataFrame containing the user's itemized sales if successful.
    - None if an error occurs or if the engine isn't initialized.

    Note:
    - This function is timed using a decorator to log its execution time.
    """

    if engine is None:
        logging.warning("Database engine is not initialized.")
        return None

    select_expenses = select(user_expenses_condensed).where(user_expenses_condensed.c.user_id == user_id)
    select_income = select(user_sales_itemized).where(user_sales_itemized.c.user_id == user_id)
    try:
        with engine.connect() as connection:
            df_expenses = pd.read_sql(select_expenses, connection)
            df_income = pd.read_sql(select_income, connection)
            
            df = merge_sales_expenses(df_expenses, df_income)
            

        
    
    except exc.NoSuchTableError as e:
        logging.error(f"Table does not exist: {e}")
        return None
    except exc.SQLAlchemyError as e:
        logging.error(f"Error while executing SQL query: {e}")
        return None

    return df.to_dict('records')
