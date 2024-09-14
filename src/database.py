import ujson as json
import sqlite3
import logging

from datetime import datetime
from typing import Dict, List, Optional

class Database:
    def __init__(self, db_name: str = "galactic_republic.db") -> None:
        self.logger = logging.getLogger(__name__)
        self.conn = self.connect(db_name)
        if self.conn:
            self.cursor = self.conn.cursor()
            self.create_tables()

    def connect(self, db_name: str):
        try:
            conn = sqlite3.connect(f"database/{db_name}.db")
            self.logger.info("Connected to SQLite database.")
            return conn

        except:
            self.logger.error("Error connecting to database.")
            return None

    def close(self):
            self.conn.close()

    def create_tables(self):
        with open('database/tables.sql', 'r') as file:
            sql_script = file.read()

        try:
            with self.conn as conn:
                sql_commands = sql_script.split(';')
                for command in sql_commands:
                    if command.strip():
                        conn.execute(command)
            self.logger.info("Tables have been created successfully.")

        except Exception as e:
            self.logger.error(f"Error setting up tables: {e}")


    def insert_stock_data(self, name: str, ticker: str, price: float, volume: int):
        timestamp = datetime.now().isoformat()
        try:
            self.cursor.execute('''
                INSERT INTO Stocks (timestamp, name, ticker, price, volume)
                VALUES (?, ?, ?, ?, ?)
            ''', (timestamp, name, ticker, price, volume))
            self.conn.commit()
            self.logger.info(f"Inserted stock data for {ticker} at {timestamp}.")
        except Exception as e:
            self.logger.error(f"Error inserting stock data: {e}")

    def add_user(self, user_id: int, username: str, balance: float):
        timestamp = datetime.now().isoformat()
        portfolio = {}  # Start with an empty portfolio
        try:
            self.cursor.execute('''
                INSERT INTO Users (timestamp, user_id, username, balance, portfolio)
                VALUES (?, ?, ?, ?, ?)
            ''', (timestamp, user_id, username, balance, json.dumps(portfolio)))
            self.conn.commit()
            self.logger.info(f"User {username} added with ID {user_id}.")
        except Exception as e:
            self.logger.error(f"Error adding user {username}: {e}")

    def user_exists(self, user_id: int) -> bool:
        """
        Check if a user already exists in the database based on their user_id.
        """
        try:
            self.cursor.execute('''
                SELECT 1 FROM Users WHERE user_id = ?
            ''', (user_id,))
            result = self.cursor.fetchone()
            return result is not None
        except Exception as e:
            self.logger.error(f"Error checking if user exists: {e}")
            return False

    def get_latest_price(self, ticker: str) -> Optional[float]:
        """
        Retrieve the latest price of a given stock based on the latest timestamp.
        """
        try:
            self.cursor.execute('''
                SELECT price FROM Stocks
                WHERE ticker = ?
                ORDER BY timestamp DESC
                LIMIT 1
            ''', (ticker,))
            result = self.cursor.fetchone()
            if result:
                return result[0]
            else:
                self.logger.error(f"No price data found for ticker {ticker}.")
                return None
        except Exception as e:
            self.logger.error(f"Error retrieving latest price for {ticker}: {e}")
            return None

    def buy_stock(self, user_id: int, ticker: str, quantity: int):
        """
        Buy stock at the latest market price and update user's portfolio stored as JSON in the Users table.
        """
        try:
            # Fetch the latest price of the stock
            price = self.get_latest_price(ticker)
            if price is None:
                self.logger.error(f"Cannot execute buy order for {ticker} due to missing price data.")
                return

            # Fetch user's current portfolio and balance
            self.cursor.execute('''
                SELECT balance, portfolio FROM Users
                WHERE user_id = ?
            ''', (user_id,))
            result = self.cursor.fetchone()

            if not result:
                self.logger.error(f"User with ID {user_id} not found.")
                return

            balance, portfolio_json = result
            portfolio = json.loads(portfolio_json) if portfolio_json else {}

            # Check if user can afford the purchase
            total_cost = quantity * price
            if balance < total_cost:
                self.logger.error(f"User {user_id} has insufficient balance.")
                return

            # Update holdings or add new stock
            if ticker in portfolio:
                current_quantity = portfolio[ticker]['quantity']
                current_average_price = portfolio[ticker]['average_price']
                new_quantity = current_quantity + quantity
                new_average_price = ((current_quantity * current_average_price) + (quantity * price)) / new_quantity

                portfolio[ticker]['quantity'] = new_quantity
                portfolio[ticker]['average_price'] = new_average_price
            else:
                portfolio[ticker] = {'quantity': quantity, 'average_price': price}

            # Update user's balance
            new_balance = balance - total_cost

            # Update the database with the new portfolio and balance
            self.cursor.execute('''
                UPDATE Users
                SET balance = ?, portfolio = ?
                WHERE user_id = ?
            ''', (new_balance, json.dumps(portfolio), user_id))
            self.conn.commit()

            self.logger.info(f"User {user_id} bought {quantity} shares of {ticker} at {price}.")
        except Exception as e:
            self.logger.error(f"Error buying stock for user {user_id}: {e}")

    def sell_stock(self, user_id: int, ticker: str, quantity: int):
        """
        Sell stock at the latest market price and update user's portfolio stored as JSON in the Users table.
        """
        try:
            # Fetch the latest price of the stock
            sell_price = self.get_latest_price(ticker)
            if sell_price is None:
                self.logger.error(f"Cannot execute sell order for {ticker} due to missing price data.")
                return

            # Fetch user's current portfolio and balance
            self.cursor.execute('''
                SELECT balance, portfolio FROM Users
                WHERE user_id = ?
            ''', (user_id,))
            result = self.cursor.fetchone()

            if not result:
                self.logger.error(f"User with ID {user_id} not found.")
                return

            balance, portfolio_json = result
            portfolio = json.loads(portfolio_json) if portfolio_json else {}

            if ticker not in portfolio or portfolio[ticker]['quantity'] < quantity:
                self.logger.error(f"User {user_id} does not have enough shares of {ticker} to sell.")
                return

            # Calculate the proceeds from the sale
            proceeds = quantity * sell_price

            # Update or remove the holding
            current_quantity = portfolio[ticker]['quantity']
            if current_quantity == quantity:
                del portfolio[ticker]  # Remove stock if all shares are sold
            else:
                portfolio[ticker]['quantity'] -= quantity

            # Update user's balance
            new_balance = balance + proceeds

            # Update the database with the new portfolio and balance
            self.cursor.execute('''
                UPDATE Users
                SET balance = ?, portfolio = ?
                WHERE user_id = ?
            ''', (new_balance, json.dumps(portfolio), user_id))
            self.conn.commit()

            self.logger.info(f"User {user_id} sold {quantity} shares of {ticker} at {sell_price}.")
        except Exception as e:
            self.logger.error(f"Error selling stock for user {user_id}: {e}")

    def get_user_portfolio(self, user_id: int) -> Optional[Dict[str, Dict]]:
        """
        Retrieve user's balance and portfolio stored as JSON.
        """
        try:
            self.cursor.execute('''
                SELECT balance, portfolio FROM Users
                WHERE user_id = ?
            ''', (user_id,))
            result = self.cursor.fetchone()
            if result:
                balance, portfolio = result
                # Properly handle JSON parsing and empty or None portfolio cases
                portfolio = json.loads(portfolio) if portfolio else {}
                return balance, portfolio
            else:
                self.logger.error(f"User with ID {user_id} not found.")
                return None
        except Exception as e:
            self.logger.error(f"Error retrieving portfolio for user {user_id}: {e}")
            return None
        
    def get_all_users(self) -> List[Dict]:
        """
        Retrieve the latest snapshot of all users and their portfolio data from the database.
        """
        try:
            self.cursor.execute('''
                SELECT user_id, balance, portfolio 
                FROM Users
                WHERE timestamp IN (
                    SELECT MAX(timestamp) 
                    FROM Users 
                    GROUP BY user_id
                )
            ''')
            users = self.cursor.fetchall()
            user_list = []
            for user in users:
                user_id, balance, portfolio_json = user
                portfolio = json.loads(portfolio_json) if portfolio_json else {}
                user_list.append({
                    'user_id': user_id,
                    'balance': balance,
                    'portfolio': portfolio
                })
            return user_list
        except Exception as e:
            self.logger.error(f"Error retrieving users: {e}")
            return []

    def get_all_stocks(self) -> List[Dict]:
        """
        Retrieve the most recent price of each stock from the database.
        """
        try:
            # Retrieve the latest price for each distinct ticker
            self.cursor.execute('''
                SELECT ticker, price
                FROM Stocks
                WHERE timestamp IN (
                    SELECT MAX(timestamp)
                    FROM Stocks
                    GROUP BY ticker
                )
            ''')
            stocks = self.cursor.fetchall()
            stock_list = [{'ticker': stock[0], 'price': stock[1]} for stock in stocks]
            return stock_list
        except Exception as e:
            self.logger.error(f"Error retrieving stocks: {e}")
            return []
