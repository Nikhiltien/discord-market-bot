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
            self.logger.debug("Tables have been created successfully.")

        except Exception as e:
            self.logger.error(f"Error setting up tables: {e}")

    def insert_stock_data(self, name: str, ticker: str, price: float, volume: int):
        timestamp = datetime.now().isoformat()
        try:
            # Insert stock into the Stocks table if it doesn't exist
            self.cursor.execute('''
                INSERT OR IGNORE INTO Stocks (ticker, name)
                VALUES (?, ?)
            ''', (ticker, name))
            
            # Retrieve the stock_id for the inserted or existing stock
            self.cursor.execute('''
                SELECT id FROM Stocks WHERE ticker = ?
            ''', (ticker,))
            stock_id = self.cursor.fetchone()[0]

            # Insert historical data into the StockHistory table
            self.cursor.execute('''
                INSERT INTO StockHistory (timestamp, stock_id, price, volume)
                VALUES (?, ?, ?, ?)
            ''', (timestamp, stock_id, price, volume))
            
            self.conn.commit()
            self.logger.info(f"Inserted stock data for {ticker} at {timestamp}.")
        except Exception as e:
            self.logger.error(f"Error inserting stock data: {e}")

    def update_stock_price(self, ticker: str, price: float, volume: int = 0):
        """
        Update the stock price in the StockHistory table with the latest price and volume.
        """
        timestamp = datetime.now().isoformat()
        try:
            # Retrieve the stock_id for the given ticker
            self.cursor.execute('''
                SELECT id FROM Stocks WHERE ticker = ?
            ''', (ticker,))
            stock = self.cursor.fetchone()
            if not stock:
                self.logger.error(f"Ticker {ticker} not found in Stocks table.")
                return

            stock_id = stock[0]

            # Insert the new price and volume into the StockHistory table
            self.cursor.execute('''
                INSERT INTO StockHistory (timestamp, stock_id, price, volume)
                VALUES (?, ?, ?, ?)
            ''', (timestamp, stock_id, price, volume))
            
            self.conn.commit()
            self.logger.debug(f"Updated stock price for {ticker} to {price} at {timestamp}.")
        except Exception as e:
            self.logger.error(f"Error updating stock price for {ticker}: {e}")

    def add_user(self, user_id: int, username: str, initial_cash: float = 100000.0):
        """
        Adds a new user to the Users table and initializes their balance, cash, and portfolio in UserHistory.
        """
        timestamp = datetime.now().isoformat()
        try:
            # Insert user into the Users table if they don't exist
            self.cursor.execute('''
                INSERT OR IGNORE INTO Users (user_id, username)
                VALUES (?, ?)
            ''', (user_id, username))
            
            # Check if the user was inserted
            if self.cursor.rowcount == 0:
                self.logger.info(f"User with ID {user_id} already exists.")
                return

            # Initialize the user's history with starting cash, balance, and empty portfolio
            portfolio = {}  # Start with an empty portfolio
            initial_balance = initial_cash  # Since portfolio value is 0 at start

            self.cursor.execute('''
                INSERT INTO UserHistory (timestamp, user_id, balance, cash, portfolio)
                VALUES (?, ?, ?, ?, ?)
            ''', (timestamp, user_id, initial_balance, initial_cash, json.dumps(portfolio)))
            self.conn.commit()
            self.logger.debug(f"User {username} added with ID {user_id}.")
        except Exception as e:
            self.logger.error(f"Error adding user {username} with ID {user_id}: {e}")

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

    def get_user_leaderboard(self) -> List[Dict]:
        """
        Retrieve users with their latest balance and 24-hour returns from the database, sorted by balance.
        """
        try:
            # Fetch the latest balance and 24-hour old balance for each user
            self.cursor.execute('''
                SELECT u.username, uh.balance,
                    (SELECT balance FROM UserHistory 
                    WHERE user_id = uh.user_id 
                    AND timestamp >= datetime('now', '-24 hours')
                    ORDER BY timestamp ASC 
                    LIMIT 1) AS old_balance
                FROM Users u
                JOIN (
                    SELECT user_id, MAX(timestamp) as latest_timestamp
                    FROM UserHistory
                    GROUP BY user_id
                ) lh ON u.user_id = lh.user_id
                JOIN UserHistory uh ON uh.user_id = lh.user_id AND uh.timestamp = lh.latest_timestamp
                ORDER BY uh.balance DESC
            ''')
            users = self.cursor.fetchall()
            user_list = []

            for user in users:
                username, balance, old_balance = user
                # Calculate 24-hour returns based on the earliest balance within the last 24 hours
                if old_balance is not None:
                    return_24h = ((balance - old_balance) / old_balance) * 100
                else:
                    return_24h = 0.0  # If no historical data within the last 24 hours, set return to 0.0

                user_list.append({
                    'username': username,
                    'balance': balance,
                    'return_24h': return_24h
                })

            return user_list
        except Exception as e:
            self.logger.error(f"Error retrieving user leaderboard: {e}")
            return []

    def company_exists(self, name: str) -> bool:
        """
        Check if a company already exists in the Stocks table based on the company name.
        """
        try:
            self.cursor.execute('''
                SELECT 1 FROM Stocks WHERE name = ?
            ''', (name,))
            result = self.cursor.fetchone()
            return result is not None
        except Exception as e:
            self.logger.error(f"Error checking if company {name} exists: {e}")
            return False

    def get_latest_price(self, ticker: str) -> Optional[float]:
        """
        Retrieve the latest price of a given stock based on the latest timestamp.
        """
        try:
            # Retrieve stock_id based on the ticker
            self.cursor.execute('''
                SELECT id FROM Stocks WHERE ticker = ?
            ''', (ticker,))
            stock = self.cursor.fetchone()
            if not stock:
                self.logger.error(f"Ticker {ticker} not found in Stocks table.")
                return None

            stock_id = stock[0]

            # Fetch the latest price from StockHistory based on stock_id
            self.cursor.execute('''
                SELECT price FROM StockHistory
                WHERE stock_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
            ''', (stock_id,))
            result = self.cursor.fetchone()
            if result:
                return result[0]
            else:
                self.logger.error(f"No price data found for ticker {ticker}.")
                return None
        except Exception as e:
            self.logger.error(f"Error retrieving latest price for {ticker}: {e}")
            return None

    def get_all_stocks(self) -> List[Dict]:
        """
        Retrieve the most recent price of each stock along with the full name and 24-hour returns from the database.
        """
        try:
            # Retrieve stock data with the latest price and the earliest price within the last 24 hours
            self.cursor.execute('''
                SELECT s.ticker, s.name, sh.price,
                    (SELECT price FROM StockHistory 
                    WHERE stock_id = s.id 
                    AND timestamp >= datetime('now', '-24 hours')
                    ORDER BY timestamp ASC 
                    LIMIT 1) AS earliest_price
                FROM Stocks s
                JOIN (
                    SELECT stock_id, MAX(timestamp) as latest_timestamp
                    FROM StockHistory
                    GROUP BY stock_id
                ) lh ON s.id = lh.stock_id
                JOIN StockHistory sh ON sh.stock_id = lh.stock_id AND sh.timestamp = lh.latest_timestamp
            ''')
            stocks = self.cursor.fetchall()
            stock_list = []

            for stock in stocks:
                ticker, name, price, earliest_price = stock
                # Calculate the return percentage based on the earliest price within the last 24 hours
                if earliest_price is not None:
                    return_24h = ((price - earliest_price) / earliest_price) * 100
                else:
                    return_24h = 0.0  # If no historical data within the last 24 hours, set return to 0.0

                stock_list.append({
                    'ticker': ticker,
                    'name': name,
                    'price': price,
                    'return_24h': return_24h
                })

            return stock_list
        except Exception as e:
            self.logger.error(f"Error retrieving stocks: {e}")
            return []

    def buy_stock(self, user_id: int, ticker: str, qty: int) -> Optional[str]:
        """
        Buy stock at the latest market price and update user's portfolio stored as JSON in the UserHistory table.
        Returns a message with the username if successful, or an error message if an issue occurs.
        """
        try:
            # Fetch the latest price of the stock
            price = self.get_latest_price(ticker)
            if price is None:
                error_msg = f"Error: Cannot execute buy order for {ticker} due to missing price data."
                self.logger.error(error_msg)
                return error_msg

            # Fetch user's current cash, balance, portfolio, and username
            self.cursor.execute('''
                SELECT u.username, uh.cash, uh.portfolio 
                FROM Users u
                JOIN (
                    SELECT * FROM UserHistory
                    WHERE user_id = ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                ) uh ON u.user_id = uh.user_id
            ''', (user_id,))
            result = self.cursor.fetchone()

            if not result:
                error_msg = f"Error: User with ID {user_id} not found in UserHistory."
                self.logger.error(error_msg)
                return error_msg

            username, cash, portfolio_json = result
            portfolio = json.loads(portfolio_json) if portfolio_json else {}

            # Check if user can afford the purchase
            total_cost = qty * price
            if cash < total_cost:
                error_msg = f"Error: User {username} has insufficient cash."
                self.logger.error(error_msg)
                return error_msg

            # Update holdings or add new stock
            if ticker in portfolio:
                current_quantity = portfolio[ticker]['quantity']
                current_average_price = portfolio[ticker]['average_price']
                new_quantity = current_quantity + qty
                new_average_price = ((current_quantity * current_average_price) + (qty * price)) / new_quantity

                portfolio[ticker]['quantity'] = new_quantity
                portfolio[ticker]['average_price'] = new_average_price
            else:
                portfolio[ticker] = {'quantity': qty, 'average_price': price}

            # Update user's cash and calculate new balance (cash + portfolio value)
            new_cash = cash - total_cost
            portfolio_value = sum(info['quantity'] * self.get_latest_price(tick) for tick, info in portfolio.items())
            new_balance = new_cash + portfolio_value

            # Update the database with the new portfolio, cash, and balance in UserHistory
            timestamp = datetime.now().isoformat()
            self.cursor.execute('''
                INSERT INTO UserHistory (timestamp, user_id, balance, cash, portfolio)
                VALUES (?, ?, ?, ?, ?)
            ''', (timestamp, user_id, new_balance, new_cash, json.dumps(portfolio)))
            self.conn.commit()

            self.logger.debug(f"User {username} bought {qty} shares of {ticker} at {price}.")
            return f"{username}"
        except Exception as e:
            error_msg = f"Error buying stock for user {user_id}: {e}"
            self.logger.error(error_msg)
            return error_msg

    def sell_stock(self, user_id: int, ticker: str, qty: int) -> Optional[str]:
        """
        Sell stock at the latest market price and update user's portfolio stored as JSON in the UserHistory table.
        Returns a message with the username if successful, or an error message if an issue occurs.
        """
        try:
            # Fetch the latest price of the stock
            sell_price = self.get_latest_price(ticker)
            if sell_price is None:
                error_msg = f"Error: Cannot execute sell order for {ticker} due to missing price data."
                self.logger.error(error_msg)
                return error_msg

            # Fetch user's current cash, balance, portfolio, and username
            self.cursor.execute('''
                SELECT u.username, uh.cash, uh.portfolio 
                FROM Users u
                JOIN (
                    SELECT * FROM UserHistory
                    WHERE user_id = ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                ) uh ON u.user_id = uh.user_id
            ''', (user_id,))
            result = self.cursor.fetchone()

            if not result:
                error_msg = f"Error: User with ID {user_id} not found in UserHistory."
                self.logger.error(error_msg)
                return error_msg

            username, cash, portfolio_json = result
            portfolio = json.loads(portfolio_json) if portfolio_json else {}

            if ticker not in portfolio or portfolio[ticker]['quantity'] < qty:
                error_msg = f"Error: User {username} does not have enough shares of {ticker} to sell."
                self.logger.error(error_msg)
                return error_msg

            # Calculate the proceeds from the sale
            proceeds = qty * sell_price

            # Update or remove the holding
            current_quantity = portfolio[ticker]['quantity']
            if current_quantity == qty:
                del portfolio[ticker]  # Remove stock if all shares are sold
            else:
                portfolio[ticker]['quantity'] -= qty

            # Update user's cash and calculate new balance (cash + portfolio value)
            new_cash = cash + proceeds
            portfolio_value = sum(info['quantity'] * self.get_latest_price(tick) for tick, info in portfolio.items())
            new_balance = new_cash + portfolio_value

            # Update the database with the new portfolio, cash, and balance in UserHistory
            timestamp = datetime.now().isoformat()
            self.cursor.execute('''
                INSERT INTO UserHistory (timestamp, user_id, balance, cash, portfolio)
                VALUES (?, ?, ?, ?, ?)
            ''', (timestamp, user_id, new_balance, new_cash, json.dumps(portfolio)))
            self.conn.commit()

            self.logger.debug(f"User {username} sold {qty} shares of {ticker} at {sell_price}.")
            return f"{username}"
        except Exception as e:
            error_msg = f"Error selling stock for user {user_id}: {e}"
            self.logger.error(error_msg)
            return error_msg

    def get_user_portfolio(self, user_id: int) -> Optional[Dict[str, Dict]]:
        """
        Retrieve the latest balance and portfolio of a user from the UserHistory table.
        """
        try:
            self.cursor.execute('''
                SELECT balance, portfolio FROM UserHistory
                WHERE user_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
            ''', (user_id,))
            result = self.cursor.fetchone()
            if result:
                balance, portfolio = result
                portfolio = json.loads(portfolio) if portfolio else {}
                return {'balance': balance, 'portfolio': portfolio}
            else:
                self.logger.error(f"User with ID {user_id} not found in UserHistory.")
                return None
        except Exception as e:
            self.logger.error(f"Error retrieving portfolio for user {user_id}: {e}")
            return None
        
    def get_all_users(self) -> List[Dict]:
        """
        Retrieve the latest snapshot of all users and their portfolio data from the UserHistory table.
        """
        try:
            # Fetch the latest balance and portfolio for each user
            self.cursor.execute('''
                SELECT uh.user_id, u.username, uh.balance, uh.portfolio
                FROM Users u
                JOIN (
                    SELECT user_id, MAX(timestamp) AS latest_timestamp
                    FROM UserHistory
                    GROUP BY user_id
                ) lh ON u.user_id = lh.user_id
                JOIN UserHistory uh ON uh.user_id = lh.user_id AND uh.timestamp = lh.latest_timestamp
            ''')
            users = self.cursor.fetchall()
            user_list = []
            for user in users:
                user_id, username, balance, portfolio_json = user
                portfolio = json.loads(portfolio_json) if portfolio_json else {}
                user_list.append({
                    'user_id': user_id,
                    'username': username,
                    'balance': balance,
                    'portfolio': portfolio
                })
            return user_list
        except Exception as e:
            self.logger.error(f"Error retrieving users: {e}")
            return []
