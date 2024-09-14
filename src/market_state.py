import random
import asyncio
import logging


from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Dict
from src import database
from src.discord_bot import DiscordBot
from src.utils.tickers import generate_company_tickers


@dataclass
class StockInfo:
    ticker: str
    price: float

@dataclass
class UserPortfolio:
    quantity: int = 0
    average_price: float = 0.0

@dataclass
class UserInfo:
    user_id: int
    balance: float
    portfolio: Dict[str, UserPortfolio] = field(default_factory=dict)


class StockMarket:
    def __init__(self, db: database.Database, client: DiscordBot) -> None:
        self.logger = logging.getLogger(__name__)
        self.db = db
        self.client = client
        self.users: List[UserInfo] = []
        self.stocks: Dict[str, StockInfo] = {}
        self.topics = {
            "BUY": self.user_buy,
            "SELL": self.user_sell,
            "NEW USER": self.new_user,
        }

    def _client_callback(self, topic, message):
        self.logger.debug(f"Received callback with topic: {topic}, message: {message}")
        if topic in self.topics:
            handler = self.topics[topic]
            return handler(message)
        else:
            self.logger.error(f"Unknown topic: {topic}")

    async def start_game(self, interval: int = 30):
        """
        Start the game by initializing the game and starting the price update loop.
        """
        self.client.callback = self._client_callback
        self.initialize_stocks()
        await self.initialize_game()
        await self.refresh(interval)

    async def initialize_game(self):
        """
        Initialize the game by retrieving all users and stock data from the database.
        """
        # Add Discord members to the database if they don't already exist
        member_list = await self.client.list_members()
        for user_id, username in member_list.items():
            if not self.db.user_exists(user_id):
                self.db.add_user(user_id=user_id, username=username, initial_cash=100000.0)
                self.logger.info(f"Added new user: {username} with ID {user_id}.")

        # Retrieve all users from the database
        user_data = self.db.get_all_users()
        for user in user_data:
            # Parse user data into UserInfo objects
            user_info = UserInfo(
                user_id=user['user_id'],
                balance=user['balance'],
                portfolio={
                    ticker: UserPortfolio(quantity=info['quantity'], average_price=info['average_price'])
                    for ticker, info in user['portfolio'].items()
                }
            )
            self.users.append(user_info)

        # Retrieve all stocks from the database
        stock_data = self.db.get_all_stocks()
        for stock in stock_data:
            # Parse stock data into StockInfo objects
            stock_info = StockInfo(
                ticker=stock['ticker'],
                price=stock['price']
            )
            self.stocks[stock['ticker']] = stock_info

        self.logger.info("Game initialized with users and stocks.")

    async def refresh(self, interval: int):
        while True:
            self.update_stock_prices()
            self.update_users()

            await asyncio.sleep(interval)

    def initialize_stocks(self):
        company_names = []
        with open('database/stocks.csv', 'r') as file:
            for line in file:
                company_name = line.strip()
                if company_name:
                    company_names.append(company_name)

        company_tickers = generate_company_tickers(company_names)
        for name, ticker in company_tickers.items():
            # Check if the company already exists in the database by name
            if self.db.company_exists(name):
                self.logger.debug(f"Company {name} already exists in the database. Skipping.")
                continue

            # Generate initial price and volume
            initial_price = round(random.uniform(10, 1_000), 2)
            initial_volume = 0
            
            # Insert the new stock data into the database
            self.db.insert_stock_data(name=name, ticker=ticker, price=initial_price, volume=initial_volume)
            self.logger.debug(f"Added {name} ({ticker}) with initial price {initial_price} and volume {initial_volume}")

    def update_stock_prices(self):
        """
        Update stock prices with realistic randomness and update them in the database.
        """
        for ticker, stock in self.stocks.items():
            new_price = self._calculate_price_change(stock.price)
            stock.price = max(new_price, 0.01)  # Ensure price doesn't drop below 0
            
            # Update the price in the database
            self.db.update_stock_price(ticker=ticker, price=stock.price)

            self.logger.debug(f"Updated price for {ticker}: {stock.price:.2f}")

    def update_users(self):
        pass

    def new_user(self, member: str) -> str:
        # user_id, username = message.split(':')
        # if not self.db.user_exists(user_id):
        #     self.db.add_user(user_id=user_id, username=username, balance=100_000.00)
        #     self.logger.info(f"Added new user: {username} with ID {user_id}.")
        print(f"New user joined: {member}")

    def _calculate_price_change(self, current_price: float) -> float:
        """
        Calculate the new price using a mix of Gaussian and fat-tailed distributions.
        """
        # Determine if the change should be normal or a rare fat-tailed event
        is_fat_tail = random.random() < 0.05  # 5% chance of a fat-tailed event

        if is_fat_tail:
            # Fat-tailed distribution: Large, infrequent changes
            change = random.uniform(-0.2, 0.2)  # Change between -20% to +20%
        else:
            # Gaussian distribution: Regular small changes
            change = random.gauss(0, 0.02)  # Mean 0%, standard deviation 2%

        # Apply the change to the current price
        new_price = current_price * (1 + change)
        return new_price

    def user_buy(self, message: Dict) -> str:
        user_id = message.get('user_id')
        ticker = message.get('ticker')
        qty = message.get('quantity')

        # Check for fractional shares
        if not isinstance(qty, int) or qty <= 0:
            error_msg = "Error: Quantity must be a positive integer. Fractional shares are not allowed."
            self.logger.error(error_msg)
            return error_msg

        # Attempt to buy stock and propagate any error messages
        result = self.db.buy_stock(user_id=user_id, ticker=ticker, qty=qty)
        if isinstance(result, str) and "Error" in result:
            self.logger.error(result)
            return result

        self.logger.info(f"User {user_id} bought {qty} shares of {ticker}.")
        return f"User {user_id} successfully bought {qty} shares of {ticker}."

    def user_sell(self, message: Dict) -> str:
        user_id = message.get('user_id')
        ticker = message.get('ticker')
        qty = message.get('quantity')

        # Check for fractional shares
        if not isinstance(qty, int) or qty <= 0:
            error_msg = "Error: Quantity must be a positive integer. Fractional shares are not allowed."
            self.logger.error(error_msg)
            return error_msg

        # Attempt to sell stock and propagate any error messages
        result = self.db.sell_stock(user_id=user_id, ticker=ticker, qty=qty)
        if isinstance(result, str) and "Error" in result:
            self.logger.error(result)
            return result

        self.logger.info(f"User {user_id} sold {qty} shares of {ticker}.")
        return f"User {user_id} successfully sold {qty} shares of {ticker}."