import random
import asyncio
import logging
import matplotlib.pyplot as plt


import numpy as np
from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Dict
from prettytable import PrettyTable
from PIL import Image, ImageDraw, ImageFont
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
            "LEADERBOARD": self.leaderboard,
            "ALL STOCKS": self.display_stocks,
            "COMPARE STOCKS": self.compare_stocks,
            "NEW USER": self.new_user,
            "BUY": self.user_buy,
            "SELL": self.user_sell,
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
                # Retrieve the latest price if the company already exists
                existing_price = self.db.get_latest_price(ticker)
                if existing_price is not None:
                    self.logger.debug(f"Company {name} ({ticker}) already exists. Using last known price: {existing_price}.")
                    self.stocks[ticker] = StockInfo(ticker=ticker, price=existing_price)
                    continue
                else:
                    self.logger.error(f"Could not retrieve the latest price for {name} ({ticker}). Skipping.")

            # Generate initial price and volume for new companies
            initial_price = round(random.uniform(10, 1_000), 2)
            initial_volume = 0

            # Insert the new stock data into the database
            self.db.insert_stock_data(name=name, ticker=ticker, price=initial_price, volume=initial_volume)
            self.logger.debug(f"Added {name} ({ticker}) with initial price {initial_price} and volume {initial_volume}")
            self.stocks[ticker] = StockInfo(ticker=ticker, price=initial_price)

    def leaderboard(self, message: None) -> str:
        # Retrieve leaderboard data and limit to the top 10 users
        leaderboard_data = self.db.get_user_leaderboard()[:10]  # Limit to top 10 users
        table = PrettyTable()
        table.field_names = ["Place", "Username", "Balance", "24h Return"]

        # Populate the table with user data, sorted by balance
        for idx, user in enumerate(leaderboard_data, start=1):
            username = user['username']
            balance = f"${user['balance']:.2f}"
            return_24h = f"{user['return_24h']:.2f}%"
            table.add_row([idx, username, balance, return_24h])

        leaderboard_header = "🏆 **Top 10 Leaderboard** 🏆"

        # Send the leaderboard as a formatted message
        return f"{leaderboard_header}\n```\n{table}\n```"

    def compare_stocks(self, message: str) -> str:
        """
        Compare two stocks by plotting their historical prices on a logarithmic scale.

        Parameters:
            ticker1 (str): The first stock ticker.
            ticker2 (str): The second stock ticker.

        Returns:
            str: The file path of the saved plot image.
        """
        ticker1 = message['ticker1']
        ticker2 = message['ticker2']
        # Fetch historical prices from the database
        data1 = self.db.get_stock_history(ticker1)
        data2 = self.db.get_stock_history(ticker2)

        if not data1 or not data2:
            self.logger.error(f"Unable to fetch data for tickers: {ticker1} or {ticker2}.")
            return "Error: Unable to fetch stock data."

        # Extract dates and prices for both stocks
        dates1, prices1 = zip(*[(entry['timestamp'], entry['price']) for entry in data1])
        dates2, prices2 = zip(*[(entry['timestamp'], entry['price']) for entry in data2])

        # Plotting the data
        plt.figure(figsize=(12, 6))
        plt.plot(dates1, prices1, label=ticker1, color='blue')
        plt.plot(dates2, prices2, label=ticker2, color='orange')
        plt.yscale('log')  # Set y-axis to logarithmic scale
        plt.xlabel('Date')
        plt.ylabel('Price')
        plt.title(f'Performance of ${ticker1} vs. ${ticker2}')
        plt.legend()
        plt.grid(True)

        # Save the plot
        file_path = f"compare_{ticker1}_{ticker2}.png"
        plt.savefig(file_path)
        plt.close()

        return file_path

    def display_stocks(self, message: None) -> str:
        """
        Generates an image of the stock market overview with tickers, full names, prices, and 24-hour returns.
        Returns the file path of the created image.
        """
        # Fetch all stocks sorted alphabetically
        stock_data = self.db.get_all_stocks()
        sorted_stocks = sorted(stock_data, key=lambda x: x['ticker'])

        # Image configuration
        image_width = 900
        row_height = 40
        header_height = 50
        padding = 20
        total_height = header_height + (len(sorted_stocks) * row_height) + padding

        # Create a blank image with a white background
        image = Image.new("RGB", (image_width, total_height), "white")
        draw = ImageDraw.Draw(image)

        # Load a font
        try:
            font = ImageFont.truetype("arial.ttf", 20)
            header_font = ImageFont.truetype("arial.ttf", 24)
        except IOError:
            font = ImageFont.load_default()
            header_font = ImageFont.load_default()

        # Draw headers
        draw.text((padding, padding), "Ticker", font=header_font, fill="black")
        draw.text((200, padding), "Name", font=header_font, fill="black")
        draw.text((500, padding), "Price", font=header_font, fill="black")
        draw.text((700, padding), "24h Return", font=header_font, fill="black")

        # Draw stock data
        for index, stock in enumerate(sorted_stocks):
            y = header_height + (index * row_height) + padding
            ticker = stock['ticker']
            name = stock.get('name', 'Unknown')
            price = f"${stock['price']:.2f}"
            return_24h = stock['return_24h']

            # Format the return with color coding
            return_text = f"{return_24h:.2f}%"
            return_color = "green" if return_24h > 0 else "red"

            draw.text((padding, y), ticker, font=font, fill="black")
            draw.text((200, y), name, font=font, fill="black")
            draw.text((500, y), price, font=font, fill="black")
            draw.text((700, y), return_text, font=font, fill=return_color)

        # Save the image to a temporary location
        file_path = "stock_overview.png"
        image.save(file_path)
        return file_path

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

        username = result
        self.logger.info(f"User {username} bought {qty} shares of {ticker}.")
        return f"{username} successfully bought {qty} shares of {ticker}."

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

        username = result
        self.logger.info(f"User {username} sold {qty} shares of {ticker}.")
        return f"{username} successfully sold {qty} shares of {ticker}."
