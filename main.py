import os
import asyncio
import discord


from dotenv import load_dotenv
from src.logger import setup_logger
from src.database import Database
from src.discord_bot import DiscordBot
from src.market_state import StockMarket


setup_logger(level='INFO', stream=True)
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
GUILD_ID = 1280681998674825256
SYSTEM_CHANNEL = 1280737210785730603
intents = discord.Intents.all()


async def monitor(market: StockMarket):
    while True:
        print(market.users)
        # print(market.stocks)
        # print(market.client.members)
        await asyncio.sleep(120)


async def main():
    try:
        db = Database("galactic_republic")

        bot = DiscordBot(command_prefix='!', intents=intents, guild_id=GUILD_ID, sys_channel=SYSTEM_CHANNEL)
        client_task = asyncio.create_task(bot.start(TOKEN))
        await bot.ready.wait()

        # channel_id = 1280737210785730603
        # channel = await client.fetch_channel(channel_id)
        # await channel.send("test")

        market_state = StockMarket(db=db, client=bot)

        # Start game and monitoring tasks
        game_task = asyncio.create_task(market_state.start_game(interval=30))
        monitor_task = asyncio.create_task(monitor(market_state))

        # Wait for the tasks to run indefinitely
        await asyncio.gather(game_task, monitor_task)
    except Exception as e:
        print(f"Unexpected error in main: {e}")
    finally:
        # Ensure the database connection is closed properly
        db.close()

    # db.insert_stock_data("Apple Inc.", "AAPL", 50.00, 1000)
    # db.insert_stock_data("Tesla, Inc.", "TSLA", 100.25, 500)

    # # Add a user and simulate stock buying
    # db.add_user(user_id=2, username="Nikhil", balance=100000.00)
    # db.buy_stock(user_id=2, ticker="AAPL", quantity=10)
    # db.buy_stock(user_id=2, ticker="TSLA", quantity=5)

    # db.sell_stock(user_id=2, ticker="AAPL", quantity=6)
    # db.sell_stock(user_id=2, ticker="TSLA", quantity=3)
    
    # # Display user portfolio
    # portfolio = db.get_user_portfolio(user_id=2)
    # print(portfolio)

    # db.close()
    # print(1)

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\nKeyboardInterrupt, shutting down...")
        # Gather all running tasks
        tasks = asyncio.all_tasks(loop)
        for task in tasks:
            task.cancel()  # Cancel each task
        # Wait for the cancellation of all tasks
        try:
            loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        except Exception as e:
            print(f"Error during task cancellation: {e}")
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
            print("Shutdown complete.")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        if not loop.is_closed():
            loop.close()
