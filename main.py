import os
import sys
import asyncio
import logging


from dotenv import load_dotenv
from src.logger import setup_logger
from src.database import Database
from src.discord_bot import DiscordBot
from src.market_state import StockMarket


setup_logger(level='INFO', stream=True)
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
GUILD_ID = 1280681998674825256
SYSTEM_CHANNEL = 1281287799567286292


async def monitor(market: StockMarket):
    while True:
        await asyncio.sleep(120)
        print(market.users)
        # print(market.stocks)
        # print(market.client.members)


async def main():
    try:
        db = Database("galactic_republic")

        if not TOKEN or TOKEN == '':
            logging.error("Discord API token is missing. Exiting.")
            sys.exit(1)

        # Start Discord client before passing to StockMarket class
        bot = DiscordBot(command_prefix='!', guild_id=GUILD_ID, sys_channel=SYSTEM_CHANNEL)
        _ = asyncio.create_task(bot.start(TOKEN))
        await bot.ready.wait()

        # channel = await bot.fetch_channel(channel_id)
        # await channel.send("test")

        market_state = StockMarket(db=db, client=bot)

        game_task = asyncio.create_task(market_state.start_game(interval=60))
        monitor_task = asyncio.create_task(monitor(market_state))

        await asyncio.gather(game_task, monitor_task)
    except Exception as e:
        print(f"Unexpected error in main: {e}")
    finally:
        db.close()


    # db.insert_stock_data("Apple Inc.", "AAPL", 50.00, 1000)
    # db.insert_stock_data("Tesla, Inc.", "TSLA", 100.25, 500)
    # db.add_user(user_id=2, username="Nikhil", balance=100000.00)

    # portfolio = db.get_user_portfolio(user_id=2)
    # print(portfolio)


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
