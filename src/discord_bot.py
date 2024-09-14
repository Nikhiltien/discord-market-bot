import asyncio
import logging
import discord


from discord import app_commands
from discord.ext import commands
from typing import Optional, Dict, List


GUILD_ID = 1280681998674825256
SYSTEM_CHANNEL = 1281287799567286292
intents = discord.Intents.all()


class DiscordBot(commands.Bot):
    def __init__(self, command_prefix, intents, guild_id, sys_channel, callback = None):
        super().__init__(command_prefix=command_prefix, intents=intents)
        self.logger = logging.getLogger(__name__)
        self.guild_id = guild_id
        self.system_channel = sys_channel
        self.callback = callback
        self.ready = asyncio.Event()

    async def setup_hook(self):
        # Add the cog containing the command
        await self.add_cog(MyCog(self))
        # Sync commands when the bot is ready
        try:
            guild = discord.Object(id=self.guild_id)
            synced = await self.tree.sync(guild=guild)
            self.logger.debug(f'Synced {len(synced)} commands for guild ID {self.guild_id}')
        except Exception as e:
            print(f'Error syncing commands: {e}')

    async def on_ready(self):
        self.logger.info(f'Logged in as {self.user}')
        self.ready.set()
        self.logger.info("Discord bot ready.")

    async def list_members(self):
        guild = self.get_guild(self.guild_id)
        if guild:
            self.logger.info(f'Guild: {guild.name}')
            await guild.chunk()  # Ensure all members are cached
            members = guild.members
            members_dict = {}
            for member in members:
                members_dict[member.id] = f'{member.name}{member.discriminator}'
            return members_dict

    async def on_member_join(self, member):
        self.callback(topic="NEW USER", message=member)

    async def on_message(self, message):
        # Don't respond to ourselves
        if message.author == self.user:
            return

        if message.content == 'ping':
            await message.channel.send('pong')

class MyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name='hello', description='Says hello!')
    @app_commands.guilds(discord.Object(id=GUILD_ID))
    async def hello(self, interaction: discord.Interaction):
        await interaction.response.send_message(f'Hello, {interaction.user.mention}!')

    @app_commands.command(name='buy', description='Buy a stock.')
    @app_commands.describe(
        ticker='The symbol of the stock you want to buy',
        quantity='The number of shares you want to buy'
    )
    @app_commands.guilds(discord.Object(id=GUILD_ID))
    async def buy(self, interaction: discord.Interaction, ticker: str, quantity: int):
        if self.bot.callback:
            message = {
                'user_id': interaction.user.id,
                'ticker': ticker.upper(),
                'quantity': quantity
            }
            result = self.bot.callback(topic='BUY', message=message)
            await interaction.response.send_message(result)

    @app_commands.command(name='sell', description='Sell a stock.')
    @app_commands.describe(
        ticker='The symbol of the stock you want to sell',
        quantity='The number of shares you want to sell'
    )
    @app_commands.guilds(discord.Object(id=GUILD_ID))
    async def sell(self, interaction: discord.Interaction, ticker: str, quantity: int):
        if self.bot.callback:
            message = {
                'user_id': interaction.user.id,
                'ticker': ticker.upper(),
                'quantity': quantity
            }
            result = self.bot.callback(topic='SELL', message=message)
            await interaction.response.send_message(result)

    @app_commands.command(name='portfolio', description='View your portfolio.')
    @app_commands.guilds(discord.Object(id=GUILD_ID))
    async def portfolio(self, interaction: discord.Interaction):
        await interaction.response.send_message('This is a placeholder command for portfolio.')

    @app_commands.command(name='leaderboard', description='View the leaderboard.')
    @app_commands.guilds(discord.Object(id=GUILD_ID))
    async def leaderboard(self, interaction: discord.Interaction):
        await interaction.response.send_message('This is a placeholder command for leaderboard.')

    @app_commands.command(name='compare_users', description='Compare users.')
    @app_commands.guilds(discord.Object(id=GUILD_ID))
    async def compare_users(self, interaction: discord.Interaction):
        await interaction.response.send_message('This is a placeholder command for compare users.')

    @app_commands.command(name='compare_stocks', description='Compare stocks.')
    @app_commands.guilds(discord.Object(id=GUILD_ID))
    async def compare_stocks(self, interaction: discord.Interaction):
        await interaction.response.send_message('This is a placeholder command for compare stocks.')

