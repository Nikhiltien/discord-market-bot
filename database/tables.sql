CREATE TABLE IF NOT EXISTS Stocks (
    timestamp DATETIME PRIMARY KEY,
    name TEXT NOT NULL,
    ticker TEXT NOT NULL,
    price REAL NOT NULL,
    volume INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS Users (
    timestamp DATETIME PRIMARY KEY,
    user_id INTEGER NOT NULL,
    username TEXT NOT NULL,
    balance REAL NOT NULL,
    portfolio JSON
);
