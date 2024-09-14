CREATE TABLE IF NOT EXISTS Stocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS StockHistory (
    timestamp DATETIME NOT NULL,
    stock_id INTEGER NOT NULL,
    price REAL NOT NULL,
    volume INTEGER NOT NULL,
    PRIMARY KEY (timestamp, stock_id),
    FOREIGN KEY (stock_id) REFERENCES Stocks(id)
);

CREATE TABLE IF NOT EXISTS Users (
    user_id INTEGER PRIMARY KEY,
    username TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS UserHistory (
    timestamp DATETIME NOT NULL,
    user_id INTEGER NOT NULL,
    balance REAL NOT NULL,
    cash REAL NOT NULL,
    portfolio JSON,
    PRIMARY KEY (timestamp, user_id),
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);