import re
import csv

# Initialize an empty list to hold company names
company_names = []

# Read company names from the file
with open('database/stocks.csv', 'r') as file:
    for line in file:
        company_name = line.strip()
        if company_name:
            company_names.append(company_name)

# Remove duplicates
company_names = list(set(company_names))

# List of common words to exclude from ticker generation
common_words = set([
    'INC', 'LTD', 'CORPORATION', 'CORP', 'COMPANY', 'CO', 'GROUP', 'SYSTEMS',
    'TECHNOLOGIES', 'INDUSTRIES', 'INTERNATIONAL', 'HOLDINGS', 'SERVICES',
    'SOLUTIONS', 'GLOBAL', 'LIMITED', 'BUSINESS', 'INCORPORATED', 'ASSOCIATION',
    'FOUNDATION', 'INSTITUTE', 'LLC', 'PLC', 'AND', '&', 'THE', 'OF', 'FOR'
])

def generate_ticker(name, existing_tickers):
    # Remove punctuation and convert to uppercase
    name_clean = re.sub(r'[^\w\s]', '', name).upper()
    # Split name into words and remove common words
    words = [word for word in name_clean.split() if word not in common_words]
    
    # If no words left after removing common words, use original words
    if not words:
        words = name_clean.split()
    
    max_ticker_length = 5  # Maximum ticker length
    ticker = ''.join(word[0] for word in words)[:max_ticker_length]
    
    if ticker not in existing_tickers:
        return ticker

    # Try to generate a unique ticker by adding more letters from each word
    for length in range(1, max_ticker_length):
        for i in range(len(words)):
            if len(words[i]) > length:
                # Build ticker by taking more letters from the conflicting word
                ticker_candidate = ''.join(
                    words[j][:length+1] if j == i else words[j][0]
                    for j in range(len(words))
                )[:max_ticker_length]
                
                if ticker_candidate not in existing_tickers:
                    return ticker_candidate

    # As a last resort, use letters from the concatenated words
    concatenated_name = ''.join(words)
    for i in range(len(ticker), len(concatenated_name)+1):
        ticker_candidate = concatenated_name[:i][:max_ticker_length]
        if ticker_candidate not in existing_tickers:
            return ticker_candidate

    # If still no unique ticker, return the first max_ticker_length letters
    return concatenated_name[:max_ticker_length]

existing_tickers = set()
company_tickers = {}

for name in company_names:
    ticker = generate_ticker(name, existing_tickers)
    existing_tickers.add(ticker)
    company_tickers[name] = ticker

# Write the company names and tickers to a new CSV file
with open('company_tickers.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Company Name', 'Ticker'])
    for name, ticker in company_tickers.items():
        writer.writerow([name, ticker])

# Optionally, print the company names and tickers
for name, ticker in company_tickers.items():
    print(f"{ticker}: {name}")