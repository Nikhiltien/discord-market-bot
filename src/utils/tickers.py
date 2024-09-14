import re

# List of common words to exclude from ticker generation
COMMON_WORDS = set([
    'INC', 'LTD', 'CORPORATION', 'CORP', 'COMPANY', 'CO', 'GROUP', 'SYSTEMS',
    'TECHNOLOGIES', 'INDUSTRIES', 'INTERNATIONAL', 'HOLDINGS', 'SERVICES',
    'SOLUTIONS', 'GLOBAL', 'LIMITED', 'BUSINESS', 'INCORPORATED', 'ASSOCIATION',
    'FOUNDATION', 'INSTITUTE', 'LLC', 'PLC', 'AND', '&', 'THE', 'OF', 'FOR'
])

def generate_ticker(name, existing_tickers=None, max_ticker_length=5):
    """
    Generates a unique ticker symbol for a given company name.
    
    Parameters:
        name (str): The company name.
        existing_tickers (set): A set of tickers that have already been assigned.
        max_ticker_length (int): The maximum length of the ticker symbol.
    
    Returns:
        str: A unique ticker symbol.
    """
    if existing_tickers is None:
        existing_tickers = set()

    # Remove punctuation and convert to uppercase
    name_clean = re.sub(r'[^\w\s]', '', name).upper()
    # Split name into words and remove common words
    words = [word for word in name_clean.split() if word not in COMMON_WORDS]
    
    # If no words left after removing common words, use original words
    if not words:
        words = name_clean.split()
    
    # Initial ticker generation
    ticker = ''.join(word[0] for word in words)[:max_ticker_length]
    
    if ticker not in existing_tickers:
        return ticker

    # Collision handling by adding more letters
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

    # Use letters from the concatenated words
    concatenated_name = ''.join(words)
    for i in range(len(ticker), len(concatenated_name)+1):
        ticker_candidate = concatenated_name[:i][:max_ticker_length]
        if ticker_candidate not in existing_tickers:
            return ticker_candidate

    # Return the first max_ticker_length letters as a last resort
    return concatenated_name[:max_ticker_length]

def generate_company_tickers(company_names, max_ticker_length=5):
    """
    Generates unique ticker symbols for a list of company names.
    
    Parameters:
        company_names (list): A list of company names.
        max_ticker_length (int): The maximum length of the ticker symbols.
    
    Returns:
        dict: A dictionary mapping company names to unique ticker symbols.
    """
    # Remove duplicates
    company_names = list(set(company_names))

    existing_tickers = set()
    company_tickers = {}
    
    for name in company_names:
        ticker = generate_ticker(name, existing_tickers, max_ticker_length)
        existing_tickers.add(ticker)
        company_tickers[name] = ticker

    return company_tickers
