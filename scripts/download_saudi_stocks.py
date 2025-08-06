"""
Saudi Stock Data Downloader Module

This module provides functionality to download and save Saudi stock market data
using Yahoo Finance API. It supports both individual stock downloads and batch
processing of multiple stocks.

Features:
- Download historical stock data for Saudi companies
- Save data in CSV format with organized directory structure
- Error handling and logging
- Flexible date range selection
- Both direct execution and module import support

Usage:
    # Direct execution (downloads all predefined stocks)
    python saudi_data_downloader.py

    # Module usage
    from saudi_data_downloader import download_saudi_stocks, SAUDI_TICKERS
    results = download_saudi_stocks()
"""

import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Saudi stock tickers dictionary
SAUDI_TICKERS = {
    "Saudi_Aramco": "2222.SR",
    "Al_Rajhi_Bank": "1120.SR",
    "Saudi_National_Bank": "1180.SR",
    "Saudi_Telecom": "7010.SR",
    "Saudi_Electricity": "5110.SR",
    "Savola_Group": "2050.SR",
    "Jarir_Marketing": "4190.SR",
    "SABIC": "2010.SR",
    "ACWA_Power": "2082.SR",
    "Banque_Saudi_Fransi": "1050.SR"
}


def create_output_directory(path="data/raw"):
    """
    Create output directory for saving CSV files.

    Args:
        path (str): Directory path to create. Defaults to "data/raw".

    Returns:
        Path: Created directory path object.

    Example:
        >>> output_dir = create_output_directory("my_data")
        >>> print(output_dir)  # PosixPath('my_data')
    """
    output_dir = Path(path)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory created/verified: {output_dir}")
    return output_dir


def download_single_stock(name, symbol, start_date, end_date, output_dir):
    """
    Download data for a single stock and save to CSV.

    Args:
        name (str): Human-readable name of the stock.
        symbol (str): Yahoo Finance ticker symbol (e.g., "2222.SR").
        start_date (str): Start date in YYYY-MM-DD format.
        end_date (str): End date in YYYY-MM-DD format.
        output_dir (Path): Directory to save the CSV file.

    Returns:
        bool: True if download was successful, False otherwise.

    Example:
        >>> success = download_single_stock(
        ...     "Saudi_Aramco", "2222.SR", "2024-01-01", "2024-12-31", Path("data")
        ... )
        >>> print(success)  # True or False
    """
    try:
        logger.info(f"📈 Downloading data for {name} ({symbol})...")

        # Download stock data
        df = yf.download(symbol, start=start_date, end=end_date, progress=False)

        if df.empty:
            logger.warning(f"⚠️  No data found for {name} ({symbol})")
            return False

        # Create CSV file path
        csv_path = output_dir / f"{name.lower()}_saudi.csv"

        # Save to CSV
        df.to_csv(csv_path)

        logger.info(f"✅ Saved {len(df)} records to {csv_path}")
        return True

    except Exception as e:
        logger.error(f"❌ Error downloading {name} ({symbol}): {str(e)}")
        return False


def download_saudi_stocks(tickers=None, start_date=None, end_date=None, output_dir="data/raw"):
    """
    Download historical data for Saudi stocks.

    This function downloads stock data for multiple Saudi companies and saves
    them as CSV files in the specified directory.

    Args:
        tickers (dict, optional): Dictionary of stock names and symbols.
                                Defaults to SAUDI_TICKERS.
        start_date (str, optional): Start date in YYYY-MM-DD format.
                                  Defaults to "2024-01-01".
        end_date (str, optional): End date in YYYY-MM-DD format.
                                Defaults to current date.
        output_dir (str, optional): Output directory path.
                                  Defaults to "data/raw".

    Returns:
        dict: Download results with keys 'successful', 'failed', 'total'.

    Example:
        >>> # Download all Saudi stocks for 2024
        >>> results = download_saudi_stocks()
        >>> print(f"Downloaded {results['successful']} stocks successfully")

        >>> # Download specific stocks for custom date range
        >>> custom_tickers = {"Saudi_Aramco": "2222.SR"}
        >>> results = download_saudi_stocks(
        ...     tickers=custom_tickers,
        ...     start_date="2023-01-01",
        ...     end_date="2023-12-31"
        ... )
    """
    # Set default values
    if tickers is None:
        tickers = SAUDI_TICKERS

    if start_date is None:
        start_date = "2024-01-01"

    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    # Create output directory
    output_path = create_output_directory(output_dir)

    # Initialize results tracking
    results = {"successful": 0, "failed": 0, "total": len(tickers)}

    logger.info(f"🚀 Starting download for {len(tickers)} stocks...")
    logger.info(f"📅 Date range: {start_date} to {end_date}")

    # Download each stock
    for name, symbol in tickers.items():
        success = download_single_stock(name, symbol, start_date, end_date, output_path)

        if success:
            results["successful"] += 1
        else:
            results["failed"] += 1

    # Print summary
    logger.info(f"\n Download Summary:")
    logger.info(f"✅ Successful: {results['successful']}")
    logger.info(f"❌ Failed: {results['failed']}")
    logger.info(f"📁 Files saved to: {output_path}")

    return results


def get_available_stocks():
    """
    Get a copy of available Saudi stock tickers.

    Returns:
        dict: Dictionary containing available stock names and symbols.

    Example:
        >>> stocks = get_available_stocks()
        >>> print(list(stocks.keys()))
        ['Saudi_Aramco', 'Al_Rajhi_Bank', ...]
    """
    return SAUDI_TICKERS.copy()


def download_custom_date_range(days_back=365):
    """
    Download stock data for a specified number of days back from today.

    Args:
        days_back (int): Number of days to go back from current date.
                        Defaults to 365 (1 year).

    Returns:
        dict: Download results with keys 'successful', 'failed', 'total'.

    Example:
        >>> # Download last 30 days data
        >>> results = download_custom_date_range(days_back=30)
        >>> print(f"Downloaded {results['successful']} stocks for last 30 days")
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    return download_saudi_stocks(
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d")
    )


def get_stock_info(symbol):
    """
    Get basic information about a specific stock.

    Args:
        symbol (str): Yahoo Finance ticker symbol (e.g., "2222.SR").

    Returns:
        dict: Basic stock information or None if error occurs.

    Example:
        >>> info = get_stock_info("2222.SR")
        >>> if info:
        ...     print(f"Company: {info.get('longName', 'N/A')}")
    """
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        return {
            'symbol': symbol,
            'name': info.get('longName', 'N/A'),
            'sector': info.get('sector', 'N/A'),
            'industry': info.get('industry', 'N/A'),
            'market_cap': info.get('marketCap', 'N/A')
        }
    except Exception as e:
        logger.error(f"Error getting info for {symbol}: {str(e)}")
        return None


def validate_date_format(date_string):
    """
    Validate if date string is in correct YYYY-MM-DD format.

    Args:
        date_string (str): Date string to validate.

    Returns:
        bool: True if date format is valid, False otherwise.

    Example:
        >>> validate_date_format("2024-01-01")  # True
        >>> validate_date_format("01-01-2024")  # False
    """
    try:
        datetime.strptime(date_string, "%Y-%m-%d")
        return True
    except ValueError:
        return False


if __name__ == "__main__":
    """
    Main execution block - runs when script is executed directly.

    This block downloads all predefined Saudi stocks for 2024 and provides
    interactive options for additional downloads.
    """
    print("🇸🇦 Saudi Stock Data Downloader")
    print("=" * 50)

    # Main download operation
    print("📥 Starting main download process...")
    results = download_saudi_stocks()

    # Display additional information
    print(f"\n📈 Available stocks: {len(SAUDI_TICKERS)}")
    print(f"🏢 Companies downloaded:")
    for name, symbol in SAUDI_TICKERS.items():
        print(f"   • {name.replace('_', ' ')} ({symbol})")

    print(f"\n🎉 Main download completed!")
    print(f"📁 Check 'data/raw/' folder for CSV files")

    # Interactive option for recent data
    try:
        response = input("\n🤔 Would you like to download recent 30-day data as well? (y/n): ")
        if response.lower() in ['y', 'yes']:
            print("\n📅 Downloading last 30 days data...")
            recent_results = download_custom_date_range(days_back=30)
            print(f"✅ Recent data download completed!")
            print(f"📊 Recent download results: {recent_results['successful']}/{recent_results['total']} successful")
    except KeyboardInterrupt:
        print("\n👋 Download process interrupted by user.")
    except Exception as e:
        logger.error(f"Error in interactive section: {str(e)}")

    print("\n✨ All operations completed!")
    print("💡 Tip: You can also import this module in other scripts:")
    print("   from saudi_data_downloader import download_saudi_stocks")
