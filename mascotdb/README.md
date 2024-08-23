# MascotDB Scraper

This script is designed to scrape data from the website **mascotdb.com** using the Playwright library. The extracted data includes details about various mascots, such as team names, locations, leagues, levels, and statuses. The scraped data is saved into a CSV file for further analysis or usage.

### Features: 
- Scrapes mascot data from mascotdb.com.
- Uses Playwright for browser automation.
- Data is saved to a CSV file (mascot_db.csv).
- Includes logging for tracking the scraping process.

### Script Breakdown:
- **Logging Setup**: Configures logging to write logs to scraper.log in write mode with a specified format.
- **CSV Initialization**: Creates and writes the headers of the CSV file (mascot_db.csv).
- **Playwright Browser**: Launches a Chromium browser in non-headless mode and navigates to mascotdb.com.
- **Dropdown Interaction**: Interacts with a dropdown menu on the site to select different states/regions.
- **Scraping Logic**: For each selected option, it clicks the search button, waits for the page to load, and parses the page content using BeautifulSoup.
- **Data Extraction**: Extracts relevant data from the HTML table and writes it to the CSV file.
- **Error Handling**: Catches and logs any errors that occur during the scraping process.
