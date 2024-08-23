from playwright.sync_api import sync_playwright
import time
from bs4 import BeautifulSoup
import csv
import logging

# Set up logging configuration
logging.basicConfig(
    filename='scraper.log',  # Log file to write to
    filemode='w',            # Write mode ('w' to overwrite, 'a' to append)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
    level=logging.INFO       # Log level (INFO, DEBUG, WARNING, ERROR, CRITICAL)
)


# Initialize the CSV file and write the headers
with open('mascot_db.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=["Mascot Team Name", "Location", "League", "Level", "Status"])
    writer.writeheader()

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()
            page.goto("https://mascotdb.com/")
            logging.info("Navigating to the mascot database website")

            # Locate the dropdown by its name attribute
            dropdown_selector = "select[name='field_state_target_id']"

            # Get all the option values
            dropdown = page.locator(dropdown_selector)
            option_values = dropdown.locator("option").evaluate_all("options => options.map(option => option.value)")

            # Iterate over each option value and select it
            for value in option_values:
                if value != "All":
                    try:
                        dropdown = page.locator(dropdown_selector)  
                        dropdown.select_option(value)


                        # Click the search button
                        search_btn = page.locator('input[value="Search"]')
                        search_btn.click()
                        
                        time.sleep(10)
                        
                        # Parse the page content with BeautifulSoup
                        bs = BeautifulSoup(page.content(), 'html.parser')

                        # Locate and parse the table
                        table_tag = bs.find('div', class_="view-content").find('table', class_="table")
                        if table_tag:
                            tr_tags = table_tag.find('tbody').find_all('tr')
                            for row in tr_tags:
                                mascot_team_name = row.find('td', class_="views-field views-field-title").get_text(strip=True)
                                city = row.find('td', class_="views-field views-field-field-city").get_text(strip=True)
                                state = row.find('td', class_="views-field views-field-field-state").get_text(strip=True)

                                location = f"{city}, {state}"
                                league = row.find('td', class_="views-field views-field-field-league").get_text(strip=True)
                                level = row.find('td', class_="views-field views-field-field-level-list").get_text(strip=True)
                                status = row.find('td', class_="views-field views-field-field-status").get_text(strip=True)

                                data = {
                                    "Mascot Team Name": mascot_team_name,
                                    "Location": location,
                                    "League": league,
                                    "Level": level,
                                    "Status": status
                                }
                                # Write the data to the CSV file
                                writer.writerow(data)
                            logging.info(f"Data extracted successfully for option: {value}")
                        else:
                            logging.warning(f"No data found for option: {value}")
                    except Exception as e:
                        print(f"Error processing option {value}: {e}")
        except Exception as e:
            logging.critical(f"Error initializing Playwright or navigating the page: {e}")
        finally:
            browser.close()