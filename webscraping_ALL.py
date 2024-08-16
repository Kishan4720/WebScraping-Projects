import mysql.connector
import requests
from bs4 import BeautifulSoup
import re
import time
import json
import csv
from concurrent.futures import ThreadPoolExecutor
import logging
from datetime import datetime
import os
from playwright.sync_api import sync_playwright
import random

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
def get_domain(url):
    domain_pattern = r"(?:https?://)(?:www\.)?([^/]+)"
    match = re.search(domain_pattern, url)
    if match:
        domain = match.group(1)
        return domain
    else:
        return None

def write_to_csv(product_url, status, data, url_country):
    # Define column names
    columns = ['Product_URL', 'Status', 'Data']
    # Prepare data row
    row = [product_url, status, data]
    # Define CSV file path
    logs_directory = "logs"
    if not os.path.exists(logs_directory):
        os.makedirs(logs_directory)
    csv_file = os.path.join(logs_directory, f"log_{url_country}.csv")
    
    try:
        # Acquire a lock on the file to prevent race conditions
        with open(csv_file, 'a', newline='', encoding='utf-8') as file:
            # Create CSV writer object
            writer = csv.writer(file)
            
            if not os.path.exists(csv_file):
                writer.writerow(columns) 
            writer.writerow(row) 
            
        logger.info("Data written to CSV file successfully.")
    except Exception as e:
        logger.error("Failed to write data to CSV file: %s", str(e))

def geturl(url, country):
    # Define request headers and cookies
    data_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    pl_cookies= {
            "sv3": "1.0_374d2957-e5b8-11ee-b8e4-be49bd3f728c",
            "__RequestVerificationToken": "CIcUUxk9VdB15DTLaGbS8vry0ZygAzj8U-0oznlhPEgdee4nrxsEb_wwMOhSc3DAFNXWKXtBGdtvVQFVU-Udb5bxY3gvsZDGq2wyuQeojJk1",
            "userCeneo": "ID=b4a2ece1-d131-46b4-af38-20e3093ab5d9&sc=1&mvv=0&nv=0",
            "ai_user": "CCZeO|2024-03-19T06:16:27.422Z",
            "_ga": "GA1.2.1546544769.1710828989",
            "_gid": "GA1.2.1298571560.1710828989",
            "_gat": "1",
            "_gat_gaMain": "1",
            "ai_session": "uS7zS|1710828989714.7|1710828989714.7",
            "__utmf": "cc7263789b20309dd222d0868ff2f670_L%2BIa%2BOWGk7CjUn5Iuwm1HQ%3D%3D",
            "browserBlStatus": "0",
            "cProdCompare_v2": "",
            "__gads": "ID=47717f0ea7cedeb8:T=1710828990:RT=1710828990:S=ALNI_MZULzXUg6F4qJCw3xXw29-aIxCh5g",
            "__gpi": "UID=00000d449bd121dd:T=1710828990:RT=1710828990:S=ALNI_MbPEE2-5sWyCFijoqsnORMN4Y9T-A",
            "__eoi": "ID=51c59fcff344ef13:T=1710828990:RT=1710828990:S=AA-AfjZMUioYLb-XgDwlxwniwCfn",
            "_gat_UA-265118-1": "1",
            "_gat_UA-51159636-1": "1",
            "ga4_ga": "GA1.2.374d2957-e5b8-11ee-b8e4-be49bd3f728c",
            "_dc_gtm_UA-265118-1": "1",
            "_dc_gtm_UA-51159636-1": "1",
            "ga4_ga_K2N2M0CBQ6": "GS1.2.1710828992.1.0.1710828993.0.0.0",
            "consentcookie": "eyJDb25zZW50cyI6WzQsMywyLDFdLCJWZXJzaW9uIjoidjEifQ==",
            "FPID": "FPID2.2.%2FjWmmoiSOX2NoONfk0kdI8mwsPVDm8F15saQe6CluHw%3D",
            "FPLC": "gdzqGccH1vxh%2FAFG%2BgTNnKdlKtWT2N6kcyaXKoqTRZDQY%2Fey9C92eLHNjKD3wmUVW7rSHB0S%2BURL%2F9Idv5bxaUljlyBEKFUOHU4Qf87ot8d7mZk%3D"
        }
    try:
        if country == 'PL':
            response = requests.get(url, headers=data_headers, cookies=pl_cookies)
            time.sleep(2)
        else:
            response = requests.get(url, headers=data_headers)

        if response.status_code == 200:
            html_content = response.content
            soup = BeautifulSoup(html_content, 'html.parser')
            return soup
        else:
            error_msg = f"Failed to fetch. Status code : {response.status_code}"
            logging.error(f"{error_msg} - URL: {url}")
            write_to_csv(url, "failure", error_msg, country)
            return None
    except requests.exceptions.RequestException as e:
        error_msg = f"Error fetching {url}: {e}"
        logging.error(f"Error fetching {url}: {e}")
        write_to_csv(url, "failure", error_msg, country)
        return None

def get_html_content(url, country, domain):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            context = browser.new_context(
                # user_anet = USER_AGENT,
                bypass_csp=True
            )
            page = context.new_page()
            
            page.goto(url, timeout=60000)
            time.sleep(10)

            if not page.query_selector("#mainsearchproductcategory > main > div.row.resultlist__content > div > div > div > section > div.sr-filterBar.sr-filterBar--hiddenOnMobile > div.sr-filterBar__content") \
                and not page.query_selector('#root > div > div > div > aside > div.c-offscreen__content.l-sidebar__content') \
                    and not page.query_selector("#root > div > div > div > aside"):
                
                # Handle country-specific actions
                if country in ('AT', 'DE', 'FR', 'IT'):
                    page.wait_for_selector('#accept')
                    try:
                        page.click('#accept')
                    except Exception as ex:
                        logger.error("Accept button not present")
                        logger.exception(ex)
                    btn = page.query_selector('#offer-list-with-pagination > ul > li.productOffers-listItemLoadMore.row > div > button')
                    if btn:
                        btn.click()
                        time.sleep(0.5)

                elif country in ('CZ', 'SK'):
                    time.sleep(random.uniform(5, 10))
                    cookie_acpt_btn = "#didomi-notice-agree-button > span"
                    if page.query_selector(cookie_acpt_btn):
                        page.click(cookie_acpt_btn)
                    
                    while True:
                        more_offers_btn = page.query_selector("div.c-offers-list__wrapper > button.e-button.e-button--simple.c-offers-list__more-button")
                        try:
                            if more_offers_btn and more_offers_btn.is_visible():
                                more_offers_btn.click()
                            elif not more_offers_btn:
                                break
                        except:
                            break
                
                elif (country in ('BG')) or ('emag' in domain):
                    if "emag" in domain: 
                        time.sleep(random.uniform(25, 30))
                        more_offers_btn = "#main-container > section.js-alternative-offers-section.alternative-offers-section.page-section.page-section-light > div.placeholder-other-offers-bundle.mb-0 > div > div > button"
                        if page.query_selector(more_offers_btn):
                            page.click(more_offers_btn)
                            page.wait_for_load_state('networkidle')
                            time.sleep(0.5)
                            try:
                                offers_str = page.query_selector("#main-container > section.js-alternative-offers-section.alternative-offers-section.page-section.page-section-light > div.placeholder-other-offers-bundle.mb-0 > div > h3")

                                if offers_str:
                                    offers_text = offers_str.text_content()

                                    offer_count = int(offers_text.split()[1])

                                    if offer_count > 10:
                                        for _ in range(10):
                                            page.mouse.wheel(0, 5000)
                                            time.sleep(0.5)
                            except:
                                for _ in range(8):
                                    page.mouse.wheel(0, 5000)
                                    time.sleep(0.5)

                    else:
                        if page.query_selector("div.c-notice--no-offers > section.c-notice.c-notice--error"):
                            error_msg = page.query_selector("div.c-notice--no-offers > section.c-notice.c-notice--error")
                            write_to_csv(url, "failure", error_msg.text_content(), country)
                            return None
                        
                        more_offers_btn = page.query_selector("button.e-button.e-button--simple.c-offers-list__more-button > font > font")
                        if more_offers_btn:
                            more_offers_btn.click()
                            time.sleep(1)
                            
                html_content = page.content()

                soup = BeautifulSoup(html_content, 'html.parser')

                return soup
            else:
                logger.error(f"This URL doesn't contain particular product or contains multiple Products - {url}")
                return None
    except Exception as e:
        logger.exception(f"Error occurred while scraping URL: {url}")
        return None   

def check_duplicate_record(cursor, product_url, product_name, domain, shop_name, price_text):
    sql = """SELECT COUNT(*) FROM webscraping
             WHERE product_name = %s AND domain = %s AND shop_name = %s AND price_text = %s AND product_url = %s"""
    val = (product_name, domain, shop_name, price_text, product_url)
    cursor.execute(sql, val)
    result = cursor.fetchone()
    return result[0] > 0

def insert_into_database(connection, cursor, url_country, domain, shop_name, product_id, product_name, price_text, stock, product_url):
    if check_duplicate_record(cursor, product_url, product_name, domain, shop_name, price_text):
        logger.info("Duplicate record detected. Skipping insertion.")
        return
    
    import_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Get current date and time
    sql = """INSERT INTO webscraping
            (country, domain, product_ID, product_name, shop_name, price_text, stock, product_url, import_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    val = (url_country, domain, product_id, product_name, shop_name, price_text, stock, product_url, import_date)
    try:
        cursor.execute(sql, val)
        connection.commit()
        logger.info("Data inserted into database successfully.")
    except mysql.connector.Error as error:
        connection.rollback()  # Rollback the transaction in case of an error
        logger.error(f"Error inserting data into database: {error}")
    except Exception as e:
        logger.error(f"Unknown error occurred: {e}")

def extract_data_emag(product_url, domain, url_country, soup):
    try:
        with connect_to_database() as connection:
            if not connection:
                logging.info("Failed to connect to the database. Exiting.")
                return
            
            cursor = connection.cursor()

            product_name = soup.find('h1').get_text(strip=True)
            product_id = soup.find('button', class_="add-to-favorites")['data-productid']

            shop_name = soup.find('div', class_="product-highlight highlight-vendor").find('a').get_text(strip=True)

            price_text = soup.find('div', class_="pricing-block").find('p', class_ = "product-new-price").get_text(strip=True)
            stock = soup.find('div', class_="stock-and-genius").get_text(strip=True)

            insert_into_database(connection, cursor, url_country, domain, shop_name, product_id, product_name, price_text, stock, product_url)

            more_offers = soup.find_all('div', class_ = "mt-1")

            for offer in more_offers:
                try:
                    vendor_name = offer.find('div', class_ ="other-offers-item-vendor-details").find('a').get_text(strip=True)
                except:
                    vendor_name = offer.find('div', class_ ="other-offers-item-vendor-details").find('strong').get_text(strip=True)
                
                vendor_price = offer.find('div', class_ = "other-offers-item-price").find('p', class_ = "product-new-price").get_text(strip=True)

                vendor_stock = offer.find('p', class_ = "other-offers-badges-container").find('span', class_ = "label").get_text(strip=True) 

                insert_into_database(connection, cursor, url_country, domain, vendor_name, product_id, product_name, vendor_price, vendor_stock, product_url)
                
            cursor.execute(f"SELECT count(*) from webscraping where product_url = '{product_url}'")

            data_len = cursor.fetchone()[0]
            write_to_csv(product_url, 'success', data_len, url_country)
            # time.sleep(random.uniform(10, 15))
    except Exception as e:
        logging.error(f"Error in extract_data_bg for {product_url}: {e}")
        error_message = f"Error in extract_data_bg: {e}"
        write_to_csv(product_url, 'failure', error_message, url_country)

def extract_data_cz_sk(product_url, domain, url_country, soup):
    try:
        with connect_to_database() as connection:
            if not connection:
                logging.info("Failed to connect to the database. Exiting.")
                return

            product_name = soup.find('h1').get_text(strip=True) if soup.find('h1') else 'Error'
            product_id = soup.find('meta', attrs={'name': 'productId'}).get('content') if soup.find('meta', attrs={'name': 'productId'}) else '0'

            options = soup.select('section.c-offer')
            for opt in options:
                shop_name = opt.find('div', class_="c-offer__logo").find('img')['alt'] if opt.find('div', class_="c-offer__logo") else 'Error'
                stock = opt.find('div', class_="c-offer__badges").find('span', attrs={'data-cy': 'variants-badge'}).get_text(strip=True).replace('•', '-') if opt.find('div', class_="c-offer__badges") else 'Error'
                price_text = opt.find('div', class_="c-offer__price").find('span', class_="c-offer__price").get_text(strip=True).replace(',', '.') if opt.find('div', class_="c-offer__price") else 'Error'

                insert_into_database(connection, connection.cursor(), url_country, domain, shop_name, product_id, product_name, price_text, stock, product_url)

            logging.info(f"Data imported successfully from {product_url}")
            with connect_to_database() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT count(*) from webscraping where product_url = '{product_url}'")
                data_len = cursor.fetchone()[0]
                write_to_csv(product_url, 'success', data_len, url_country)
    except Exception as e:
        logging.error(f"Error in extract_data_cz_sk for {product_url}: {e}")
        error_message = f"Error in extract_data_cz_sk: {e}"
        write_to_csv(product_url, 'failure', error_message, url_country)
        
def extract_data_si_hr(product_url, domain, url_country, soup):
    try:
        with connect_to_database() as connection:
            if not connection:
                logging.info("Failed to connect to the database. Exiting.")
                return
            
            cursor = connection.cursor()

            if soup:
                product_name = soup.find('h1').get_text(strip=True)
                product_id = soup.find('div', class_="top-offer").get('data-product-id') if soup.find('div', class_="top-offer") else '0'

                shop_options = soup.find_all('div', class_="premium-product")
                if len(shop_options) == 0:
                    shop_options = soup.find_all('div', class_="offerByBrandB")

                for option in shop_options: 
                    shop = option.find('div', class_="topRow")
                    shop_name = shop.find('a').find('img')['alt'] if shop.find('a').find('img') else 'Error'

                    price_element = shop.find('div', class_="price-value") or shop.find('span', class_="price-value")
                    price_text = price_element.get_text(strip=True) if price_element else 'Price not available'

                    stock = option.find('div', class_='grayRowContent hide').find('i', class_="far fa-truck").find_next('div', class_="details-wrap").get_text(strip=True) if option.find('div', class_='grayRowContent hide').find('i', class_="far fa-truck") else 'N/A'

                    insert_into_database(connection, cursor, url_country, domain, shop_name, product_id, product_name, price_text, stock, product_url)
                
                logging.info(f"Data imported successfully from {product_url}")
                cursor.execute(f"SELECT count(*) from webscraping where product_url = '{product_url}'")
                data_len = cursor.fetchone()[0]
                write_to_csv(product_url, 'success', data_len, url_country)
            
    except Exception as e:
        logging.error(f"Error in extract_data_si_hr for {product_url}: {e}")
        error_message = f"Error in extract_data_si_hr: {e}"
        write_to_csv(product_url, 'failure', error_message, url_country)

def extract_data_bg(product_url, domain, url_country, soup):
    try:
        with connect_to_database() as connection:
            if not connection:
                logging.info("Failed to connect to the database. Exiting.")
                return

            cursor = connection.cursor()

            product_name = soup.find('h1').get_text(strip=True) if soup.find('h1') else 'Error'
            product_id = soup.find('meta', attrs={'name': 'productId'})['content'] if soup.find('meta', attrs={'name': 'productId'}) else '0'

            shop_options = soup.find_all('section', class_="c-offer")
            for option in shop_options:
                shop_name = option.find('div', class_="c-offer__logo").find('a', class_="c-offer__shop-logo-cont")['aria-label'] if option.find('div', class_="c-offer__logo") else 'Error'
                price_text = option.find('div', class_="c-offer__price").get_text(strip=True) if option.find('div', class_="c-offer__price") else 'Error'

                stock_element = option.find('div', class_="c-offer__badges").find('span', class_="c-offer__badge c-offer__badge--green c-offer__badge--clickable") or option.find('div', class_="c-offer__badges").find('span', class_="c-offer__badge c-offer__badge--clickable")
                stock = stock_element.get_text(strip=True) if stock_element else 'N/A'

                insert_into_database(connection, cursor, url_country, domain, shop_name, product_id, product_name, price_text, stock, product_url)

            cursor.execute(f"SELECT count(*) from webscraping where product_url = '{product_url}'")
            data_len = cursor.fetchone()[0]
            write_to_csv(product_url, 'success', data_len, url_country)
            logging.info(f"Data imported successfully from {product_url}")

    except Exception as e:
        logging.error(f"Error in extract_data_bg for {product_url}: {e}")
        error_message = f"Error in extract_data_bg: {e}"
        write_to_csv(product_url, 'failure', error_message, url_country)

def extract_data_hu(product_url, domain, url_country, soup):
    try:
        with connect_to_database() as connection:
            if not connection:
                logging.info("Failed to connect to the database. Exiting.")
                return

            cursor = connection.cursor()

            product_name = soup.find('h1').get_text(strip=True)
            shop_offers = soup.find_all('div', class_="optoffer")
            product_id = soup.find('a', class_="product-image-wrapper").get('data-compare-image') if soup.find('a', class_="product-image-wrapper") else \
                            soup.find('span', class_="product-image-wrapper").get('data-compare-image', 'N/A')
            
            for offer in shop_offers:
                shop_name = offer.find('div', class_="col-logo").find('img').get('alt') if offer.find('div', class_="col-logo").find('img') else \
                            offer.find('div', class_="logo-host").get_text(strip=True) if offer.find('div', class_="logo-host") else 'N/A'
                price_text = offer.find('div', class_="row-price").get_text(strip=True) if offer.find('div', class_="row-price") else 'N/A'
                stock = offer.find('div', class_="delivery-info").get_text(strip=True) if offer.find('div', class_="delivery-info") else 'N/A'
                insert_into_database(connection, cursor, url_country, domain, shop_name, product_id, product_name, price_text, stock, product_url)

            logging.info(f"Data Imported Successfully from - {product_url}")
            cursor.execute(f"SELECT count(*) from webscraping where product_url = '{product_url}'")
            data_len = cursor.fetchone()[0]
            write_to_csv(product_url, 'success', data_len, url_country)
            logging.info(f"Data Imported Successfully from - {product_url}")
    except Exception as e:
        logging.error(f"Error in extract_data_hu for {product_url}: {e}")
        error_message = f"Error in extract_data_hu: {e}"
        write_to_csv(product_url, 'failure', error_message, url_country)
    
def extract_data_pl(product_url, domain, url_country, soup):
    try:
        with connect_to_database() as connection:
            if not connection:
                logging.info("Failed to connect to the database. Exiting.")
                return

            cursor = connection.cursor()

            if soup:
                try:
                    product_name = soup.find('h1').get_text(strip=True)
                except AttributeError:
                    product_name = soup.h1.text.strip()

                try:
                    product_id = soup.find('div', class_="product-top__price-column").get('data-productid', 'N/A')
                except AttributeError:
                    product_id = soup.find('div', id="product-specification").get('data-productid', 'N/A')

                shop_offers = soup.find_all('li', class_="product-offers__list__item")

                for offer in shop_offers:
                    shop_name = offer.find('div', class_="product-offer__logo").find('img').get('alt', 'N/A')
                    price_text = offer.find('div', class_="product-offer__product__price").find('span', class_="price-format nowrap").get_text(strip=True).replace(' ', '').replace(',', '.')
                    stock = offer.find('div', class_="product-availability").find('span').get_text(strip=True)
                    insert_into_database(connection, cursor, url_country, domain, shop_name, product_id, product_name, price_text, stock, product_url)

                logging.info(f"Data Imported Successfully from - {product_url}")

                cursor.execute(f"SELECT count(*) from webscraping where product_url = '{product_url}'")
                data_len = cursor.fetchone()[0]
                write_to_csv(product_url, 'success', data_len, url_country)
                logging.info(f"Data Imported Successfully from - {product_url}")
    except Exception as e:
        logging.error(f"Error in extract_data_pl for {product_url}: {e}")
        error_message = f"Error in extract_data_pl: {e}"
        write_to_csv(product_url, 'failure', error_message, url_country)

def extract_data_ro(product_url, domain, url_country, soup):
    try:
        with connect_to_database() as connection:
            if not connection:
                logging.info("Failed to connect to the database. Exiting.")
                return

            cursor = connection.cursor()

            product_name = soup.find('h1').get_text(strip=True)
            try:
                product_id = soup.find('a', class_="product-image-wrapper").get('data-compare-image', 'N/A')
            except AttributeError:
                product_id = soup.find('span', class_="product-image-wrapper").get('data-compare-image', 'N/A')

            shop_offers = soup.find_all('div', class_="optoffer device-desktop")

            for offer in shop_offers:
                shop_name = offer.find('div', class_="col-logo").find('img').get('alt', 'N/A')

                price_text = offer.find('div', class_="col-price col-price-delivery").find('div', class_="row-price").get_text(strip=True).replace(',', '.') if offer.find('div', class_="col-price col-price-delivery") else 'Error'

                stock = offer.find('div', class_="col-price col-price-delivery").find('div', class_="delivery-info").get_text(strip=True) if offer.find('div', class_="col-price col-price-delivery") else 'Error'

                insert_into_database(connection, cursor, url_country, domain, shop_name, product_id, product_name, price_text, stock, product_url)

            logging.info(f"Data Imported Successfully from - {product_url}")
            cursor.execute(f"SELECT count(*) from webscraping where product_url = '{product_url}'")
            data_len = cursor.fetchone()[0]
            write_to_csv(product_url, 'success', data_len, url_country)
            logging.info(f"Data Imported Successfully from - {product_url}")

    except Exception as e:
        logging.error(f"Error in extract_data_ro for {product_url}: {e}")
        error_message = f"Error in extract_data_ro: {e}"
        write_to_csv(product_url, 'failure', error_message, url_country)
        if cursor:
            cursor.close()

def extract_data_de_at_fr_it(product_url, domain, url_country, soup):
    try:
        with connect_to_database() as connection:
            if not connection:
                logging.info("Failed to connect to the database. Exiting.")
                return

            cursor = connection.cursor()
            selector = 'li.productOffers-listItem'
            
            if soup.find("ul", class_ = "productOffers-list").find('span', class_=  "productOffers-listItemTitleInnerEmpty"):
                error_msg = soup.find("ul", class_ = "productOffers-list").find('span', class_=  "productOffers-listItemTitleInnerEmpty").get_text(strip=True)
                logging.info(f"There is no Offers Available - {product_url}")
                write_to_csv(product_url, "failure", error_msg, url_country)
                return None
            
            product_name = soup.find('h1').get_text(strip=True)
            for offer in soup.select(selector):
                product_data = json.loads(offer.select_one('a.productOffers-listItemTitle')['data-dl-click'])
                shop_name = product_data.get('shop_name', 'N/A')
                other_data = product_data.get('products')[0]
                product_id = other_data.get("id", "N/A")

                price_text = offer.find('a', class_ = "productOffers-listItemOfferPrice").get_text(strip=True)

                stock = offer.select_one('p.productOffers-listItemOfferDeliveryStatus').get_text(strip=True)
                print(product_name, " - ", product_id, " - ", shop_name, " - ", price_text, " - ", stock)
                insert_into_database(connection, cursor, url_country, domain, shop_name, product_id, product_name, price_text, stock, product_url)
            logger.info("Data inserted successfully.")
            cursor.execute(f"SELECT count(*) from webscraping where product_url = '{product_url}'")
            data_len = cursor.fetchone()[0]
            write_to_csv(product_url, 'success', data_len, url_country)

    except Exception as e:
        logging.error(f"Error in extract_data_de_at_fr_it for {product_url}: {e}")
        error_message = f"Error in extract_data_de_at_fr_it: {e}"
        write_to_csv(product_url, 'failure', error_message, url_country)
    finally:
        if cursor:
            cursor.close()

def connect_to_database():
# login: host can be localhost or server 113.30.191.170, databases: dev or CSW

    try:
#         connection = mysql.connector.connect(host="113.30.191.170", user="root", password="DyrHjC3dB1DyrHjC3dB1", database="dev")  # For development database
#         connection = mysql.connector.connect(host="113.30.191.170", user="root", password="xxx", database="CSW")  # For production database
#         connection = mysql.connector.connect(host="localhost", user="root", password="", database="dev")  # For local development
       
        connection = mysql.connector.connect(host="localhost",user="root",password="Ckishan@333",database="web_url")
        logging.info("Connected to the database.")
        return connection
    except mysql.connector.Error as e:
        logging.error("Error connecting to MySQL:", e)
        return None

def close_connection(connection):
    if connection:
        connection.close()
        logging.info("Connection closed.")

def extract_data_by_country(product_url, domain, url_country, soup):
    error_message = "Skipping URL - doesn't contain required content"
    if url_country in ('CZ', 'SK'):
        if soup.find("div", class_ = "c-notice--no-offers"):
            error_msg = soup.find("div", class_ = "c-notice--no-offers").find('section', class_ = "c-notice").get_text(strip=True)
            logger.info(f"{error_msg} - {product_url}")
            write_to_csv(product_url, "failure", error_msg, url_country)
            return None
        else:
            extract_data_cz_sk(product_url, domain, url_country, soup)
    elif url_country in ('SI', 'HR'):
        if soup.find('div', class_="leftBox"):
            logging.error(f"{error_message}: {product_url}")
            write_to_csv(product_url, 'failure', error_message, url_country)
        else:
            extract_data_si_hr(product_url, domain, url_country, soup)
    elif "emag" in domain:
        if soup.find("div", class_=  "sidebar-container-body"):
            logging.error(f"{error_message}: {product_url}")
            write_to_csv(product_url, 'failure', error_message, url_country)
        else:
            extract_data_emag(product_url, domain, url_country, soup)
    elif url_country == 'HU':
        if soup.find("section", id = "filter-bar"):
            logging.info(f"{error_message} - {product_url}")    
            write_to_csv(product_url, 'failure', error_message, url_country)
        else:
            extract_data_hu(product_url, domain, url_country, soup)
    elif url_country == 'RO':
        if soup.find('section', id="filter-bar"):
            logging.error(f"{error_message}: {product_url}")
            write_to_csv(product_url, 'failure', error_message, url_country)
        else:
            extract_data_ro(product_url, domain, url_country, soup)
    elif url_country == 'BG':
            extract_data_bg(product_url, domain, url_country, soup)
    elif url_country == 'PL':
        if "szukaj" in product_url:
            logging.error(f"{error_message}: {product_url}")
            write_to_csv(product_url, 'failure', error_message, url_country)
        else:
            extract_data_pl(product_url, domain, url_country, soup)
    elif url_country in ('DE', 'IT', 'FR', 'AT'):
        extract_data_de_at_fr_it(product_url, domain, url_country, soup)
    
def webscraping(country_param=None):
    try:
        # Connect to the database
        connection = connect_to_database()
        if not connection:
            logging.error("Failed to connect to the database. Exiting.")
            return

        cursor = connection.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS webscraping (
                country VARCHAR(255),
                domain VARCHAR(255),
                product_ID VARCHAR(255),
                product_name VARCHAR(255),
                shop_name VARCHAR(255),
                price_text VARCHAR(255),
                stock VARCHAR(255),
                product_url VARCHAR(1024),
                import_date TEXT,
                rownum INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                price decimal(12,2),
                price_nodph_dm decimal(12,2),
                price_nodph_cm decimal(12,2)
            )
        """)

        # Define valid country codes
        valid_countries = {'CZ', 'SK', 'SI', 'HR', 'DE', 'AT', 'FR', 'IT', 'PL', 'BG', 'RO', 'HU'}

        if country_param and country_param in valid_countries:
            sql_query = f"SELECT DISTINCT country, product_url FROM web_url WHERE country = '{country_param}'"
        else:
            logging.error("Please enter a valid country code.")
            return

        cursor.execute(sql_query)
        rows = cursor.fetchall()

        # Function to process each URL row
        def process_url(row):
            url_country, product_url = row
            domain = get_domain(product_url)

            logging.info(f"Processing URL: {product_url}")
            if (url_country in ('AT', 'IT', 'DE', 'FR', 'CZ', 'SK', 'BG')) or ("emag" in domain):
                if "geizhals" in domain:
                    logger.info(f"Skipping URL: {product_url}")
                    return None
                else:            
                    soup = get_html_content(product_url, url_country, domain)
            else:
                soup = geturl(product_url, url_country)
            if soup:
                extract_data_by_country(product_url, domain, url_country, soup)

        # Process URLs concurrently using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.map(process_url, rows)

        connection.commit()
        logging.info(f"Web scraping completed")

    except mysql.connector.Error as e:
        # Handle database errors
        logging.error(f"Database error: {e}")

    except Exception as ex:
        # Handle other exceptions
        logging.error(f"An error occurred: {ex}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            logging.info("Connection closed.")

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        country_param = sys.argv[1]
        if country_param == 'ALL':
            countries = ('AT', 'DE', 'FR', 'IT', 'CZ', 'SK', 'SI', 'BG', 'HR', 'BR', 'HR', 'PL', 'HU')
            for country_param in countries:
                webscraping(country_param)
        else:
            webscraping(country_param)

