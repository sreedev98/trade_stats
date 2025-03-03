from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time

# Constants
IMPORT_URL = "https://tradestat.commerce.gov.in/eidb/icomcntq.asp"
EXPORT_URL = "https://tradestat.commerce.gov.in/eidb/ecomcntq.asp"


# Function to set up Selenium WebDriver
def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run in headless mode
    options.add_argument("--user-data-dir=C:/Temp/ChromeProfile")
    driver = webdriver.Chrome(options=options)
    driver.delete_all_cookies()
    return driver


# Function to format fiscal years
def format_fiscal_year(year):
    return f"{year}-{str(year + 1)[-2:]}"


# Scraping function
def scrape_hs_code(hs_code, start_year, end_year, trade_type):
    url = IMPORT_URL if trade_type.lower() == "import" else EXPORT_URL
    print(f"Scraping {trade_type} data for HS Code {hs_code} from {start_year} to {end_year}...")

    driver = setup_driver()
    driver.get(url)
    time.sleep(2)  # Allow page to load

    yearwise_data = {}

    for year in range(start_year, end_year + 1):
        try:
            driver.get(url)
            time.sleep(1)  # Allow page to reload

            # Select Year
            select = Select(driver.find_element(By.XPATH, '//*[@id="select2"]'))
            select.select_by_value(str(year))

            # Input HS Code
            hs_input = driver.find_element(By.XPATH, '/html/body/div/div[2]/div/form/table[1]/tbody/tr[2]/td[2]/p/input')
            hs_input.clear()
            hs_input.send_keys(str(hs_code))

            # Select USD
            usd_radio = driver.find_element(By.XPATH, '//*[@id="radiousd"]')
            usd_radio.click()

            # Click Submit
            submit_btn = driver.find_element(By.XPATH, '//*[@id="button1"]')
            submit_btn.click()

            # Wait for table to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/div/div[2]/div/table[2]'))
            )

            # Scrape Table Data
            table = driver.find_element(By.XPATH, '/html/body/div/div[2]/div/table[2]')
            rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # Exclude header row

            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) < 4 or "colspan" in row.get_attribute("outerHTML"):  # Skip merged rows
                    continue
                country = cols[1].text.strip()
                value = cols[3].text.strip().replace(',', '')  # Remove commas
                value = float(value) if value else 0.0

                fiscal_year = f"{year}-{str(year + 1)[-2:]}"  # Convert to fiscal year format
                if country not in yearwise_data:
                    yearwise_data[country] = {}
                yearwise_data[country][fiscal_year] = value

        except Exception as e:
            print(f"Error scraping {trade_type} HS code {hs_code} for year {year}: {e}")

    driver.quit()  # Close the browser session

    # Save data to CSV
    df = pd.DataFrame(yearwise_data).T.fillna(0)
    df = df[sorted(df.columns, key=lambda x: int(x.split("-")[0]))]  # Ensure fiscal years are ordered correctly
    file_path = f"{hs_code}_{trade_type.lower()}.csv"
    df.to_csv(file_path)

    return file_path

# Main function
def main(hs_codes, start_year, end_year, choice):
    driver = setup_driver()

    if choice in ["import", "both"]:
        for hs_code in hs_codes:
            data = scrape_hs_code(driver, hs_code, IMPORT_URL, start_year, end_year, "imports")
            save_data(hs_code, data, "imports")

    if choice in ["export", "both"]:
        for hs_code in hs_codes:
            data = scrape_hs_code(driver, hs_code, EXPORT_URL, start_year, end_year, "exports")
            save_data(hs_code, data, "exports")

    driver.quit()
    print("Scraping complete. Data saved.")