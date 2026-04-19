# Sales Data Pipeline — PySpark & Medallion Architecture

I built this project to practice real-world data engineering workflows 
using PySpark on Databricks. The idea was simple — take a messy, 
dirty sales dataset and transform it into a clean, analytics-ready 
star schema using the Medallion Architecture pattern.

The dataset has around 25,000 rows covering four real-world domains — 
Customers, Products, Orders, and Transactions — with intentional data 
quality issues like nulls, duplicates, wrong data types, inconsistent 
formats, and invalid values.


## What I Built

### Bronze Layer
The raw CSV file is ingested as-is into a Delta table without any 
changes. This layer acts as the single source of truth and preserves 
the original data exactly how it came in.

### Silver Layer
This is where most of the work happens. The raw data goes through a 
full cleaning and transformation process:

- Fixed data types using safe casting (try_cast, try_to_date)
- Removed duplicate records
- Filled missing values with meaningful defaults
- Validated order and transaction statuses against accepted values
- Dropped rows where critical columns like price, quantity, and 
  dates were null
- Split the address column into House No, Street, City, State, Country
- Formatted phone numbers based on the customer's country 
  (India, USA, UK, Germany, France, Australia, Brazil)
- Validated email addresses using regex
- Added date features like year, month, quarter, day of week, 
  and a weekend flag

### Gold Layer
The cleaned silver data is modeled into a star schema with:

- 4 Dimension tables — Customers, Products, Location, Date
- 2 Fact tables — Sales, Transactions

All tables are written as Delta tables in the Gold catalog.

---

## Business Questions Answered

Once the Gold layer was ready, I ran several analytical queries:

- Which country generates the most revenue?
- Who are the top 5 customers by total spending?
- How did sales trend month by month in 2024?
- Which product category drives the most revenue?
- Do weekdays or weekends bring in more sales?
- What is the most sold product in each country?
- Which quarter had the highest revenue in 2024?


## Tech Stack

- PySpark
- Databricks
- Delta Lake
- Medallion Architecture
- SQL
- Python


## What I Learned

This project helped me get hands-on experience with the full data 
engineering lifecycle — from raw ingestion all the way to 
analytics-ready tables. The Silver layer was the most challenging 
part, especially handling multi-country phone formatting and building 
safe type casting without breaking the pipeline on bad data.
