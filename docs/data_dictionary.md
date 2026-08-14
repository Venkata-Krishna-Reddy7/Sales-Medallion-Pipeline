# Data Dictionary — Gold Layer

## dim_customers
| Column | Type | Notes |
|---|---|---|
| dim_customer_id | bigint | surrogate key |
| customer_id | string | natural key |
| customer_name | string | |
| age | int | |
| gender | string | |
| email | string | regex-validated |
| Mobile_Number | string | country-prefixed, formatted |

## dim_products
| Column | Type | Notes |
|---|---|---|
| dim_product_id | bigint | surrogate key |
| product_id | string | natural key |
| product_name | string | |
| product_category | string | |

## dim_location
| Column | Type | Notes |
|---|---|---|
| dim_location_id | bigint | surrogate key |
| customer_id | string | natural key, links to dim_customers |
| House_No | string | parsed from address |
| Street_Name | string | parsed from address |
| City | string | parsed from address |
| State | string | parsed from address |
| Country | string | parsed from address |

## dim_date
| Column | Type | Notes |
|---|---|---|
| dim_date_id | bigint | surrogate key |
| order_date | date | natural key |
| year | int | |
| month | int | |
| day | int | |
| day_of_week | string | e.g. "Monday" |
| quarter | int | |
| is_weekend | boolean | |

## fact_sales
| Column | Type | Notes |
|---|---|---|
| fact_sales_id | bigint | surrogate key |
| order_id | string | |
| customer_id | string | FK → dim_customers, dim_location |
| product_id | string | FK → dim_products |
| order_date | date | FK → dim_date |
| quantity | int | |
| price | int | |
| order_status | string | validated: Delivered / Cancelled / Processing / Shipped / Returned / Unknown |

## fact_transactions
| Column | Type | Notes |
|---|---|---|
| fact_transaction_id | bigint | surrogate key |
| transaction_id | string | |
| order_id | string | FK → fact_sales |
| customer_id | string | FK → dim_customers |
| transaction_date | date | |
| transaction_amount | int | |
| payment_method | string | |
| transaction_status | string | validated: Success / Refunded / Unknown |
| transaction_type | string | derived: Sale / Refund |
