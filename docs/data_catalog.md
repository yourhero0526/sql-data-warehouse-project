# Data Dictionary for Gold Layer

---

## Overview

---

The Gold Layer is the business-level data representation, structured to support analytical and reporting use
cases. It consists of dimension tables and fact tables for specific business metrics.

---

### 1. gold.dim_customers

- Purpose: Stores customer details enriched with demographic and geographic data.
- Columns:

| Column Name | Data Type | Description |
| --- | --- | --- |
| customer_key | INT | Surrogate key uniquely identifying each customer record in the dimension table |
| customer_id | INT | Unique numerical identifier assigned to each customer |
| customer_number | NVARCHAR(50) | Alphaneumeric identifier representing the customer, used for tracking and referencing. |
| first_name | NVARCHAR(50) | Customer’s first name (e.g., ‘Enzo’, ‘David’) |
| last_name | NVARCHAR(50) | Customer’s last name (e.g., ‘De La Pena’, ‘Antonio’) |
| country | NVARCHAR(50) | Country of residence of the customer (e.g., ‘Australia’) |
| marital_status | NVARCHAR(50) | Marital status of the customer (e.g., ‘Married’, ‘Single’) |
| gender | NVARCHAR(50) | Customer’s Gender (e.g., ‘Male’, ‘Female’ |
| birth_date | DATE | customer’s birthdate formatted as YYYY-MM-DD (e.g., 1971-01-01) |
| create_date | DATE | Date and time when the customer record was created in the system |

### 2. gold.dim_products

- Purpose: Provides information about the products and their respective categories
- Columns:

| Column Name | Data Type | Description |
| --- | --- | --- |
| product_key | INT | Surrogate key uniquely identifying each product in the product dimension table |
| product_id | INT | A unique identifier assigned to the product for internal tracking and referencing. |
| product_number | NVARCHAR(50) | A structured alphanumeric code representing the product, often used for categorization or inventory. |
| product_name | NVARCHAR(50) | Descriptive name of the product, including key details such as type, color, and size (e.g., ) |
| category_id | NVARCHAR(50) | A unique identified for the product’s category, linking to its high-level classification. |
| category | NVARCHAR(50) | The high-level classification of the product (e.g., ‘Bikes’, ‘Components’) to group-related items. |
| subcategory | NVARCHAR(50) | A more detailed classification of the product within the category such as product type |
| maintenance | NVARCHAR(50) | Indicates whether the product requires maintenance (e.g., ‘Yes’, ‘No’) |
| product_cost | INT | The cost or base of the product, measured in monetary units. |
| product_line | NVARCHAR(50) | The specific product line or series to which the product belongs to (e.g., ‘Road’, ‘Mountain’) |
| product_start_date | DATE | The date when the product became available for sale or use, stored in  |

### 2. gold.fact_sales

- Purpose: Stores transactional sales data for anaytical purposes
- Columns:

| Column Name | Data Type | Description |
| --- | --- | --- |
| order_number | NVARCHAR(50) | A unique alphanumeric identifier for each sales order |
| product_key | INT | Surrogate key linking the order to the product dimension table |
| customer_key | INT | Surrogate key linking the order to the customer dimension table |
| order_date | DATE | The date when the order was placed. |
| shipping_date | DATE | The date when the order was shipped to the customer. |
| due_date | DATE | The date when the order payment was due. |
| sales_amount | INT | The total monetary value of the sale for the line item, in whole currency units (e.g., 25) |
| quantity | INT | The number of the units of the product ordered for the line item (e.g., 1) |
| price | INT | The price per unit of the product for the line item, in whole currency units (e.g., 25) |
