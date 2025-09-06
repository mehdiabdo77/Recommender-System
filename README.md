# Order Recommender System

This project is an order recommender system for customers, which suggests products based on previous purchase history, invoice counts, and customer similarity within a specified geographic radius.

## Features
- Calculates customer similarity based on purchase history and geographic location (lat, long)
- Limits the invoice count for each product using the `cv` parameter
- Recommends `n` products to each customer using the `n` parameter
- Uses the Haversine algorithm to calculate distances between customers
- Parallel data processing for improved speed
- Saves results to an Excel file and a MySQL database

## How to Run
1. Place the data files (`data.xlsx` and `data2.xlsx`) in the project directory.
2. Set your database connection information at the top of the code.
3. Run the script:
   ```powershell
   python main.py
   ```
4. The output will be saved in `output_file.xlsx` and in the `recomender` table in your database.

## Parameter Descriptions
- `cv`: Maximum invoice count for each product (purchase cap per customer)
- `n`: Number of products to recommend to each customer
- Customer code: Unique identifier for each customer to receive recommendations
- Data tables: Contain customer purchase information and their geographic locations

## Dependencies
- pandas
- numpy
- dask
- tqdm
- sqlalchemy
- pymysql

To install dependencies:
```powershell
pip install pandas numpy dask tqdm sqlalchemy pymysql
```

## Usage
This system is suitable for stores and businesses that want to provide personalized product recommendations to customers based on their purchase behavior and location, helping to increase sales.
