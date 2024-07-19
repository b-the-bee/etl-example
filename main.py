from sql_utils import *
import csv
from datetime import datetime
### SETUP THE DATABASE

connection, cursor = setup_db_connection()
create_db_tables(connection, cursor) # set up the sql tables that we will be loading to

### EXTRACT

with open('sales_data.csv', newline='') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
    my_table = []
    header = next(csvreader)
    for row in csvreader:
        my_table.append(row)

### TRANSFORM

# 2. Clean that data (minimum requirement is to remove any rows that contain null cells).
i = 0
for item in my_table:
    for subitem in item:
        if not subitem:
            my_table.pop(i)
    i += 1
            
# 3. Filter data for the period 1 December 2020 - 5 December 2020
filtered_table = [header]
start_date = datetime(2020, 12, 1)
end_date = datetime(2020, 12, 5)

for row in my_table[1:]:
    date_str = row[1] 
    row_date = datetime.strptime(date_str, '%Y-%m-%d')
    
    if start_date <= row_date <= end_date:
        filtered_table.append(row)


# 4. Calculate each customer's total spend
customer_spend = {}
customer_id_index = 0
customer_spend_index = 2

for row in filtered_table[1:]:
    customer_id = row[customer_id_index]
    spend =  float((row[customer_spend_index]))
    if customer_id in customer_spend:
        customer_spend[customer_id] += spend
    else:
        customer_spend[customer_id] = spend
        
rounded_customer_spend = {customer_id: round(total_spend, 2) for customer_id, total_spend in customer_spend.items()}
print(rounded_customer_spend)

# 5. Calculate each customer's average spend
customer_order_count = {}
customer_id_index = 0
order_date_index = 1

unique_orders = set()

for row in filtered_table[1:]:
    customer_id = row[customer_id_index]
    order_date = row[order_date_index]

    order_key = (customer_id, order_date)

    if order_key not in unique_orders:
        unique_orders.add(order_key)

        if customer_id in customer_order_count:
            customer_order_count[customer_id] += 1
        else:
            customer_order_count[customer_id] = 1

print(customer_order_count)
average_spends = {}

for customer_id, total_spend in customer_spend.items():
    orders_count = customer_order_count.get(customer_id, 0)  # Get the count of unique orders

    if orders_count > 0:
        average_spend_value = total_spend / orders_count
        average_spend_value = round(average_spend_value, 2)
        average_spends[customer_id] = average_spend_value
        
print(average_spends)
# 6. Calculate how many times each customer has purchased a specific item
customer_item_count = {}
customer_id_index = 0
customer_item_index = 3  # Assuming the item ID is in the fourth column (index 3)

for row in filtered_table[1:]:  # Skip the header
    customer_id = row[customer_id_index]
    item_id = row[customer_item_index]
    
    if customer_id not in customer_item_count:
        customer_item_count[customer_id] = {}
    
    if item_id in customer_item_count[customer_id]:
        customer_item_count[customer_id][item_id] += 1
    else:
        customer_item_count[customer_id][item_id] = 1
print(customer_item_count)
### LOAD

# 7. Load the transformed data to the created tables

def insert_customer_products(customer_id, product_id, quantity):
    connection, cursor = setup_db_connection()
    insert_sql = """
    INSERT INTO customer_products (customer_id, product_id, quantity)
    VALUES (%s, %s, %s)
    """
    values = (customer_id, product_id, quantity)
    cursor.execute(insert_sql, values)
    connection.commit()

def insert_customer_spend(customer_id, average_spend_per_day, total_spend):
    connection, cursor = setup_db_connection()
    insert_sql = """
    INSERT INTO customer_spend (customer_id, average_spend_per_day, total_spend)
    VALUES (%s, %s, %s)
    """
    values = (customer_id, average_spend_per_day, total_spend)
    cursor.execute(insert_sql, values)
    connection.commit()
        
def insert_sales_data(customer_id, purchase_date, purchase_amount, product_id):
    connection, cursor = setup_db_connection()
    insert_sql = """
    INSERT INTO sales_data (customer_id, purchase_date, purchase_amount, product_id)
    VALUES (%s, %s, %s, %s)
    """
    values = (customer_id, purchase_date, purchase_amount, product_id)
    cursor.execute(insert_sql, values)
    connection.commit()

for customer in customer_item_count:
    customer_id = customer
    for item in customer_item_count[customer_id]:
        item_id = item
        item_count = customer_item_count[customer_id][item_id]
        insert_customer_products(customer_id=customer_id, product_id=item_id, quantity=item_count)


for customer in customer_spend:
    customer_id = customer
    customer_spent = customer_spend[customer_id]
    customer_average_spent = average_spends[customer_id]
    insert_customer_spend(customer_id=customer_id, average_spend_per_day=customer_average_spent, total_spend=customer_spent)

print(filtered_table)
for row in filtered_table[1:]:
    customer_id = row[0]
    purchase_date = row[1]
    purchase_amount = row[2]
    product_id = row[3]
    insert_sales_data(customer_id, purchase_date, purchase_amount, product_id)