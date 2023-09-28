
-- Task 1. Cast the columns of the orders_table to the correct data types.
ALTER TABLE orders_table
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN product_quantity TYPE SMALLINT;

-- Task 2. Cast the columns of the dim_users table to the correct data types.
ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE,
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN join_date TYPE DATE,
    ADD PRIMARY KEY (user_uuid);

-- Task 3. Update the dim_store_details table.
ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,
    ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
    ALTER COLUMN opening_date TYPE DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN store_type DROP NOT NULL,
    ALTER COLUMN country_code TYPE VARCHAR(3),
    ALTER COLUMN continent TYPE VARCHAR(255),
    ADD PRIMARY KEY (store_code);

-- Task 4. Make changes to the dim_products table for the delivery team.
UPDATE dim_products
    SET product_price = LTRIM(product_price, '£');

ALTER TABLE dim_products
    RENAME removed TO still_available;

-- Task 5. Update the dim_products table with the required data types.
ALTER TABLE dim_products
    ADD COLUMN weight_class VARCHAR(14),
    ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
    ALTER COLUMN weight TYPE FLOAT,
    ALTER COLUMN "EAN" TYPE VARCHAR(17),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN date_added TYPE DATE,
    ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
    ALTER COLUMN still_available TYPE BOOL USING CASE still_available WHEN 'Still_avaliable' THEN TRUE ELSE FALSE END,
    ADD PRIMARY KEY (product_code);

UPDATE dim_products
    SET weight_class = 
        CASE
            WHEN weight < 2.0 THEN 'Light'
            WHEN weight >= 2.0 AND weight < 40.0 THEN 'Mid_Sized'
            WHEN weight >= 40.0 AND weight < 140.0 THEN 'Heavy'
            WHEN weight >= 140.0 THEN 'Truck_Required'
        END;

-- Task 6. Update the dim_date_times table.
ALTER TABLE dim_date_times
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN month TYPE VARCHAR(2),
    ALTER COLUMN year TYPE VARCHAR(4),
    ALTER COLUMN day TYPE VARCHAR(2),
    ALTER COLUMN time_period TYPE VARCHAR(10),
    ADD PRIMARY KEY (date_uuid);

-- Task 7. Update the dim_card_details table
ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN expiry_date TYPE VARCHAR(5),
    ALTER COLUMN date_payment_confirmed TYPE DATE,
    ADD PRIMARY KEY (card_number);

-- Task 8. Create the primary keys in the dimension table - above
-- Task 9. Add foreign keys to the orders table
ALTER TABLE orders_table
    ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid),
    ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid),
    ADD FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number),
    ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code),
    ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code);




    


