SELECT * FROM project.customer_datamart;
SELECT * FROM project.transaction_datamart;
SELECT * FROM project.bls_data;



-- Adding an additional column which can be used as PK to reference the table:
ALTER TABLE project.transaction_datamart
ADD COLUMN ind VARCHAR(100);

UPDATE project.transaction_datamart
SET ind = CONCAT(Customer_ID ,Amount,Transaction_Type);
UPDATE transaction_datamart
SET ind = REPLACE(ind, '.', '');

SELECT * FROM project.transaction_datamart;

use project;

-- Adding columns named Annaul_Medain and OCC_name as occupation 

ALTER TABLE customer_datamart
ADD COLUMN Annual_Median VARCHAR(45),
ADD COLUMN Occupation_Name VARCHAR(255);

/* Updating the column A_MEDIAN in the customer_datamart table with A_MEDIAN values from 
bls_data joined on Profession_Code and OCC_CODE */

UPDATE customer_datamart c
LEFT JOIN bls_data b ON c.Profession_Code = b.OCC_CODE
SET c.Annual_Median = b.A_MEDIAN,
    c.Occupation_Name = b.OCC_TITLE;   


-- ------ 1. Customer Datamart data preparation ---------
SELECT * FROM customer_datamart;
DESCRIBE customer_datamart;

-- check for null values in customer_datamart
SELECT * FROM customer_datamart WHERE Annual_Median IS NULL;
-- No null values found

--  Replacing the comma with empty value
UPDATE customer_datamart
SET Annual_Median = REPLACE(Annual_Median, ',', '');

/* Since '*' in the 'annual_median' column signifies an unavailable wage estimate and includes wages greater than $239,200 per year, 
deleting these rows would be a better option as we lack the true estimate of their salary.
*/
delete from customer_datamart where Annual_Median='*' or Annual_Median='#';


-- ------- 2. Transaction Datamart data preparation ----------
SELECT * FROM transaction_datamart;
DESCRIBE transaction_datamart;

-- Convert Timestamp column(which is in text format) into a timestamp format accepted my sql
UPDATE transaction_datamart
SET TimeStamp = STR_TO_DATE(Timestamp, '%m/%d/%Y %H:%i');
-- Change the datatype in navigator section to timestamp

-- No null values found 

-- ----------------------------------------------------------------------------------
-- 2 . Stored procedure development:

-- Ensure the account_profile table exists before creating the procedure
CREATE TABLE IF NOT EXISTS account_profile (
    Customer_ID INT PRIMARY KEY,
    Card_Avg DECIMAL(10,2),
    Check_Avg DECIMAL(10,2),
    Deposit_Avg DECIMAL(10,2),
    Loan_Payment_Avg DECIMAL(10,2),
    Transfer_Avg DECIMAL(10,2),
    Withdrawal_Avg DECIMAL(10,2),
    Card_Count INT,
    Check_Count INT,
    Deposit_Count INT,
    Loan_Payment_Count INT,
    Transfer_Count INT,
    Withdrawal_Count INT
);



DELIMITER $$

CREATE PROCEDURE AccountProfileProcedure()
BEGIN
    DELETE FROM account_profile;

    INSERT INTO account_profile (Customer_ID, Card_Avg, Check_Avg, Deposit_Avg, Loan_Payment_Avg, Transfer_Avg, Withdrawal_Avg, 
                                 Card_Count, Check_Count, Deposit_Count, Loan_Payment_Count, Transfer_Count, Withdrawal_Count)
    SELECT
        Customer_ID, MAX(Card_Avg), MAX(Check_Avg), MAX(Deposit_Avg), MAX(Loan_Payment_Avg),
        MAX(Transfer_Avg), MAX(Withdrawal_Avg), MAX(Card_Count), MAX(Check_Count),
        MAX(Deposit_Count), MAX(Loan_Payment_Count), MAX(Transfer_Count), MAX(Withdrawal_Count)
    FROM
        (
            SELECT
                Customer_ID,
                Transaction_Type,
                AVG(Amount) AS Amount_Avg,
                COUNT(*) AS Transaction_Count,
                CASE WHEN Transaction_Type = 'Card' THEN AVG(Amount) END AS Card_Avg,
                CASE WHEN Transaction_Type = 'Check' THEN AVG(Amount) END AS Check_Avg,
                CASE WHEN Transaction_Type = 'Deposit' THEN AVG(Amount) END AS Deposit_Avg,
                CASE WHEN Transaction_Type = 'Loan Payment' THEN AVG(Amount) END AS Loan_Payment_Avg,
                CASE WHEN Transaction_Type = 'Transfer' THEN AVG(Amount) END AS Transfer_Avg,
                CASE WHEN Transaction_Type = 'Withdrawal' THEN AVG(Amount) END AS Withdrawal_Avg,
                CASE WHEN Transaction_Type = 'Card' THEN COUNT(*) END AS Card_Count,
                CASE WHEN Transaction_Type = 'Check' THEN COUNT(*) END AS Check_Count,
                CASE WHEN Transaction_Type = 'Deposit' THEN COUNT(*) END AS Deposit_Count,
                CASE WHEN Transaction_Type = 'Loan Payment' THEN COUNT(*) END AS Loan_Payment_Count,
                CASE WHEN Transaction_Type = 'Transfer' THEN COUNT(*) END AS Transfer_Count,
                CASE WHEN Transaction_Type = 'Withdrawal' THEN COUNT(*) END AS Withdrawal_Count
            FROM
                transaction_datamart
            GROUP BY
                Customer_ID, Transaction_Type
        ) AS s
    GROUP BY
        Customer_ID;
END$$

DELIMITER ;


CALL AccountProfileProcedure();


-- Trigger Implementation
DELIMITER $$

CREATE TRIGGER AfterTransactionInsert
AFTER INSERT ON transaction_datamart
FOR EACH ROW
BEGIN
    CALL AccountProfileProcedure();
END$$

DELIMITER ;

-- Check if the trigger works by inserting new value to transaction_datamart
INSERT INTO transaction_datamart (Customer_ID, Timestamp, Amount, Transaction_Type)
VALUES (1000,now(), 100.00, 'Card'); 

-- Working but takes alot of time to run - 4.1s

-- Delete the records inserted 
DELETE FROM transaction_datamart 
WHERE Customer_ID=1000 AND Amount=100.00 AND Transaction_Type='Card';

-- -------- An advanced version of AccountProfileProcedure-

DELIMITER $$

CREATE PROCEDURE AdvancedAccountProfileProcedure(customerId INT)
BEGIN
    -- Variables to hold the computed averages and counts
    DECLARE v_card_avg DECIMAL(10,2);
    DECLARE v_check_avg DECIMAL(10,2);
    DECLARE v_deposit_avg DECIMAL(10,2);
    DECLARE v_loan_payment_avg DECIMAL(10,2);
    DECLARE v_transfer_avg DECIMAL(10,2);
    DECLARE v_withdrawal_avg DECIMAL(10,2);
    DECLARE v_card_count INT;
    DECLARE v_check_count INT;
    DECLARE v_deposit_count INT;
    DECLARE v_loan_payment_count INT;
    DECLARE v_transfer_count INT;
    DECLARE v_withdrawal_count INT;

    -- Calculate the new averages and counts for the specific customer
    SELECT
        AVG(CASE WHEN Transaction_Type = 'Card' THEN Amount ELSE NULL END),
        AVG(CASE WHEN Transaction_Type = 'Check' THEN Amount ELSE NULL END),
        AVG(CASE WHEN Transaction_Type = 'Deposit' THEN Amount ELSE NULL END),
        AVG(CASE WHEN Transaction_Type = 'Loan Payment' THEN Amount ELSE NULL END),
        AVG(CASE WHEN Transaction_Type = 'Transfer' THEN Amount ELSE NULL END),
        AVG(CASE WHEN Transaction_Type = 'Withdrawal' THEN Amount ELSE NULL END),
        COUNT(CASE WHEN Transaction_Type = 'Card' THEN 1 ELSE NULL END),
        COUNT(CASE WHEN Transaction_Type = 'Check' THEN 1 ELSE NULL END),
        COUNT(CASE WHEN Transaction_Type = 'Deposit' THEN 1 ELSE NULL END),
        COUNT(CASE WHEN Transaction_Type = 'Loan Payment' THEN 1 ELSE NULL END),
        COUNT(CASE WHEN Transaction_Type = 'Transfer' THEN 1 ELSE NULL END),
        COUNT(CASE WHEN Transaction_Type = 'Withdrawal' THEN 1 ELSE NULL END)
    INTO
        v_card_avg, v_check_avg, v_deposit_avg, v_loan_payment_avg, v_transfer_avg, v_withdrawal_avg,
        v_card_count, v_check_count, v_deposit_count, v_loan_payment_count, v_transfer_count, v_withdrawal_count
    FROM 
        transaction_datamart
    WHERE 
        Customer_ID = customerId;

    -- Update the account_profile for the specific customer
    UPDATE account_profile
    SET
        Card_Avg = v_card_avg,
        Check_Avg = v_check_avg,
        Deposit_Avg = v_deposit_avg,
        Loan_Payment_Avg = v_loan_payment_avg,
        Transfer_Avg = v_transfer_avg,
        Withdrawal_Avg = v_withdrawal_avg,
        Card_Count = v_card_count,
        Check_Count = v_check_count,
        Deposit_Count = v_deposit_count,
        Loan_Payment_Count = v_loan_payment_count,
        Transfer_Count = v_transfer_count,
        Withdrawal_Count = v_withdrawal_count
    WHERE Customer_ID = customerId;

    -- If no row exists for the customer, insert a new row
    IF ROW_COUNT() = 0 THEN
        INSERT INTO account_profile (Customer_ID, Card_Avg, Check_Avg, Deposit_Avg, Loan_Payment_Avg, Transfer_Avg, Withdrawal_Avg, 
                                     Card_Count, Check_Count, Deposit_Count, Loan_Payment_Count, Transfer_Count, Withdrawal_Count)
        VALUES (customerId, v_card_avg, v_check_avg, v_deposit_avg, v_loan_payment_avg, v_transfer_avg, v_withdrawal_avg,
                v_card_count, v_check_count, v_deposit_count, v_loan_payment_count, v_transfer_count, v_withdrawal_count);
    END IF;
END$$

DELIMITER ;

CALL AdvancedAccountProfileProcedure(1000);

DELIMITER $$

CREATE TRIGGER AdvancedAfterTransactionInsert
AFTER INSERT ON transaction_datamart
FOR EACH ROW
BEGIN
    -- Call the stored procedure with the Customer_ID of the newly inserted transaction
    CALL AdvancedAccountProfileProcedure(NEW.Customer_ID);
END$$

DELIMITER ;

-- Check if the trigger and procedure works by inserting new record
INSERT INTO transaction_datamart (Customer_ID, Timestamp, Amount, Transaction_Type)
VALUES (1000,now(), 100.00, 'Card'); 

/* This one is far better than the previous stored procedure
 as this takes 63ms to run on average */
 
 -- ------------------3. Clusttering analysis--
-- Comnbining customer and transaction datamart and storing it into a new table customer_transaction

-- Check if there are any customers who hasn't been involved in any of the transactions
SELECT customer_ID FROM transaction_datamart WHERE customer_ID 
NOT IN (SELECT customer_ID FROM customer_datamart);
-- 0 rows returned indicationg that each customer has done a transaction

CREATE TABLE customer_transaction(
Customer_ID INT ,
Timestamp DATETIME ,
Amount DOUBLE ,
Transaction_Type VARCHAR(45),
Gender VARCHAR(45),
Age INT,
Profession_Code VARCHAR(45),
Work_Experience INT,
Family_Size INT,
Annual_Median VARCHAR(45),
Occupation_Name VARCHAR(255)
);



INSERT INTO customer_transaction (Customer_ID, TimeStamp, Amount, Transaction_Type, Gender, Age, Profession_Code, Work_Experience, Family_Size, Annual_Median, Occupation_Name)
SELECT 
    td.Customer_ID, 
    td.TimeStamp, 
    td.Amount, 
    td.Transaction_Type, 
    cd.Gender, 
    cd.Age, 
    cd.Profession_Code, 
    cd.Work_Experience, 
    cd.Family_Size, 
    cd.Annual_Median, 
    cd.Occupation_Name
FROM 
    transaction_datamart td
LEFT JOIN 
    customer_datamart cd ON td.Customer_ID = cd.Customer_ID;



-- customer segmentation table
CREATE TABLE customer_segmentation(
Customer_ID INT ,
Gender VARCHAR(45),
Age INT,
Profession_Code VARCHAR(45),
Work_Experience INT,
Family_Size INT,
Annual_Median VARCHAR(45),
Occupation_Name VARCHAR(255),
Card_Avg DOUBLE,
Check_Avg DOUBLE,
Deposit_Avg DOUBLE,
Loan_Payment_Avg DOUBLE,
Transfer_Avg DOUBLE,
Withdrawal_Avg DOUBLE,
Card_Count INT,
Check_Count INT,
Deposit_Count INT,
Loan_Payment_Count INT,
Transfer_Count INT,
Withdrawal_Count INT
);

INSERT INTO customer_segmentation (Customer_ID, Gender, Age, Profession_Code, Work_Experience, Family_Size, Annual_Median, Occupation_Name,
Card_Avg, Check_Avg, Deposit_Avg, Loan_Payment_Avg, Transfer_Avg,
Withdrawal_Avg, Card_Count, Check_Count, Deposit_Count, Loan_Payment_Count,
Transfer_Count, Withdrawal_Count)
SELECT 
    cd.Customer_ID, 
    cd.Gender, 
    cd.Age, 
    cd.Profession_Code, 
    cd.Work_Experience, 
    cd.Family_Size, 
    cd.Annual_Median, 
    cd.Occupation_Name,
    ap.Card_Avg,
    ap.Check_Avg, 
    ap.Deposit_Avg, 
    ap.Loan_Payment_Avg, 
    ap.Transfer_Avg,
	ap.Withdrawal_Avg, 
    ap.Card_Count, 
    ap.Check_Count, 
    ap.Deposit_Count, 
    ap.Loan_Payment_Count,
	ap.Transfer_Count, 
    ap.Withdrawal_Count
FROM 
    customer_datamart cd
JOIN 
    account_profile ap ON cd.Customer_ID = ap.Customer_ID;

/* This would be the main table I will be working on in jupyter notebook . This data consists 
null values and I will try to clean it in jupyter notebook using python */

-- onto the jupyter notebook for python analysis



