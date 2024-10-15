-- running payment sum & max per customer
SELECT
	customer_id,
	rental_id, 
  	payment_date, 
	SUM(amount) OVER(PARTITION BY customer_id) AS customer_total_ytd_amount,
  	MAX(amount) OVER(PARTITION BY customer_id) AS customer_max_ytd_amount
FROM
	payment;

-- ranking customers by total amount spent (brute force ranking asigns different ranks to the same amount value)
SELECT
	payment_id,
  	customer_id, 
  	staff_id
	rental_id, 
	amount,
	SUM(amount) OVER(PARTITION BY customer_id) AS customer_total_amount, 
	ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY amount) AS ranking,
	ROW_NUMBER() OVER(ORDER BY amount DESC) AS ranking_2 -- removing partition ranks just on output order
FROM
	payment;

-- ranking accounting for rows with equal amounts using RANK() and DENSE_RANK()
 SELECT
	payment_id,
  	customer_id, 
  	staff_id
	rental_id, 
  	amount,
  	SUM(amount) OVER(PARTITION BY customer_id) AS customer_total_amount, 
  	ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY amount) AS ranking, -- row_number used most often when picking exactly one record from each partition group, usually first or last
	RANK() OVER(PARTITION BY customer_id ORDER BY amount) AS ranking_with_rank, -- matching amounts are ranked the same, next rank reflects overall position in ranking group (ie. 1x9 -> 10)
  	DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY amount) AS ranking_with_dense_rank -- next rank maintains sequential ranking order
FROM
	payment;

-- Taking values from subsequent or previous rows with LEAD() and LAG()
SELECT
	payment_id, 
  	customer_id, 
  	amount, 
  	payment_date,
  	LEAD(amount,1) OVER(PARTITION BY customer_id ORDER BY payment_date) AS next_payment_amount, -- ',1' specifies that we want the value from the next row
  	LAG(amount,1) OVER(PARTITION BY customer_id ORDER BY payment_date) AS previous_payment_amount
FROM
	payment;

-- Combining subqueries & window functions - highest payment amount for each customer (window function can't be in WHERE)
SELECT
	*
FROM
  (SELECT
    payment_id,
    customer_id, 
    staff_id
    rental_id, 
    amount,
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY amount DESC) AS ranking
  FROM
    payment) AS a
WHERE
	ranking = 1;
