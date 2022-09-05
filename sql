CREATE TABLE TRANSACTIONS (
   date_t       DATE  NOT NULL 
  ,order_id   INTEGER  NOT NULL
  ,client_id  INTEGER  NOT NULL
  ,prod_id    INTEGER  NOT NULL
  ,prod_price FLOAT
  ,prod_qty   INTEGER
);
INSERT INTO TRANSACTIONS(date_t,order_id,client_id,prod_id,prod_price,prod_qty) VALUES (TO_DATE('01/01/2020', 'DD/MM/YYYY'),1234,999,490756,50,1);
INSERT INTO TRANSACTIONS(date_t,order_id,client_id,prod_id,prod_price,prod_qty) VALUES (TO_DATE('01/01/2020', 'DD/MM/YYYY'),1234,999,389728,3.56,4);
INSERT INTO TRANSACTIONS(date_t,order_id,client_id,prod_id,prod_price,prod_qty) VALUES (TO_DATE('01/01/2020', 'DD/MM/YYYY'),3456,845,490756,50,2);
INSERT INTO TRANSACTIONS(date_t,order_id,client_id,prod_id,prod_price,prod_qty) VALUES (TO_DATE('01/01/2020', 'DD/MM/YYYY'),3456,845,549380,300,1);
INSERT INTO TRANSACTIONS(date_t,order_id,client_id,prod_id,prod_price,prod_qty) VALUES (TO_DATE('01/01/2020', 'DD/MM/YYYY'),3456,845,293718,10,6);


CREATE TABLE PRODUCT_NOMENCLATURE(
   product_id   INTEGER  NOT NULL PRIMARY KEY 
  ,product_type VARCHAR(30) NOT NULL
  ,product_name VARCHAR(30) NOT NULL
);
INSERT INTO PRODUCT_NOMENCLATURE(product_id,product_type,product_name) VALUES (490756,'MEUBLE','Chaise');
INSERT INTO PRODUCT_NOMENCLATURE(product_id,product_type,product_name) VALUES (389728,'DECO','Boule de Noel');
INSERT INTO PRODUCT_NOMENCLATURE(product_id,product_type,product_name) VALUES (549380,'MEUBLE','Canapé');
INSERT INTO PRODUCT_NOMENCLATURE(product_id,product_type,product_name) VALUES (293718,'DECO','Mug');



-- Q1
with calendar as (
        select rownum - 1 as daynum
        from dual
        connect by rownum < sysdate - TO_DATE('01/01/2019', 'DD/MM/YYYY') + 1
    )
SELECT  date_c,
COALESCE(sum( TRANSACTIONS.prod_price * TRANSACTIONS.prod_qty),0) as ventes 
FROM (
  select TO_DATE('01/01/2019', 'DD/MM/YYYY') + daynum as date_c
  from calendar c
)
LEFT JOIN TRANSACTIONS
ON date_c=TRANSACTIONS.date_t
group by date_c
ORDER BY date_c;

-- Q2

SELECT TRANSACTIONS.client_id,
sum(CASE
    WHEN  PRODUCT_NOMENCLATURE.product_type='MEUBLE'  
    THEN (TRANSACTIONS.prod_price* TRANSACTIONS.prod_qty) 
    END) as ventes_meuble,
sum(CASE  
    WHEN  PRODUCT_NOMENCLATURE.product_type='DECO'  
    THEN (TRANSACTIONS.prod_price* TRANSACTIONS.prod_qty) 
    END)as ventes_deco
FROM TRANSACTIONS
LEFT JOIN PRODUCT_NOMENCLATURE ON TRANSACTIONS.prod_id=PRODUCT_NOMENCLATURE.product_id
WHERE TRANSACTIONS.date_t BETWEEN TO_DATE('01/01/2019', 'DD/MM/YYYY') and TO_DATE('31/12/2019', 'DD/MM/YYYY')
GROUP BY TRANSACTIONS.client_id;
