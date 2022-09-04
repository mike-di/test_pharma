INSERT DATA :

INSERT INTO 
	testsql.transacts ( 
		date, order_id, client_id, prop_id, prod_price, prod_qty
	)
VALUES
	('2020/10/10',123,8908,21313,200,20),
	('2020/10/10',124,8910,11313,100,30),
	('2020/10/10',124,8910,9813,100,30),
	('2020/10/10',124,8910,113,100,30),
	('2020/10/10',123,8908,313,200,20);



CREATE TABLE product_nomenclature (product_id serial PRIMARY KEY, product_type VARCHAR(128), product_name VARCHAR(123));

testsql=# INSERT INTO
product_nomenclature(
product_type,product_id, product_name
)
VALUES
('DECO',313,'sjdq'),
('MEUBLE',11313,'kqldq'),
('MEUBLE',9813,'sskdla'),
('MEUBLE',113,'jojad'),
('DECO',21313,'aaaz');













