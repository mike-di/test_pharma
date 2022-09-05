# test_pharma

I) Python et Data Engineering

Etapes du DAG

#1. Instantiation du cluster dataproc en utilisant un script init (pour communiquer les env vars composer/airflow) + selection d'une custom image familly

#2. Lancement du premier traitement (géneration du JSON resutlat)

#3. Lancement du traitement adhoc

#4. decomissionnement du cluster crée en 1



1. Code de géneration du Graph JSON, tout est fait sous PySpark :
- Code testé j'arrive à bien génerer le JSON, concernant l'organisation du code, il reste à decouper les fonctions en utils et les mettre dans le fichier utils.py / repertoire utils
- le reformattage des dates n'est pas complet, comme amélioration on aura encore à transformer toutes les dates en (YYYY/MM/DD - MM/DD/YYYY ...) vers du YYYY-MM-DD
- je n'ai pas eu le temps de detailler la mise en place de l'environement local PySpark, mais j'ai bien testé le code est fonctionnel
- comme amélioration supplémentaire, en vue de passer le code en Prod il serait judicieux de partitionner les données par date et parametrer le code par Date, afin d'avoir une pipeline de traitement au jour le jour (sous composer/airflow et le dag joint au code)

2. Pour du code avec une grosse volumétrie quelques points d'attention :
- la restitution du resultat en JSON (conversion du Dataframe) doit se faire par une routine PySpark et non Pandas ralanti le traitement
- partitionnement des données (en date par exemple)
passer les traitement sur un filesystem plus conséquent (GCS par exemple) et non en local
- faire le traitement directement sur cloud public GCP par exemple avec un cluster DataProc (gcloud dataproc jobs submit spark ...)

3. la Partie SQL est dans le fichier SQL, par contrainte de temps scripting et testing je me suis arréter en milieu de la partie 2


II) SQL

les tests ont été fait sur une base de données ORACLE
en ligne sur sqlfiddle.com et choisir oracle 11gR2

creation des tables depuis les données exemples

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




-----------------------------------------------------------------------------------------------
Question 1:

afin d'obtenir le chiffre d'affaire pour un jour donnée on genere un calendrier à partir de 2019 ensuite on applique une jointure avec la table transaction pour obtenir le chiffre d'affaire par jour:
-----------------------------------------------------------------------------------------------

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


-----------------------------------------------------------------------------------------------
Question 2:

pour obtenir les ventes et déco réalisées pour chaque clients on mets en place des cases when ainsi que des jointures entre les tables TRANSACTIONS ainsi que PRODUCT_NOMENCLATURE pour obtenir les ventes par client

-----------------------------------------------------------------------------------------------

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

