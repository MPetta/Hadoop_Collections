
/* Title: Baseball Stats2 */
/* Author: Marc Petta */


/* The playerID’s of the top 3 ranked players from 2005 through 2009 (including 2005 and 2009) 
   who maximized (number of hits (H) / number of at bats (AB)) – (number of errors (E) / number of games (G)) */

DROP TABLE IF EXISTS batting;
CREATE EXTERNAL TABLE IF NOT EXISTS batting(id STRING, year INT, team STRING, league STRING, games INT, ab INT, runs INT, hits INT, doubles INT, triples INT, homeruns INT, rbi INT, sb INT, cs INT, walks INT, strikeouts INT, ibb INT, hbp INT, sh INT, sf INT, gidp INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/maria_dev/hivetest/batting' tblproperties ("skip.header.line.count"="1");

DROP TABLE IF EXISTS fielding;
CREATE EXTERNAL TABLE fielding(id STRING, year INT, team STRING, league STRING, position STRING, g INT, gs INT, innouts INT, po INT, a INT, e INT, dp INT, pb INT, wp INT, sb INT, cs INT, zr INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/maria_dev/hivetest/fielding' tblproperties ("skip.header.line.count"="1");

DROP VIEW IF EXISTS fFull;
CREATE VIEW fFull AS
SELECT ff.id , ff.e, ff.g, ff.year
FROM fielding AS ff 
WHERE ff.year IS NOT NULL AND ff.innouts IS NOT NULL AND ff.year > 2004 AND ff.year < 2010;

DROP VIEW IF EXISTS bFull;
CREATE VIEW bFull AS
SELECT bf.id , bf.hits, bf.ab, bf.year
FROM batting AS bf 
WHERE bf.hits IS NOT NULL AND bf.ab IS NOT NULL AND bf.year > 2004 AND bf.year < 2010;

DROP VIEW IF EXISTS fSums;
CREATE VIEW fSums AS
SELECT f.id, SUM(f.e) AS totalE, SUM(f.g) AS totalG
FROM fFull AS f
GROUP BY f.id;

DROP VIEW IF EXISTS bSums;
CREATE VIEW bSums AS
SELECT b.id, SUM(b.hits) AS totalH, SUM(b.ab) AS totalAB
FROM bFull AS b
GROUP BY b.id;

DROP VIEW IF EXISTS batField;
CREATE VIEW batField AS
SELECT f.id, f.totalE, f.totalG, b.totalH, b.totalAB
FROM bSums AS b
JOIN fSums AS f on b.id = f.id
WHERE f.totalG > 19 AND b.totalAB > 39;

DROP VIEW IF EXISTS final;
CREATE VIEW final AS
SELECT bf.id, ((bf.totalh/bf.totalab)-(bf.totale/bf.totalg)) AS total
FROM batField AS bf;

SELECT id FROM (SELECT id, total, DENSE_RANK() 
				OVER (ORDER BY total DESC) AS ranked 
				FROM final) subquery 
				WHERE subquery.ranked < 4;

OUTPUT:
escobal01
suzukic01
hoppeno01


/* The birthMonth/birthState combination that minimized (number of hits (H) / number of at bats (AB)). */

DROP TABLE IF EXISTS master;
CREATE EXTERNAL TABLE master(id STRING, byear INT, bmonth INT, bday INT, bcountry STRING, bstate STRING, bcity STRING, dyear INT, dmonth INT, dday INT, dcountry STRING, dstate STRING, dcity STRING, fname STRING, lname STRING, name STRING, weight INT, height INT, bats STRING, throws STRING, debut STRING, finalgame STRING, retro STRING, bbref STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/maria_dev/hivetest/master' tblproperties ("skip.header.line.count"="1");

DROP TABLE IF EXISTS batting;
CREATE EXTERNAL TABLE IF NOT EXISTS batting(id STRING, year INT, team STRING, league STRING, games INT, ab INT, runs INT, hits INT, doubles INT, triples INT, homeruns INT, rbi INT, sb INT, cs INT, walks INT, strikeouts INT, ibb INT, hbp INT, sh INT, sf INT, gidp INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/maria_dev/hivetest/batting' tblproperties ("skip.header.line.count"="1");

DROP VIEW IF EXISTS batSmall;
CREATE VIEW batSmall AS
SELECT b.id AS id, b.hits AS hits, b.ab AS ab
FROM batting AS b;

DROP VIEW IF EXISTS bFull;
CREATE VIEW bFull AS
SELECT bs.id , CASE WHEN LENGTH(hits) > 0 THEN hits ELSE '0' END AS hits, CASE WHEN LENGTH(ab) > 0 THEN ab ELSE '0' END AS ab
FROM batSmall AS bs;

DROP VIEW IF EXISTS bFullG;
CREATE VIEW bFullG AS
SELECT bf.id, SUM(bf.hits) AS totalH, SUM(bf.ab) AS totalAB
FROM bFull AS bf
GROUP BY bf.id;

DROP VIEW IF EXISTS mastSmall;
CREATE VIEW mastSmall AS
SELECT m.id AS id, m.bmonth AS month, m.bstate AS state
FROM master AS m
WHERE m.bmonth IS NOT NULL AND m.bstate IS NOT NULL;

DROP VIEW IF EXISTS mastSmallClean;
CREATE VIEW mastSmallClean AS
SELECT msml.id AS id, CASE WHEN LENGTH(msml.month) > 0 THEN msml.month ELSE 'DELETE' END AS month, CASE WHEN LENGTH(msml.state) > 0 THEN msml.state ELSE 'DELETE' END AS state
FROM mastSmall AS msml
WHERE msml.month IS NOT NULL AND msml.state IS NOT NULL;

DROP VIEW IF EXISTS mastSmall2;
CREATE VIEW mastSmall2 AS
SELECT msml2.id AS id, msml2.month AS month, msml2.state AS state
FROM mastSmallClean AS msml2
WHERE msml2.month != 'DELETE' AND msml2.state != 'DELETE';

DROP VIEW IF EXISTS PlayerMonthState;
CREATE VIEW PlayerMonthState AS
SELECT msm.id, CONCAT(msm.month,'/', msm.state) AS ms, msm.month AS month, msm.state AS state
FROM mastSmall2 AS msm;

DROP VIEW IF EXISTS msTotal;
CREATE VIEW msTotal AS
SELECT c.ms, t.id, t.totalh, t.totalab
FROM bFullG AS t
JOIN PlayerMonthState AS c on t.id = c.id;

DROP VIEW IF EXISTS msfinal;
CREATE VIEW msFinal AS
SELECT f.ms, SUM(f.totalh) AS totalH, SUM(f.totalab) AS totalAB, COUNT(f.id) AS cnt
FROM msTotal AS f
GROUP BY f.ms;

DROP VIEW IF EXISTS Final;
CREATE VIEW Final AS
SELECT f.ms, ((f.totalH)/(f.totalAB)) AS total, f.totalAB, f.cnt
FROM msFinal AS f
WHERE f.totalAB >100 AND f.cnt > 4;

SELECT ms FROM (SELECT ms, total, DENSE_RANK() 
				OVER (ORDER BY total ASC) AS ranked 
				FROM final) subquery 
				WHERE subquery.ranked = 1;

OUTPUT: 
10/San Cristobal


/* The top 5 ranked birthCity/birthState combinations that produced the players who had the most doubles and triples. */

DROP TABLE IF EXISTS master;
CREATE EXTERNAL TABLE master(id STRING, byear INT, bmonth INT, bday INT, bcountry STRING, bstate STRING, bcity STRING, dyear INT, dmonth INT, dday INT, dcountry STRING, dstate STRING, dcity STRING, fname STRING, lname STRING, name STRING, weight INT, height INT, bats STRING, throws STRING, debut STRING, finalgame STRING, retro STRING, bbref STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/maria_dev/hivetest/master' tblproperties ("skip.header.line.count"="1");

DROP TABLE IF EXISTS batting;
CREATE EXTERNAL TABLE IF NOT EXISTS batting(id STRING, year INT, team STRING, league STRING, games INT, ab INT, runs INT, hits INT, doubles INT, triples INT, homeruns INT, rbi INT, sb INT, cs INT, walks INT, strikeouts INT, ibb INT, hbp INT, sh INT, sf INT, gidp INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/maria_dev/hivetest/batting' tblproperties ("skip.header.line.count"="1");

DROP VIEW IF EXISTS PlayerTotal;
CREATE VIEW PlayerTotal AS
SELECT b.id, ((SUM(b.doubles))+(SUM(b.triples))) AS total
FROM batting AS b
GROUP BY b.id;

DROP VIEW IF EXISTS PlayerCityState;
CREATE VIEW PlayerCityState AS
SELECT m.id, CONCAT(m.bcity,'/', m.bstate) AS cs
FROM master AS m
WHERE m.byear IS NOT NULL AND m.bcity IS NOT NULL AND m.bstate IS NOT NULL;

DROP VIEW IF EXISTS CityTotal;
CREATE VIEW CityTotal AS
SELECT c.cs, t.id, t.total
FROM PlayerTotal AS t
JOIN PlayerCityState AS c on t.id = c.id;

DROP VIEW IF EXISTS final;
CREATE VIEW final AS
SELECT f.cs, SUM(f.total) AS total
FROM CityTotal AS f
GROUP BY f.cs;

SELECT cs FROM (SELECT cs, total, DENSE_RANK() 
				OVER (ORDER BY total DESC) AS ranked 
				FROM final) subquery 
				WHERE subquery.ranked < 6;

OUTPUT:
Los Angeles/CA
Chicago/IL
Philadelphia/PA
San Francisco/CA
St. Louis/MO


/*  The birth city of the player who had the most at bats in his career. */

DROP TABLE IF EXISTS master;
CREATE EXTERNAL TABLE master(id STRING, byear INT, bmonth INT, bday INT, bcountry STRING, bstate STRING, bcity STRING, dyear INT, dmonth INT, dday INT, dcountry STRING, dstate STRING, dcity STRING, fname STRING, lname STRING, name STRING, weight INT, height INT, bats STRING, throws STRING, debut STRING, finalgame STRING, retro STRING, bbref STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/maria_dev/hivetest/master' tblproperties ("skip.header.line.count"="1");

DROP TABLE IF EXISTS batting;
CREATE EXTERNAL TABLE IF NOT EXISTS batting(id STRING, year INT, team STRING, league STRING, games INT, ab INT, runs INT, hits INT, doubles INT, triples INT, homeruns INT, rbi INT, sb INT, cs INT, walks INT, strikeouts INT, ibb INT, hbp INT, sh INT, sf INT, gidp INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/maria_dev/hivetest/batting' tblproperties ("skip.header.line.count"="1");

DROP VIEW IF EXISTS PlayerTotal;
CREATE VIEW PlayerTotal AS
SELECT b.id, SUM(b.ab) AS total
FROM batting AS b
GROUP BY b.id;

DROP VIEW IF EXISTS PlayerCity;
CREATE VIEW PlayerCity AS
SELECT m.id, m.bcity AS city
FROM master AS m;

DROP VIEW IF EXISTS CityTotal;
CREATE VIEW CityTotal AS
SELECT c.city, t.id, t.total
FROM PlayerTotal AS t
JOIN PlayerCity AS c on t.id = c.id;

SELECT city FROM (SELECT city, total, DENSE_RANK() 
				  OVER (ORDER BY total DESC) AS ranked 
				  FROM CityTotal) subquery WHERE subquery.ranked = 1;

OUTPUT:
Cincinnati


/* The top three ranked birthdates that had the most players born. */

DROP TABLE IF EXISTS master;
CREATE EXTERNAL TABLE master(id STRING, byear INT, bmonth INT, bday INT, bcountry STRING, bstate STRING, bcity STRING, dyear INT, dmonth INT, dday INT, dcountry STRING, dstate STRING, dcity STRING, fname STRING, lname STRING, name STRING, weight INT, height INT, bats STRING, throws STRING, debut STRING, finalgame STRING, retro STRING, bbref STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/maria_dev/hivetest/master' tblproperties ("skip.header.line.count"="1");

DROP VIEW IF EXISTS mFull;
CREATE VIEW mFull AS
SELECT mf.id , mf.bmonth, mf.bday 
FROM master AS mf 
WHERE mf.bmonth IS NOT NULL AND mf.bday IS NOT NULL ;

DROP VIEW IF EXISTS PlayerBd;
CREATE VIEW PlayerBd AS
SELECT m.id , CONCAT( m.bmonth,'/', m.bday) AS monthday
FROM mFull AS m;

DROP VIEW IF EXISTS BdCnt;
CREATE VIEW BdCnt AS
SELECT b.monthday, COUNT(*) AS total
FROM PlayerBd AS b
GROUP BY b.monthday;

SELECT monthday FROM (SELECT monthday, total, DENSE_RANK() 
					  OVER (ORDER BY total DESC) AS ranked 
					  FROM BdCnt) subquery 
					  WHERE subquery.ranked < 4;
					  
OUTPUT:
11/18
8/15
8/4


/* The second most common weight by rank */

DROP TABLE IF EXISTS master;
CREATE EXTERNAL TABLE master(id STRING, byear INT, bmonth INT, bday INT, bcountry STRING, bstate STRING, bcity STRING, dyear INT, dmonth INT, dday INT, dcountry STRING, dstate STRING, dcity STRING, fname STRING, lname STRING, name STRING, weight INT, height INT, bats STRING, throws STRING, debut STRING, finalgame STRING, retro STRING, bbref STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/maria_dev/hivetest/master' tblproperties ("skip.header.line.count"="1");

DROP VIEW IF EXISTS mFull;
CREATE VIEW mFull AS
SELECT mf.id , mf.weight 
FROM master AS mf 
WHERE mf.weight IS NOT NULL;

DROP VIEW IF EXISTS PlayerWeight;
CREATE VIEW PlayerWeight AS
SELECT m.weight, COUNT(*) AS total
FROM mFull AS m
GROUP BY m.weight;

SELECT weight FROM (SELECT weight, total, DENSE_RANK() 
					OVER (ORDER BY total DESC) AS ranked 
					FROM PlayerWeight) subquery 
					WHERE subquery.ranked = 2;

OUTPUT:
185


/* The team that had the most errors in 2001. */

DROP TABLE IF EXISTS fielding;
CREATE EXTERNAL TABLE fielding(id STRING, year INT, team STRING, league STRING, position STRING, g INT, gs INT, innouts INT, po INT, a INT, e INT, dp INT, pb INT, wp INT, sb INT, cs INT, zr INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/maria_dev/hivetest/fielding' tblproperties ("skip.header.line.count"="1");

DROP VIEW IF EXISTS fFull;
CREATE VIEW fFull AS
SELECT ff.team , ff.e, ff.year 
FROM fielding AS ff 
WHERE ff.e IS NOT NULL AND ff.team IS NOT NULL AND ff.year = 2001 ;

DROP VIEW IF EXISTS TeamE;
CREATE VIEW TeamE AS
SELECT f.team, SUM(f.e) AS total
FROM fFull AS f
GROUP BY f.team;

SELECT team FROM (SELECT team, total, DENSE_RANK() 
				  OVER (ORDER BY total DESC) AS ranked 
				  FROM TeamE) subqueryWHERE subquery.ranked = 1;

/*OUTPUT:*/
SDN


/* The playerID(s) of the player who had the most errors in all seasons combined. */

DROP TABLE IF EXISTS fielding;
CREATE EXTERNAL TABLE fielding(id STRING, year INT, team STRING, league STRING, position STRING, g INT, gs INT, innouts INT, po INT, a INT, e INT, dp INT, pb INT, wp INT, sb INT, cs INT, zr INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/maria_dev/hivetest/fielding' tblproperties ("skip.header.line.count"="1");

DROP VIEW IF EXISTS fFull;
CREATE VIEW fFull AS
SELECT ff.id , ff.e
FROM fielding AS ff 
WHERE ff.e IS NOT NULL;

DROP VIEW IF EXISTS idE;
CREATE VIEW idE AS
SELECT f.id, SUM(f.e) AS total
FROM fFull AS f
GROUP BY f.id;

SELECT id FROM (SELECT id, total, DENSE_RANK() 
				OVER (ORDER BY total DESC) AS ranked 
				FROM idE) subquery 
				WHERE subquery.ranked = 1;

OUTPUT:
longhe01

