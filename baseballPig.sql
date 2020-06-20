/* Title: Baseball Stats */ 
/* Author: Marc Petta */

/* The birthMonth/birthState combination that produced the worst players by  minimizing (number of hits (H) / number of at bats (AB))*/

batters = LOAD 'hdfs:/user/maria_dev/pigtest/Batting.csv' 
			USING PigStorage(',') 
			AS (playerID:chararray,yearID:int,teamID:chararray,lgID:chararray,G:int,AB:int,R:int,H:int,B2:int,B3:int,HR:int,RBI:int,SB:int,CS:int,BB:int,SO:int,IBB:int,HBP:int,SH:int,SF:int,GIDP:int);

realbatters = FILTER batters BY $1>0;
new_batters = FOREACH realbatters GENERATE $0 AS player, $7 AS H, $5 As AB;
A = GROUP new_batters BY ($0);
B = FOREACH A GENERATE FLATTEN($0) AS (player), 
							  (SUM(new_batters.H)) AS total_H, 
							  (SUM(new_batters.AB)) AS total_AB;
C = FOREACH B GENERATE $0 AS player, $1 AS total_H, $2 AS total_AB;

master = LOAD 'hdfs:/user/maria_dev/pigtest/Master.csv' 
			  USING PigStorage(',') 
			  AS (playerID:chararray,birthYear:int,birthMonth:int,birthDay:int,birthCountry:chararray,birthState:chararray,birthCity:chararray,deathYear:int,deathMonth:int,deathDay:int,deathCountry:chararray,deathState:chararray,deathCity:chararray,nameFirst:chararray,nameLast:chararray,nameGiven:chararray,weight:int,height:int,bats:chararray,throws:chararray,debut:datetime,finalGame:datetime,retroID:chararray,bbrefID:chararray);

realmaster = FILTER master BY $1>0;  
statefilter = FILTER realmaster BY $5 IS NOT null;
monthfilter = FILTER statefilter BY $2 IS NOT null;
new_master = FOREACH monthfilter GENERATE $0 AS player, $2 AS month, $5 As state;

get_state_player = JOIN new_master BY player, C BY player;
E = GROUP get_state_player BY ($1,$2);
F = FOREACH E GENERATE FLATTEN($0) AS (month,state), 
			SUM(get_state_player.total_H) AS total_H, 
			SUM(get_state_player.total_AB) AS total_AB, 
			COUNT(get_state_player.$0) AS player_count;			
G = FOREACH F GENERATE $0 AS month, $1 AS state, $2 AS total_h, $3 AS total_AB, $4 AS player_count, 
						(float)((float)total_H/(float)total_AB) AS score;
						 
/*the sum of the at-bats for all of the players from the same birthMonth/birthState exceeds 1500.*/
AB_filter = FILTER G BY $3>1500;
/*at least 10 people came from the same state and were born in the same month*/
PC_filter = FILTER AB_filter BY $4>9;
/*PC_filter: {month: int,state: chararray,total_H: long,total_AB: long,player_count: long}*/
final = FOREACH PC_filter GENERATE $0 AS month, $1 AS state, $5 AS score;

sorted = ORDER final BY score ASC;
ranked = RANK sorted BY score ASC DENSE;
topranked = FILTER ranked BY rank_sorted ==1;
H = FOREACH topranked GENERATE $1 AS month, $2 AS state;
DUMP H;

OUTPUT:
(6,MN)


/* PlayerID’s of the top 3 ranked players from 2005 through 2009 (including 2005 and 2009) who maximized (number of hits (H) / number of at bats (AB)) – (number of errors (E) / number of games (G)) */
fielders = LOAD 'hdfs:/user/maria_dev/pigtest/Fielding.csv' 
				USING PigStorage(',') 
				AS (playerID:chararray,yearID:int,teamID:chararray,lgID:chararray,POS:chararray,G:int,GS:int,InnOuts:int,PO:int,A:int,E:int,DP:int,PB:int,WP:int,SB:int,CS:int,ZR:int);

realfielders = FILTER fielders BY $1>2004;
realfielders2 = FILTER realfielders BY $1<2010;
realfielders3 = FILTER realfielders2 BY (PO is not null);

new_fielders = FOREACH realfielders3 GENERATE $0 AS player, $10 AS E, $5 As G; 
A1 = GROUP new_fielders BY ($0);
B1 = FOREACH A1 GENERATE FLATTEN($0) AS (player), (SUM(new_fielders.E)) AS total_E, (SUM(new_fielders.G)) AS total_G;
B1_filter = FILTER B1 BY total_G>19;

batters = LOAD 'hdfs:/user/maria_dev/pigtest/Batting.csv' 
				USING PigStorage(',') 
				AS (playerID:chararray,yearID:int,teamID:chararray,lgID:chararray,G:int,AB:int,R:int,H:int,B2:int,B3:int,HR:int,RBI:int,SB:int,CS:int,BB:int,SO:int,IBB:int,HBP:int,SH:int,SF:int,GIDP:int);

realbatters = FILTER batters BY $1>2004;
realbatters2 = FILTER realbatters BY $1<2010;
new_batters = FOREACH realbatters2 GENERATE $0 AS player, $7 AS H, $5 As AB;
A2 = GROUP new_batters BY ($0);
B2 = FOREACH A2 GENERATE FLATTEN($0) AS (player), (SUM(new_batters.H)) AS total_H, (SUM(new_batters.AB)) AS total_AB;
B2_filter = FILTER B2 BY total_AB>39;

get_stats = JOIN B1_filter BY player, B2_filter BY player;
C = FOREACH get_stats GENERATE $0 AS player, (float)(((float)B2_filter::total_H / (float)B2_filter::total_AB) - ( (float)B1_filter::total_E / (float)B1_filter::total_G)) AS score;
sorted = ORDER C BY score DESC;
ranked = RANK sorted BY score DESC DENSE;
topranked = FILTER ranked BY rank_sorted <4;
D = FOREACH topranked GENERATE $1 AS player;
DUMP D;

OUTPUT:
(1,escobal01)
(2,suzukic01)
(3,hoppeno01)


/* Top ranked cities that maximized the sum of the number of doubles and triples for each birthCity/birthState combination*/
batters = LOAD 'hdfs:/user/maria_dev/pigtest/Batting.csv' 
				USING PigStorage(',') 
				AS (playerID:chararray,yearID:int,teamID:chararray,lgID:chararray,G:int,AB:int,R:int,H:int,B2:int,B3:int,HR:int,RBI:int,SB:int,CS:int,BB:int,SO:int,IBB:int,HBP:int,SH:int,SF:int,GIDP:int);

realbatters = FILTER batters BY $1>0;
new_batters = FOREACH realbatters GENERATE $0 AS player, $8 AS dub, $9 As trip;
A = GROUP new_batters BY ($0);
B = FOREACH A GENERATE FLATTEN($0) AS (player), ((SUM(new_batters.dub)) + (SUM(new_batters.trip))) AS total;

master = LOAD 'hdfs:/user/maria_dev/pigtest/Master.csv' 
			   USING PigStorage(',') 
			   AS (playerID:chararray,birthYear:int,birthMonth:int,birthDay:int,birthCountry:chararray,birthState:chararray,birthCity:chararray,deathYear:int,deathMonth:int,deathDay:int,deathCountry:chararray,deathState:chararray,deathCity:chararray,nameFirst:chararray,nameLast:chararray,nameGiven:chararray,weight:int,height:int,bats:chararray,throws:chararray,debut:datetime,finalGame:datetime,retroID:chararray,bbrefID:chararray);

realmaster = FILTER master BY $1>0;  
statefilter = FILTER realmaster BY $5 IS NOT null;
/*The birthCity must start with a vowel (i.e an A, E, I, O or U). (?i) is used to ensure not case sensitive*/
cityfilter = FILTER statefilter BY (birthCity MATCHES '(?i)A.*') 
								OR (birthCity MATCHES '(?i)E.*') 
								OR (birthCity MATCHES '(?i)I.*') 
								OR (birthCity MATCHES '(?i)O.*') 
								OR ($6 MATCHES '(?i)U.*');   
								
new_master = FOREACH cityfilter GENERATE $0 AS player, $5 AS state, $6 As city;
get_city_player = JOIN new_master BY player, B BY player;
C = GROUP get_city_player BY ($2,$1);
D = FOREACH C GENERATE FLATTEN($0) AS (city,state), SUM(get_city_player.total) AS total;
sorted = ORDER D BY total DESC;
ranked = RANK sorted BY total DESC DENSE;
topranked = FILTER ranked BY rank_sorted <6;
E = FOREACH topranked GENERATE $1 AS city, $2 AS state;
DUMP E;

OUTPUT:
(1,Atlanta,GA)
(2,Oakland,CA)
(3,Oklahoma City,OK)
(4,Austin,TX)
(5,Utica,NY)


/* PlayerID and team of the player who had the most errors with any 1 team in all seasons combined. */
fielders = LOAD 'hdfs:/user/maria_dev/pigtest/Fielding.csv' 
				USING PigStorage(',') 
				AS (playerID:chararray,yearID:int,teamID:chararray,lgID:chararray,POS:chararray,G:int,GS:int,InnOuts:int,PO:int,A:int,E:int,DP:int,PB:int,WP:int,SB:int,CS:int,ZR:int);

realfielders = FILTER fielders BY $1>1950;
realfielders2 = FILTER realfielders BY $10>0; 
realfielders3 = FILTER realfielders2 BY $10>0; 
new_fielders = FOREACH realfielders2 GENERATE $0 AS player, $2 AS team, $10 As error;
A = GROUP new_fielders BY ($0, $1);
B = FOREACH A GENERATE FLATTEN($0) AS (player, team), (SUM(new_fielders.error)) AS total;
sorted = ORDER B BY total DESC;
ranked = RANK sorted BY total DESC DENSE;
topranked = FILTER ranked BY rank_sorted ==1;
C = FOREACH topranked GENERATE $1 AS player, $2 AS team;
DUMP C;

OUTPUT:
(russebi01,LAN)


/* The birth city of the player who had the most runs batted in in his career. */
batters = LOAD 'hdfs:/user/maria_dev/pigtest/Batting.csv' 
				USING PigStorage(',') 
				AS (playerID:chararray,yearID:int,teamID:chararray,lgID:chararray,G:int,AB:int,R:int,H:int,B2:int,B3:int,HR:int,RBI:int,SB:int,CS:int,BB:int,SO:int,IBB:int,HBP:int,SH:int,SF:int,GIDP:int);

realbatters = FILTER batters BY $1>0;
new_batters = FOREACH realbatters GENERATE $0 AS player, $11 AS rbi:int;
A = GROUP new_batters BY $0;
B = FOREACH A GENERATE FLATTEN($0) AS player, (SUM(new_batters.rbi)) AS total;
sorted = ORDER B BY total DESC;
ranked = RANK sorted BY total DESC;
topranked = FILTER ranked BY rank_sorted == 1;
/*DUMP topranked;*/

master = LOAD 'hdfs:/user/maria_dev/pigtest/Master.csv' 
			   USING PigStorage(',') 
			   AS (playerID:chararray,birthYear:int,birthMonth:int,birthDay:int,birthCountry:chararray,birthState:chararray,birthCity:chararray,deathYear:int,deathMonth:int,deathDay:int,deathCountry:chararray,deathState:chararray,deathCity:chararray,nameFirst:chararray,nameLast:chararray,nameGiven:chararray,weight:int,height:int,bats:chararray,throws:chararray,debut:datetime,finalGame:datetime,retroID:chararray,bbrefID:chararray);

realmaster = FILTER master BY $1>0;
new_master = FOREACH realmaster GENERATE $0 AS player, $6 AS city;
get_city = JOIN topranked BY player, new_master BY player;
C = FOREACH get_city GENERATE $4 AS city;
DUMP C;

OUTPUT:
(Mobile)


/* Top three ranked birthMonth/birthYear that had the most players born. */
master = LOAD 'hdfs:/user/maria_dev/pigtest/Master.csv' 
			   USING PigStorage(',') 
			   AS (playerID:chararray,birthYear:int,birthMonth:int,birthDay:int,birthCountry:chararray,birthState:chararray,birthCity:chararray,deathYear:int,deathMonth:int,deathDay:int,deathCountry:chararray,deathState:chararray,deathCity:chararray,nameFirst:chararray,nameLast:chararray,nameGiven:chararray,weight:int,height:int,bats:chararray,throws:chararray,debut:datetime,finalGame:datetime,retroID:chararray,bbrefID:chararray);

realmaster = FILTER master BY $1>0;
realmaster2 = FILTER realmaster BY $2>0; 
less_master = FOREACH realmaster2 GENERATE $0 AS player, $1 AS year:chararray, $2 AS month:chararray;
new_master = foreach less_master Generate CONCAT ($2, '/', $1), $0 AS player;
A = GROUP new_master BY $0;
B = FOREACH A GENERATE FLATTEN($0) AS monthyear, (COUNT(new_master.player)) AS total;
sorted = ORDER B BY total DESC;
ranked = RANK sorted BY total DESC DENSE;
topranked = FILTER ranked BY rank_sorted < 4;
C = FOREACH topranked GENERATE $1 AS monthyear;
DUMP C;

OUTPUT:
(1,8/1979)
(1,11/1968)
(2,9/1983)
(3,10/1982)
(3,2/1983)

/* Players that had unique heights. */
master = LOAD 'hdfs:/user/maria_dev/pigtest/Master.csv' 
			   USING PigStorage(',') 
			   AS (playerID:chararray,birthYear:int,birthMonth:int,birthDay:int,birthCountry:chararray,birthState:chararray,birthCity:chararray,deathYear:int,deathMonth:int,deathDay:int,deathCountry:chararray,deathState:chararray,deathCity:chararray,nameFirst:chararray,nameLast:chararray,nameGiven:chararray,weight:int,height:int,bats:chararray,throws:chararray,debut:datetime,finalGame:datetime,retroID:chararray,bbrefID:chararray);

realmaster = FILTER master BY $1>0;
realmaster2 = FILTER realmaster BY $17>0; 
less_master = FOREACH realmaster2 GENERATE $0 AS player, $17 AS height;
A = GROUP less_master BY $1;
B = FOREACH A GENERATE FLATTEN($0) AS height, (COUNT(less_master.player)) AS total;
C = FILTER B BY (total==1);
D = FOREACH C GENERATE $0 AS height;
names = FOREACH master GENERATE $17 AS height, $13 AS first, $14 AS last; 
get_names = JOIN D BY height, names BY height;
final = FOREACH get_names GENERATE $2 AS first, $3 AS last;
DUMP final;

OUTPUT:
(Eddie,Gaedel)
(Jon,Rauch)

/* Team, after 1950, that had the most errors in any 1 season. */
fielders = LOAD 'hdfs:/user/maria_dev/pigtest/Fielding.csv' 
				 USING PigStorage(',') 
				 AS (playerID:chararray,yearID:int,teamID:chararray,lgID:chararray,POS:chararray,G:int,GS:int,InnOuts:int,PO:int,A:int,E:int,DP:int,PB:int,WP:int,SB:int,CS:int,ZR:int);

realfielders = FILTER fielders BY $1>1950;
realfielders2 = FILTER realfielders BY $10>0; 
new_fielders = FOREACH realfielders2 GENERATE $1 AS year, $2 AS team, $10 As error;
A = GROUP new_fielders BY ($0, $1);
B = FOREACH A GENERATE FLATTEN($0) AS (year, team), (SUM(new_fielders.error)) AS total;
sorted = ORDER B BY total DESC;
ranked = RANK sorted BY total DESC DENSE;
topranked = FILTER ranked BY rank_sorted ==1;
C = FOREACH topranked GENERATE $2 AS team;
DUMP C;

OUTPUT: 
(NYN)


































