Title: Niners Stats
Author: Marc Petta

/* When were the most touchdowns scored in a fifteen minute window in a Niners game in the last ten years?*/

import org.apache.spark.sql._
import org.apache.spark.sql.expressions._

//val df: DataFrame = spark.read.option("header", true).csv("/user/maria_dev/final/nfl5.csv")

/* create view from dataframe to run sql against*/
df.createOrReplaceTempView("dfView")

/*cast types*/
val df1 = spark.sql("SELECT CAST(game_half as STRING) as game_half, 
							CAST(time as STRING) as time, 
							CAST(home_team as STRING) as home_team, 
							CAST(away_team as STRING) as away_team, 
							CAST(game_date as STRING) as game_date, 
							CAST(game_seconds_remaining as INT) as game_seconds_remaining, 
							CAST(safety as INT) as safety, CAST(touchdown as INT) as touchdown, 
							CAST(extra_point_result as STRING) as extra_point_result, 
							CAST(field_goal_result AS STRING) AS field_goal_result, 
							CAST(two_point_conv_result AS STRING) AS two_point_conv_result FROM dfView")

/* create INT variables for the amount of points per score category*/
val df2 = df1.withColumn("tdPoint", when(col("touchdown").equalTo("1"), lit("7")).otherwise("0"))
    .withColumn("expPoint", when(col("extra_point_result").equalTo("good"), lit("1")).otherwise("0"))
    .withColumn("fgPoint", when(col("field_goal_result").equalTo("made"), lit("3")).otherwise("0"))
    .withColumn("sftPoint", when(col("safety").equalTo("1"), lit("2")).otherwise("0"))
    .withColumn("twoPoint", when(col("two_point_conv_result").equalTo("success"), lit("2")).otherwise("0"));
    
/* get only niner games*/
val df3  = df2.filter($"home_team" === "SF" || $"away_team" === "SF").select($"home_team", 
																			 $"away_team", 
																			 $"game_date",
																			 $"time", 
																			 $"game_seconds_remaining", 
																			 $"tdPoint", 
																			 $"expPoint",
																			 $"fgPoint",
																			 $"sftPoint",
																			 $"twoPoint",
																			 $"touchdown")

/*create view from dataframe to run sql against*/
df3.createOrReplaceTempView("df3View")

/* cast types*/
val df4 = spark.sql("SELECT CAST(home_team as STRING) as home_team, 
							CAST(away_team as STRING) as away_team, 
							CAST(time as STRING) as time, 
							CAST(game_date as STRING) as game_date, 
							CAST(game_seconds_remaining as INT) as game_seconds_remaining, 
							CAST(tdPoint as INT) as tdPoint, CAST(expPoint as INT) as expPoint, 
							CAST(fgPoint AS INT) AS fgPoint, CAST(sftPoint AS INT) AS sftPoint, 
							CAST(twoPoint AS INT) AS twoPoint, CAST(touchdown as INT) as touchdown FROM df3View")
							
/* concat for date time*/
val df5 = df4.withColumn("dateTime", concat(col("game_date"), lit(" "), col("time")))

/*only likes when these calls are seperate*/
val df6 = df5.withColumn("uxTime", unix_timestamp($"dateTime", "YYYY-MM-dd HH:mm"))

/*create view from dataframe to run sql against*/
//df6.createOrReplaceTempView("df6View")

val df7 = df6.filter($"touchdown" === "1").select($"home_team", 
												  $"uxTime", 
												  $"away_team", 
												  $"game_date", 
												  $"game_seconds_remaining", 
												  $"tdPoint", $"expPoint",
												  $"fgPoint",
												  $"sftPoint",
												  $"twoPoint",
												  $"touchdown")

val windowSpec = Window.orderBy("uxTime").rangeBetween(-450,450)

val answer = df7.withColumn("totalTDs", count(df7("touchdown")).over(windowSpec))

// create view to run sql against
answer.createOrReplaceTempView("answerView")

// get average per group
val a = spark.sql("SELECT home_team, away_team, game_date, uxTime, totalTDs 
				   FROM answerView 
				   ORDER BY totalTDs DESC LIMIT 10 ")

a.show()

+---------+---------+----------+----------+--------+
|home_team|away_team| game_date|    uxTime|totalTDs|
+---------+---------+----------+----------+--------+
|       GB|       SF|2010-12-05|1261878780|       5||
+---------+---------+----------+----------+--------+

/////////////////////////////////////////////////////////////////////////////////////////


/* In which month have the Niners scored the most touchdown in the last ten years?*/

import org.apache.spark.sql._
import org.apache.spark.sql.expressions._

val df: DataFrame = spark.read.option("header", true).csv("/user/maria_dev/final/nfl5.csv")

/* create view from dataframe to run sql against*/
df.createOrReplaceTempView("dfView")

/* cast types*/
val df1 = spark.sql("SELECT CAST(game_date as STRING) as game_date, 
							CAST(game_half as STRING) as game_half, 
							CAST(home_team as STRING) as home_team, 
							CAST(away_team as STRING) as away_team, 
							CAST(game_seconds_remaining as INT) as game_seconds_remaining, 
							CAST(safety as INT) as safety, CAST(touchdown as INT) as touchdown, 
							CAST(extra_point_result as STRING) as extra_point_result, 
							CAST(field_goal_result AS STRING) AS field_goal_result, 
							CAST(two_point_conv_result AS STRING) AS two_point_conv_result FROM dfView")

/* split game_date to get month*/
val df2 = df1.withColumn("tempDateOne", split($"game_date", "-"))
  .withColumn("year", $"tempDateOne"(0))
  .withColumn("month", $"tempDateOne"(1))
  .withColumn("day", $"tempDateOne"(2))
  .drop("tempDateOne")
  
val df3 = df2.filter($"touchdown" === "1").select($"home_team", 
												  $"away_team", 
												  $"game_date", 
												  $"game_seconds_remaining",
												  $"touchdown",
												  $"month")

/* get only niner games*/
val df4  = df3.filter($"home_team" === "SF" || $"away_team" === "SF").select($"home_team", 
																			 $"away_team", 
																			 $"game_date", 
																			 $"game_seconds_remaining",
																			 $"touchdown",
																			 $"month")

/* create views to run sql against*/
df4.createOrReplaceTempView("df4View")

/* get average per group*/
val answer = spark.sql("SELECT (sum(touchdown)) as totalTDByMonth, month AS Month 
						FROM df4View 
						GROUP BY month 
						ORDER BY totalTDByMonth DESC ")

answer.show()

+--------------+-----+
|totalTDByMonth|Month|
+--------------+-----+
|           196|   12|
|           191|   10|
|           170|   09|
|           147|   11|
|            24|   01|
+--------------+-----+

/////////////////////////////////////////////////////////////////////////////////////////

/* For each season, against which team are the most penalties called in Niners games?*/

import org.apache.spark.sql._
import org.apache.spark.sql.expressions._

val df: DataFrame = spark.read.option("header", true).csv("/user/maria_dev/final/nfl5.csv")

/* create view from dataframe to run sql against*/
df.createOrReplaceTempView("dfView")

/* cast types*/
val df1 = spark.sql("SELECT CAST(penalty as STRING) as penalty, 
							CAST(home_team as STRING) as home_team, 
							CAST(away_team as STRING) as away_team, 
							CAST(game_date as STRING) as game_date, 
							CAST(penalty_team as STRING) as penalty_team FROM dfView")

/* split game_date to get month*/
val df2 = df1.withColumn("tempDateOne", split($"game_date", "-"))
  .withColumn("year", $"tempDateOne"(0))
  .withColumn("month", $"tempDateOne"(1))
  .withColumn("day", $"tempDateOne"(2))
  .drop("tempDateOne")
  
/* get only plays where there was a penalty called against the niners */
val df3 = df2.filter($"penalty" === "1").select($"home_team", 
												$"away_team", 
												$"game_date", 
												$"penalty",
												$"year",
												$"month",
												$"penalty_team")

/* get only plays where niners were called for the penalty */
val df4  = df3.filter($"penalty_team" === "SF").select($"home_team", 
													   $"away_team", 
													   $"game_date", 
													   $"penalty",
													   $"year",
													   $"month",
													   $"penalty_team")

/* create view from dataframe to run sql against */
df4.createOrReplaceTempView("df4View")

/* cast types*/
val df5 = spark.sql("SELECT COUNT(penalty) as totalPenalties, home_team, away_team, year 
					 FROM df4View 
					 GROUP BY year, home_team, away_team 
					 ORDER BY totalPenalties DESC  ")

df5.show()

+--------------+---------+---------+----+
|totalPenalties|home_team|away_team|year|
+--------------+---------+---------+----+
|            20|       SF|      NYG|2018|
|            16|      STL|       SF|2012|
|            16|       SF|      CHI|2014|
|            15|      DET|       SF|2011|
|            14|      STL|       SF|2010|
|            14|       KC|       SF|2018|
|            14|       SF|      STL|2010|
|            13|      ARI|       SF|2017|
|            13|       SF|      ARI|2015|
|            12|      SEA|       SF|2018|
|            12|      CIN|       SF|2011|
|            12|      STL|       SF|2015|
|            12|      SEA|       SF|2013|
|            12|       SF|      ARI|2011|
+--------------+---------+---------+----+
/////////////////////////////////////////////////////////////////////////////////////////

/* What passer thru the most interceptions to the same defender? */

import org.apache.spark.sql._
import org.apache.spark.sql.expressions._

val df: DataFrame = spark.read.option("header", true).csv("/user/maria_dev/final/nfl5.csv")

/* create view from dataframe to run sql against */
df.createOrReplaceTempView("dfView")

/* cast types */
val df1 = spark.sql("SELECT CAST(interception_player_name as STRING) as interception_player_name, 
							CAST(receiver_player_name as STRING) as receiver_player_name, 
							CAST(passer_player_name as STRING) as passer_player_name, 
							CAST(game_date as STRING) as game_date, 
							CAST(home_team as STRING) as home_team, 
							CAST(away_team as STRING) as away_team, 
							CAST(interception as STRING) as interception FROM dfView")

/* get only plays where there was a penalty called against the niners */  
val df2 = df1.filter($"interception" === "1").select($"home_team", 
													 $"away_team", 
													 $"interception_player_name", 
													 $"receiver_player_name",
													 $"passer_player_name",
													 $"interception")

/* create view from dataframe to run sql against */
df2.createOrReplaceTempView("df2View")

/* cast types */
val df3 = spark.sql("SELECT COUNT(interception) as totalInterceptions, interception_player_name, passer_player_name 
					 FROM df2View GROUP BY passer_player_name, interception_player_name 
					 ORDER BY totalInterceptions DESC  ")

df3.show()

+------------------+------------------------+------------------+
|totalInterceptions|interception_player_name|passer_player_name|
+------------------+------------------------+------------------+
|                 6|                R.Nelson|  B.Roethlisberger|
|                 5|                D.Harmon|  B.Roethlisberger|
|                 5|                 J.Haden|          J.Flacco|
|                 4|                M.Peters|          P.Rivers|
|                 4|              C.Greenway|        M.Stafford|
|                 4|               M.Jenkins|         E.Manning|
|                 4|                D.Harmon|       R.Tannehill|
|                 4|               R.Sherman|         J.Skelton|
|                 4|                A.Samuel|         E.Manning|
|                 4|               B.Goodson|         N.Mullens|
|                 4|               R.Johnson|            M.Ryan|
|                 4|                  D.Hall|          J.Cutler|
|                 4|                  L.Webb|          A.Dalton|
|                 4|                B.Grimes|          P.Rivers|
|                 4|                J.Joseph|          J.Flacco|
|                 4|                E.Weddle|          M.Cassel|
|                 4|               R.Sherman|      C.Kaepernick|
+------------------+------------------------+------------------+

/////////////////////////////////////////////////////////////////////////////////////////

/* What team taunts each other the most? */

import org.apache.spark.sql._
import org.apache.spark.sql.expressions._

val df: DataFrame = spark.read.option("header", true).csv("/user/maria_dev/final/nfl5.csv")

/* create view from dataframe to run sql against */
df.createOrReplaceTempView("dfView")

/* cast types */
val df1 = spark.sql("SELECT CAST(penalty as STRING) as penalty, 
							CAST(home_team as STRING) as home_team, 
							CAST(away_team as STRING) as away_team, 
							CAST(game_date as STRING) as game_date, 
							CAST(penalty_type as STRING) as penalty_type FROM dfView")

/* split game_date to get month */
val df2 = df1.withColumn("tempDateOne", split($"game_date", "-"))
  .withColumn("year", $"tempDateOne"(0))
  .withColumn("month", $"tempDateOne"(1))
  .withColumn("day", $"tempDateOne"(2))
  .drop("tempDateOne")
  
/* get only plays where there was a penalty called against the niners */  
val df3 = df2.filter($"penalty" === "1").select($"home_team", 
												$"away_team", 
												$"game_date", 
												$"penalty",
												$"year",
												$"month",
												$"penalty_type")

/* get only plays where niners were called for the penalty */
val df4  = df3.filter($"penalty_type" === "Taunting").select($"home_team", 
															 $"away_team", 
															 $"game_date", 
															 $"penalty",
															 $"year",
															 $"month",
															 $"penalty_type")

/* create view from dataframe to run sql against */
df4.createOrReplaceTempView("df4View")

/* cast types */
val df5 = spark.sql("SELECT COUNT(penalty) as totalPenalties, home_team, away_team  
					FROM df4View 
					GROUP BY home_team, away_team 
					ORDER BY totalPenalties DESC  ")

df5.show()

+--------------+---------+---------+
|totalPenalties|home_team|away_team|
+--------------+---------+---------+
|             3|      CIN|      CLE|
|             3|      MIA|      BUF|
|             2|      SEA|      STL|
|             2|      MIN|      PHI|
|             2|      PHI|      WAS|
+--------------+---------+---------+