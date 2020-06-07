package ca.mcit.bigdata.course2.project

import ca.mcit.bigdata.course2.project.config.myConfig

object Main extends App with myConfig{

/*.txt"))
/.txt"))
t4/calendar_dates/.txt"*/
  val database = "CREATE DATABASE IF NOT EXISTS deshbandhu1504"
  println(database)
  stmt.execute(database)

  //1.==================================================================================================
  val query_trips = " CREATE EXTERNAL TABLE IF NOT EXISTS deshbandhu1504.trips ( "+
    " route_id INT, "+
  " service_id STRING, "+
  " trip_id STRING, "+
  " trip_headsign STRING, "+
  " direction_id INT, "+
  " shape_id INT, "+
  " wheelchair_accessible INT, "+
  " note_fr STRING, "+
  " note_en STRING "+
  ")"+
  "ROW FORMAT DELIMITED "+
    "FIELDS TERMINATED BY ',' "+
  "STORED AS TEXTFILE "+
    "LOCATION '/user/deshbandhu1504/project4/trips/' "+
  "TBLPROPERTIES ('skip.header.line.count' = '1', 'serialization.null.format' = '')"
  println(query_trips)
  stmt.execute(query_trips)
//2.====================================================================================================
val query_calendar_dates ="CREATE EXTERNAL TABLE IF NOT EXISTS deshbandhu1504.calendar_dates ( "+
  "service_id STRING, "+
  "cdate STRING, "+
  "exception_type INT "+
") "+
 " ROW FORMAT DELIMITED "+
  " FIELDS TERMINATED BY ',' "+
  "STORED AS TEXTFILE "+
  "LOCATION '/user/deshbandhu1504/project4/calendar_dates/' "+
  "TBLPROPERTIES ('skip.header.line.count' = '1', 'serialization.null.format' = '') "
  println(query_calendar_dates)
  stmt.execute(query_calendar_dates)
//3.======================================================================================================
  val query_frequencies = "CREATE EXTERNAL TABLE IF NOT EXISTS deshbandhu1504.frequencies( " +
    "trip_id STRING, " +
    "start_time STRING, " +
    "end_time STRING, " +
    "headway_secs INT " +
    ") " +
  " ROW FORMAT DELIMITED "+
  " FIELDS TERMINATED BY ',' "+
  "STORED AS TEXTFILE "+
  "LOCATION '/user/deshbandhu1504/project4/frequencies/' "+
  "TBLPROPERTIES ('skip.header.line.count' = '1', 'serialization.null.format' = '') "
   stmt.execute(query_frequencies)
  println(query_frequencies)
  //4.===================================================================================================
  val query_dynamic= "set hive.exec.dynamic.partition.mode=nonstrict;"
  println(query_dynamic)
  stmt.execute(query_dynamic) // To allow dynamic partitiions permission

//5.======================================================================================================
  val query_enriched_trip = "CREATE TABLE IF NOT EXISTS deshbandhu1504.enriched_trip ( " +
    "route_id INT, " +
    "service_id STRING, " +
    "trip_id STRING, " +
    "trip_headsign STRING, " +
    "direction_id INT, " +
    "shape_id INT, " +
    //"wheelchair_accessible INT, " + --  Will not take it because of partition we are going to perform
    "note_fr STRING, " +
    "note_en STRING, " +
    //"service_id STRING, " + -- duplicate
    "mdate STRING, " +
    "exception_type INT, " +
    //"trip_id STRING, " + -- duplication
    "start_time STRING, " +
    "end_time STRING, " +
    "headway_secs INT " +
    ") " +
    "PARTITIONED BY (wheelchair_accessible INT)" +
    "STORED AS PARQUET " +
    "TBLPROPERTIES('parquet.compression' = 'GZIP') "
  println(query_enriched_trip)
  stmt.execute(query_enriched_trip)

  // 13 fields
 /* "t.route_id, t.service_id, t.trip_id, t.trip_headsign, t.direction_id,
  "t.shape_id, t.note_fr, t.note_en, d.date,d.exception_type, f.start_time,f.end_time,"
  "f.headway_secs, t.wheelchair_accessible " */
  //6.================================================================================================================
val usedatabse = "use deshbandhu1504 "
  val insertquery_enrich_trip = " INSERT OVERWRITE TABLE enriched_trip PARTITION(wheelchair_accessible) "+
      "  SELECT t.route_id, t.service_id, t.trip_id, t.trip_headsign, t.direction_id, "+
    "t.shape_id, t.note_fr,t.note_en, c.cdate,c.exception_type, f.start_time,f.end_time, "+
    "f.headway_secs, t.wheelchair_accessible as wheelchair_accessible "+
    " FROM trips t LEFT JOIN calendar_dates c ON t.service_id = c.service_id "+
    "LEFT JOIN frequencies f ON t.trip_id = f.trip_id"

  println(insertquery_enrich_trip)
  //println(selectquery_enrich_trip)
  stmt.execute(usedatabse)
  stmt.execute(insertquery_enrich_trip)//+selectquery_enrich_trip)


  stmt.close()
  con.close()
}
