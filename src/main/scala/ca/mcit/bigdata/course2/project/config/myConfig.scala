package ca.mcit.bigdata.course2.project.config

import java.io.FileNotFoundException
import java.sql.{Connection, DriverManager, Statement}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait myConfig {


// Hive Database connection

  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)
  val con: Connection = DriverManager.getConnection("jdbc:hive2://172.17.0.2:10000/default;user=cloudera;password=cloudera")
  Class.forName("org.apache.hive.jdbc.HiveDriver")
  val stmt: Statement = con.createStatement()



  //Hadoop File creation
System.setProperty("hadoop.home.dir", "/")
  val conf = new Configuration()
  conf.set("fs.defaultFS", "hdfs://172.17.0.2:8020")
  val hadoop:FileSystem = FileSystem.get(conf)

  val stagingpath = new Path("/user/deshbandhu1504/project4")
  if (hadoop.exists(stagingpath)) hadoop.delete(stagingpath, true)
  hadoop.mkdirs(stagingpath)

try {



  hadoop.copyFromLocalFile(new Path("/home/deshbandhu/dummy_files/gtfs_stm/trips.txt"), new Path("/user/deshbandhu1504/project4/trips/trips.txt"))
  hadoop.copyFromLocalFile(new Path("/home/deshbandhu/dummy_files/gtfs_stm/frequencies.txt"), new Path("/user/deshbandhu1504/project4/frequencies/frequencies.txt"))
  hadoop.copyFromLocalFile(new Path("/home/deshbandhu/dummy_files/gtfs_stm/calendar_dates.txt"), new Path("/user/deshbandhu1504/project4/calendar_dates/calendar_dates.txt"))

}
catch{
  case e : FileNotFoundException =>print("File not found=="+e)

}

  }

