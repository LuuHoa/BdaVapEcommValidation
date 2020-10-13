package com.fis.ecomm.validation

import org.apache.spark.sql.SparkSession

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import java.sql.Timestamp

object BdaVapValidation {

  def main(args: Array[String]): Unit = {
    println("Default values")


    val start_time = new Timestamp(System.currentTimeMillis()).toString
    printf("BdaVapValidation::job is started at %s", start_time)
    val schema="dev_audit"
    var table_id=""
    var count_date=""
    var run_date=""
    var current_date=java.time.LocalDate.now.toString
    var target=""
    if (args.length == 2) {
      run_date = args(0)
      table_id = args(1)
    }
    else if(args.length == 3) {

      run_date = args(0)
      table_id = args(1)
      count_date = args(2)
    }
    else {
      println("Option 1:RunDate tableID")
      println("Option 1:RunDate tableID CountDate")
      return
    }





    val spark = SparkSession
      .builder()
      .appName("BdaVapValidation")
      .config("spark.sql.warehouse.dir", "/tmp")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.debug.maxToStringFields", 100)
      .config("spark.scheduler.mode", "FAIR")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    try {

      println("Start running count for tables")

      val df_confsql = spark.sql("select table_name,num_days,sql,date_column_name  from "+schema+".bda_data_validation_conf where table_id = "+table_id);
      val row_confsql = df_confsql.collect.toList(0)
      val table_name = row_confsql.getString(0)
      val num_days = row_confsql.getString(1)
      var date_column_name = row_confsql.getString(3)
      if (count_date==""){
        val current_date_formatted = LocalDate.parse(run_date, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
        count_date=current_date_formatted.minusDays(num_days.toInt).toString

      }
      printf("table Id %s %s with count_date %s ", table_id, table_name, count_date)

      val runtime_sql = row_confsql.getString(2).replace("{var1}", "'"+ count_date +"'")

      printf("runtime_sql %s ", runtime_sql)

      try {
        val df_count = spark.sql(runtime_sql)

        val bda_count = df_count.collect.toList(0).getString(0)
        val end_time = new Timestamp(System.currentTimeMillis()).toString
        //val row_done = Seq((table_id, table_name, summarized_date, runtime_sql, application_id, count, start_time, end_time, "DONE"))
        //val row_done = List(table_id, table_name, summarized_date, runtime_sql, application_id, count, start_time, end_time, "DONE")

        //val row_done = List(1,1,1,1,1,1,1,1, table_id, table_name, count_date, runtime_sql, application_id, bda_count)


        val bda_count_df= Seq((1,1,1,1,1,1,1,1, table_id, table_name, count_date, runtime_sql, bda_count)).toDF("gdg_position", "gdg_txoppos", "gdg_txind", "gdg_opcode", "gdg_timestamp", "gdg_schema", "gdg_table", "id", "table_name", "count_date", "bda_count", "vap_count","runtime_sql")

        bda_count_df.createOrReplaceTempView("vw_bda_count_df")

        val query_rerun = """
             with ranges AS (
             select t1.*, row
             from vap_to_bda_data_validation
             where extract_date='"""+count_date+""" to_date(current_date)

        )
        Select record_count

        from vw_bda_count_df t1 inner join """+schema+""".vap_to_bda_data_validation
        """.stripMargin

        val vap_count_df = spark.sql(
          """
             with ranges AS (
             select *
             from vap_to_bda_data_validation
             where extract_date='"""

             )
            Select record_count

            from vw_bda_count_df t1 inner join """+schema+""".vap_to_bda_data_validation
            """.stripMargin)







        bda_count_df.join()
        var bda_count_df = spark.

        gdg_position ,
        gdg_txoppos ,
        gdg_txind ,
        gdg_opcode ,
        gdg_timestamp,
        gdg_schema ,
        gdg_table ,
        id ,
        table_name ,
        count_date ,
        bda_count ,
        vap_count,
        matched string,
        count_diff bigint,
        date_column_name string,
        inserted_date string

          save_data(row_done, spark, saved_path)
        printf("EcommCountStar::job is completed at %s", end_time)
      } catch {
        case e: Throwable =>
          println(e)
          val end_time = new Timestamp(System.currentTimeMillis()).toString
          printf("EcommCountStar::job is failed at %s", end_time)
          // val row_failed = Seq((table_id, table_name, summarized_date, runtime_sql, application_id, null, start_time, end_time, "ERROR: "+ e.getMessage))
          //val row_failed = Seq((table_id, table_name, summarized_date, runtime_sql, application_id, null, start_time, end_time, "ERROR: "+ e.getMessage))
          val row_failed = List(table_id, table_name, summarized_date, runtime_sql, application_id, null, start_time, end_time, "ERROR: "+ e.getMessage)

          save_data(row_failed, spark, saved_path )
          System.exit(1)}






    } catch {
      case e: Throwable =>
        println(e)

    } finally {
      spark.stop()
    }
  }


}
