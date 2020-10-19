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
    var run_date=""
    var table_id=""
    var table_name=""
    var count_date=""
    var bda_count = 0L
    var runtime_sql=""
    var query_rerun=""
    var date_column_name=""
    val target_path="/lake/audit/ecomm_parquet/bda_data_counts_validation/"
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
    val run_date_formatted = LocalDate.parse(run_date, DateTimeFormatter.ofPattern("yyyy-MM-dd"))






    val spark = SparkSession
      .builder()
      .appName("BdaVapValidation")
      .config("spark.sql.warehouse.dir", "/tmp")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.debug.maxToStringFields", 32000)
      .config("spark.scheduler.mode", "FAIR")
      .enableHiveSupport()
      .getOrCreate()

    /*spark.sparkContext.setLogLevel("INFO")*/

    var application_id = spark.sparkContext.getConf.getAppId
    println("Spark application Id: " + application_id)


    try {

      println("Start running count for table:")

      val sql_conf_tab = "select table_name,num_days,sql,date_column_name  from "+schema+".bda_data_validation_conf where table_id = "+table_id
      val df_confsql = spark.sql(sql_conf_tab);
      val row_confsql = df_confsql.collect.toList(0)
      table_name = row_confsql.getString(0)
      val ANYnum_days = row_confsql.get(1)
      var num_days = 0
      if (ANYnum_days != null)
      { num_days = ANYnum_days.toString.toInt }
      date_column_name = row_confsql.getString(3)
      if (count_date==""){
        count_date=run_date_formatted.minusDays(num_days).toString
      }
      printf("table Id: %s - %s - count_date: %s ", table_id, table_name, count_date)
      runtime_sql = row_confsql.getString(2).replace("{var1}", "'"+ count_date +"'")
      printf("\n runtime_sql: %s ", runtime_sql)

      try {
        val df_count = spark.sql(runtime_sql)

        if (!df_count.head(1).isEmpty){
          val ANYbda_count = df_count.collect.toList(0).get(0)
          if (ANYbda_count != null)
            bda_count = ANYbda_count.toString.toLong

        }
        import spark.implicits._
        var bda_count_df= Seq((1,1,1,1,1,1,"application_id "+application_id, table_id, table_name, count_date,  bda_count, runtime_sql)).toDF("gdg_position", "gdg_txoppos", "gdg_txind", "gdg_opcode", "gdg_timestamp", "gdg_schema", "gdg_table", "id", "table_name", "count_date", "bda_count", "runtime_sql")

        bda_count_df.createOrReplaceTempView("vw_bda_count_df")

        query_rerun = """
        WITH vaprun as (
         select table_name, target_date, record_count  as vap_count
         from
         (
             select upper(table_name) table_name, target_date, record_count, ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY extract_date desc, inserted_date desc) rn
             from """+schema+""".vap_to_bda_data_validation
             where extract_date between '"""+count_date+"""' and to_date(current_date())
             and table_name = '"""+ table_name +"""'
             and target_date = '"""+ count_date +"""'
         ) a where a.rn  = 1 )
         Select CAST(t1.gdg_position AS bigint),
              CAST(t1.gdg_txoppos AS bigint),
              CAST(t1.gdg_txind AS string),
              CAST(t1.gdg_opcode AS string),
              CAST(t1.gdg_timestamp AS string),
              CAST(t1.gdg_schema AS string),
              CAST(t1.gdg_table AS string),
              CAST(t1.id AS bigint),
              CAST(t1.table_name AS string),
              CAST(t1.count_date AS string),
              CAST(t1.bda_count AS bigint),
              CAST(t2.vap_count AS bigint),
              CASE WHEN (NVL(t1.bda_count,0)==NVL(t2.vap_count,0)) THEN 'Y' ELSE 'N' END AS matched,
              CAST(NVL(t1.bda_count,0) - NVL(t2.vap_count,0) AS bigint) AS count_diff,
              CAST('"""+date_column_name+"""' AS STRING) as date_column_name,
              CAST (now() AS STRING) AS  inserted_date,
              CAST(t1.runtime_sql AS string)
         FROM vw_bda_count_df t1
         LEFT JOIN vaprun t2
         ON t1.table_name = t2.table_name""".stripMargin

        printf("query_rerun: %s\n",query_rerun)
        val vap_vs_bda_count_df = spark.sql(query_rerun)
        vap_vs_bda_count_df.write.mode("append").parquet(target_path+"extract_date="+run_date_formatted)
        refreshTable()

        val end_time = new Timestamp(System.currentTimeMillis()).toString
        printf("BdaVapValidation::job for table %s is completed at %s", table_name, end_time)


      } catch {
        case e: Throwable =>
          println(e)
          println("Errors happended at try block 1 for "+ application_id)
          save_failed_data()
          refreshTable()}
    }
    catch {
      case e: Throwable =>
        println(e)
        println("Errors happended at try block 2 for "+ application_id)
        save_failed_data()
        refreshTable() }
    finally {
      spark.stop()
    }


    def save_failed_data(): Unit = {
      println("Start to save failed record into target bda_data_counts_validation")
      import spark.implicits._
      var bda_count_failed_df= Seq((1,1,1,1,1,1,"application_id "+application_id, table_id, table_name, count_date,  null , runtime_sql+" has failed")).toDF("gdg_position", "gdg_txoppos", "gdg_txind", "gdg_opcode", "gdg_timestamp", "gdg_schema", "gdg_table", "id", "table_name", "count_date", "bda_count", "runtime_sql")
      bda_count_failed_df.createOrReplaceTempView("vw_bda_count_df")
      printf("query_rerun: %s",query_rerun)
      val vap_vs_bda_count_failed_df = spark.sql(query_rerun)
      vap_vs_bda_count_failed_df.write.mode("append").parquet(target_path+"extract_date="+run_date_formatted)

      println("Save failed-record into target bda_data_counts_validation is completed.")
    }

    def refreshTable():Unit = {

      println("Refresh target table bda_data_counts_validation")
      val addPartitionStatmt = "ALTER TABLE "+schema+".bda_data_counts_validation add if not exists partition(extract_date='"+run_date_formatted.toString+"')"
      spark.sql(addPartitionStatmt)
      spark.sql("REFRESH TABLE "+schema+".bda_data_counts_validation")


    }


  }

}