// Databricks notebook source
// MAGIC %sql
// MAGIC drop table if exists dev_db.file_process_log;
// MAGIC create table dev_db.file_process_log
// MAGIC (
// MAGIC file_id                 string     comment "none",
// MAGIC filename                string     comment "none",
// MAGIC processing_timestamp    timestamp  comment "none",
// MAGIC processing_status       string     comment "none"
// MAGIC )
// MAGIC using delta
// MAGIC options(path="s3a://filestoragedatabricks/parquet_files/tables2/file_process_log")

// COMMAND ----------

val p1 = spark.read
    .format("parquet")
    .load("s3a://filestoragedatabricks/parquet_files/SapOpenHub_ZOH_EKPO_480560_1.parquet")

// COMMAND ----------

p1.write.partitionBy("OHREQUID").format("delta").save("s3a://filestoragedatabricks/parquet_files/tables2/master_table")

// COMMAND ----------

spark.sql("""INSERT INTO dev_db.file_process_log VALUES ("480560", "SapOpenHub_ZOH_EKPO_480560_1.parquet","Null","Null")""")

// COMMAND ----------

val log_df = spark.sql("""SELECT * FROM dev_db.file_process_log""")
val file_id_list = log_df.select("filename").collect().map(_(0)).toList

// COMMAND ----------

file_id_list

// COMMAND ----------

import scala.collection.mutable.ListBuffer 
  
val files_to_process = ListBuffer[String]()
val files_in_s3 = dbutils.fs.ls("s3a://filestoragedatabricks/parquet_files/")
files_in_s3.foreach(e => {
                            if(e.path.endsWith("parquet"))
                              if(file_id_list.contains(e.path.toString.split("/").last) == false){
                                files_to_process +=e.path.toString.split("/").last
                              }
                                 })

// COMMAND ----------

files_to_process

// COMMAND ----------

val  delta_df = if(files_to_process.isEmpty == false) spark.read.format("parquet").load("s3a://filestoragedatabricks/parquet_files/" + files_to_process.last) else null
if(files_to_process.init.isEmpty == false){
    files_to_process.init.foreach(file => delta_df.union(spark.read.format("parquet").load("s3a://filestoragedatabricks/parquet_files/" +file)))
  }
files_to_process.foreach(file => spark.sql("""INSERT INTO dev_db.file_process_log VALUES (%s, %s,"Null","Null")""".format(file.split("_").init.last,file.split("\\.").init.last)))

// COMMAND ----------

files_to_process.foreach(file => print(file.split("_").init.last,file))

// COMMAND ----------

val file = "SapOpenHub_ZOH_EKPO_481743_1.parquet"
spark.sql("""INSERT INTO dev_db.file_process_log  VALUES (%s, %s,"Null","Null")""".format(1,3))

// COMMAND ----------

(file_id, filename, processing_timestamp, processing_status)  

// COMMAND ----------

spark.sql("""INSERT INTO dev_db.file_process_log VALUES (%s, %s ,Null,Null)""".format(file.split("_").init.last.toString,file.toString))

// COMMAND ----------

files_to_process.foreach(file => spark.sql("""INSERT INTO dev_db.file_process_log (file_id, filename, processing_timestamp, processing_status) VALUES (%s, %s ,Null,Null)""".format(file.split("_").init.last,file)))

// COMMAND ----------

spark.sql("""INSERT INTO dev_db.file_process_log VALUES (%s, %s, "Null", "Null")""".format(file.split("_").init.last,file))

// COMMAND ----------

// val file = "SapOpenHub_ZOH_EKPO_481743_1.parquet"
print(file.split("\\.").init.last)

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from dev_db.file_process_log

// COMMAND ----------

files_to_process.foreach(file => println(file.split("_").init.last))

// COMMAND ----------

files_to_process

// COMMAND ----------

delta_df.show()

// COMMAND ----------

// fix if to return val 
if(files_to_process.isEmpty == false){
  val   delta_df = spark.read.format("parquet").load("s3a://filestoragedatabricks/parquet_files/" + files_to_process.last)
  
}

// COMMAND ----------

val   delta_df = spark.read
       .format("parquet")
       .load("s3a://filestoragedatabricks/parquet_files/" + files_to_process.last)

// COMMAND ----------

delta_df.write.partitionBy("OHREQUID").format("delta").save("s3a://filestoragedatabricks/parquet_files/tables2/delta_table")

// COMMAND ----------

p1.createOrReplaceTempView("data1")

// COMMAND ----------

val agg_df1 = spark.sql(""" 
SELECT * FROM
(
SELECT
      `SOURSYSTEM`
      ,`DOC_NUM`
      ,`DOC_ITEM`
      ,`RES_DEL`
      ,`PO_STATUS`
      ,`PROD_DESCR`
      ,`MATERIAL`
      ,`COMP_CODE`
      ,`PLANT`
      ,`STOR_LOC`
      ,`MATL_GROUP`
      ,`INFO_REC`
      ,`AB_VENDMAT`
      ,SUM(`TARG_QTY`) AS `TARG_QTY`
      ,SUM(`ORDER_QUAN`) AS `ORDER_QUAN`
      ,`BASE_UOM`
      ,`PO_PR_UNIT`
      ,SUM(`NUMERA_UC`) AS `NUMERA_UC`
      ,SUM(`DENOMI_UC`) AS `DENOMI_UC`
      ,SUM(`NUMERATOR`) AS `NUMERATOR`
      ,SUM(`DENOMINTR`) AS `DENOMINTR`
      ,SUM(`NETVAL_INV`) AS `NETVAL_INV`
      ,`PO_UNIT`
      ,`DOC_CURRCY`
      ,`/BIC/ZBEDNR`
      ,SUM(`BBP_PRCUNT`) AS `BBP_PRCUNT`
      ,SUM(`NETPRICE`) AS `NETPRICE`
      ,SUM(`GROSS_VAL`) AS `GROSS_VAL`
      ,`LOC_CURRCY`
      ,SUM(`UPPR_BND`) AS `UPPR_BND`
      ,`BND_IND`
      ,SUM(`LOWR_BND`) AS `LOWR_BND`
      ,`COMPL_DEL`
      ,`ITM_CAT`
      ,`GT_KNTTPMM`
      ,`GR_RE_IND`
      ,`CONTRACT`
      ,`INV_RE_IND`
      ,`DATETO`
      ,`DOC_CAT`
      ,SUM(`PUR_EFFWR`) AS  `PUR_EFFWR`
      ,`ORDER_CURR`
      ,`/BIC/ZPACKNO`
      ,`/BIC/ZANFNR`
      ,`/BIC/ZANFPS`
      ,`DF_GPRQHDR`
      ,`REQUESTER`
      ,`DF_PRIOURG`
      ,`/BIC/ZPRIO_REQ`
      ,`/BIC/ZSRMCOID`
      ,`/BIC/ZPRSDR`
      ,`/BIC/ZPROCSTA`
      ,`/BIC/ZFRGGR`
      ,`/BIC/ZKFRGSX`
      ,`ME_CALSCEM`
      ,`/BIC/ZKNUMV`
      ,`VAL_START`
      ,`VAL_END`
      ,`CALYEAR`
      ,`PUR_GROUP`
      ,`PURCH_ORG`
      ,`GT_ZTERM`
      ,`VENDOR`
      ,`CREATEDBY`
      ,`CREATEDON`
      ,`DOC_TYPE`
      ,SUM(`EXCHG_RATE`) AS `EXCHG_RATE`
  FROM data1
  GROUP BY  
      `SOURSYSTEM`
      ,`DOC_NUM`
      ,`DOC_ITEM`
      ,`RES_DEL`
      ,`PO_STATUS`
      ,`PROD_DESCR`
      ,`MATERIAL`
      ,`COMP_CODE`
      ,`PLANT`
      ,`STOR_LOC`
      ,`MATL_GROUP`
      ,`INFO_REC`
      ,`AB_VENDMAT`
      ,`BASE_UOM`
      ,`PO_PR_UNIT`
      ,`PO_UNIT`
      ,`DOC_CURRCY`
      ,`/BIC/ZBEDNR`
      ,`LOC_CURRCY`
      ,`BND_IND`
      ,`COMPL_DEL`
      ,`ITM_CAT`
      ,`GT_KNTTPMM`
      ,`GR_RE_IND`
      ,`CONTRACT`
      ,`INV_RE_IND`
      ,`DATETO`
      ,`DOC_CAT`
      ,`ORDER_CURR`
      ,`/BIC/ZPACKNO`
      ,`/BIC/ZANFNR`
      ,`/BIC/ZANFPS`
      ,`DF_GPRQHDR`
      ,`REQUESTER`
      ,`DF_PRIOURG`
      ,`/BIC/ZPRIO_REQ`
      ,`/BIC/ZSRMCOID`
      ,`/BIC/ZPRSDR`
      ,`/BIC/ZPROCSTA`
      ,`/BIC/ZFRGGR`
      ,`/BIC/ZKFRGSX`
      ,`ME_CALSCEM`
      ,`/BIC/ZKNUMV`
      ,`VAL_START`
      ,`VAL_END`
      ,`CALYEAR`
      ,`PUR_GROUP`
      ,`PURCH_ORG`
      ,`GT_ZTERM`
      ,`VENDOR`
      ,`CREATEDBY`
      ,`CREATEDON`
      ,`DOC_TYPE`
         ) TMP_EKPO
WHERE (
      `TARG_QTY`
      +`ORDER_QUAN`
      +`NUMERA_UC`
      +`DENOMI_UC`
      +`NUMERATOR`
      +`DENOMINTR`
      +`NETVAL_INV`
      +`BBP_PRCUNT`
      +`NETPRICE`
      +`GROSS_VAL`
      +`UPPR_BND`
      +`LOWR_BND`
      +`PUR_EFFWR`
      +`EXCHG_RATE`
         )<>0""")
agg_df1.write.format("delta").save("s3a://filestoragedatabricks/parquet_files/tables2/agg_table")
agg_df1.createOrReplaceTempView("agg_data1")

// COMMAND ----------

// MAGIC %sql 
// MAGIC SELECT * FROM agg_data1 limit 100

// COMMAND ----------

delta_df.createOrReplaceTempView("delta1")

// COMMAND ----------

val p2 = p1.union(delta_df)
p2.createOrReplaceTempView("data2")

// COMMAND ----------

val agg_df2 = spark.sql(""" 
SELECT * FROM
(
SELECT
      `SOURSYSTEM`
      ,`DOC_NUM`
      ,`DOC_ITEM`
      ,`RES_DEL`
      ,`PO_STATUS`
      ,`PROD_DESCR`
      ,`MATERIAL`
      ,`COMP_CODE`
      ,`PLANT`
      ,`STOR_LOC`
      ,`MATL_GROUP`
      ,`INFO_REC`
      ,`AB_VENDMAT`
      ,SUM(`TARG_QTY`) AS `TARG_QTY`
      ,SUM(`ORDER_QUAN`) AS `ORDER_QUAN`
      ,`BASE_UOM`
      ,`PO_PR_UNIT`
      ,SUM(`NUMERA_UC`) AS `NUMERA_UC`
      ,SUM(`DENOMI_UC`) AS `DENOMI_UC`
      ,SUM(`NUMERATOR`) AS `NUMERATOR`
      ,SUM(`DENOMINTR`) AS `DENOMINTR`
      ,SUM(`NETVAL_INV`) AS `NETVAL_INV`
      ,`PO_UNIT`
      ,`DOC_CURRCY`
      ,`/BIC/ZBEDNR`
      ,SUM(`BBP_PRCUNT`) AS `BBP_PRCUNT`
      ,SUM(`NETPRICE`) AS `NETPRICE`
      ,SUM(`GROSS_VAL`) AS `GROSS_VAL`
      ,`LOC_CURRCY`
      ,SUM(`UPPR_BND`) AS `UPPR_BND`
      ,`BND_IND`
      ,SUM(`LOWR_BND`) AS `LOWR_BND`
      ,`COMPL_DEL`
      ,`ITM_CAT`
      ,`GT_KNTTPMM`
      ,`GR_RE_IND`
      ,`CONTRACT`
      ,`INV_RE_IND`
      ,`DATETO`
      ,`DOC_CAT`
      ,SUM(`PUR_EFFWR`) AS  `PUR_EFFWR`
      ,`ORDER_CURR`
      ,`/BIC/ZPACKNO`
      ,`/BIC/ZANFNR`
      ,`/BIC/ZANFPS`
      ,`DF_GPRQHDR`
      ,`REQUESTER`
      ,`DF_PRIOURG`
      ,`/BIC/ZPRIO_REQ`
      ,`/BIC/ZSRMCOID`
      ,`/BIC/ZPRSDR`
      ,`/BIC/ZPROCSTA`
      ,`/BIC/ZFRGGR`
      ,`/BIC/ZKFRGSX`
      ,`ME_CALSCEM`
      ,`/BIC/ZKNUMV`
      ,`VAL_START`
      ,`VAL_END`
      ,`CALYEAR`
      ,`PUR_GROUP`
      ,`PURCH_ORG`
      ,`GT_ZTERM`
      ,`VENDOR`
      ,`CREATEDBY`
      ,`CREATEDON`
      ,`DOC_TYPE`
      ,SUM(`EXCHG_RATE`) AS `EXCHG_RATE`
  FROM data2
  GROUP BY  
      `SOURSYSTEM`
      ,`DOC_NUM`
      ,`DOC_ITEM`
      ,`RES_DEL`
      ,`PO_STATUS`
      ,`PROD_DESCR`
      ,`MATERIAL`
      ,`COMP_CODE`
      ,`PLANT`
      ,`STOR_LOC`
      ,`MATL_GROUP`
      ,`INFO_REC`
      ,`AB_VENDMAT`
      ,`BASE_UOM`
      ,`PO_PR_UNIT`
      ,`PO_UNIT`
      ,`DOC_CURRCY`
      ,`/BIC/ZBEDNR`
      ,`LOC_CURRCY`
      ,`BND_IND`
      ,`COMPL_DEL`
      ,`ITM_CAT`
      ,`GT_KNTTPMM`
      ,`GR_RE_IND`
      ,`CONTRACT`
      ,`INV_RE_IND`
      ,`DATETO`
      ,`DOC_CAT`
      ,`ORDER_CURR`
      ,`/BIC/ZPACKNO`
      ,`/BIC/ZANFNR`
      ,`/BIC/ZANFPS`
      ,`DF_GPRQHDR`
      ,`REQUESTER`
      ,`DF_PRIOURG`
      ,`/BIC/ZPRIO_REQ`
      ,`/BIC/ZSRMCOID`
      ,`/BIC/ZPRSDR`
      ,`/BIC/ZPROCSTA`
      ,`/BIC/ZFRGGR`
      ,`/BIC/ZKFRGSX`
      ,`ME_CALSCEM`
      ,`/BIC/ZKNUMV`
      ,`VAL_START`
      ,`VAL_END`
      ,`CALYEAR`
      ,`PUR_GROUP`
      ,`PURCH_ORG`
      ,`GT_ZTERM`
      ,`VENDOR`
      ,`CREATEDBY`
      ,`CREATEDON`
      ,`DOC_TYPE`
         ) TMP_EKPO
WHERE (
      `TARG_QTY`
      +`ORDER_QUAN`
      +`NUMERA_UC`
      +`DENOMI_UC`
      +`NUMERATOR`
      +`DENOMINTR`
      +`NETVAL_INV`
      +`BBP_PRCUNT`
      +`NETPRICE`
      +`GROSS_VAL`
      +`UPPR_BND`
      +`LOWR_BND`
      +`PUR_EFFWR`
      +`EXCHG_RATE`
         )<>0""")

agg_df2.createOrReplaceTempView("agg_data2")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM agg_data2 limit 100

// COMMAND ----------


agg_df2.write
  .format("delta")
  .mode("overwrite")
  .save("s3a://filestoragedatabricks/parquet_files/tables2/agg_table")

// COMMAND ----------

p1
  .as("data1")
  .merge(
    delta_df.as("delta1"),
    "logs.OHREQUID = updates.OHREQUID")
  .whenNotMatched()
  .insertAll()
  .execute()

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM dev_db.file_process_log

// COMMAND ----------

