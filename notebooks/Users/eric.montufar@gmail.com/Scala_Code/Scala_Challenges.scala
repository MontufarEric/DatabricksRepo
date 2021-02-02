// Databricks notebook source
// MAGIC %md 
// MAGIC # Code challenges with scala

// COMMAND ----------

// MAGIC %md 
// MAGIC Here I will add some scala snipets used to solve code challenges

// COMMAND ----------

def restock(itemcount: Seq[Int], target: Int): Int = {
    var cont = 0
    for (e <- itemcount){
        cont += e
        if(cont >= target)
          return math.abs(target- cont)
    }
    return math.abs(target - cont)  
}
    

// COMMAND ----------



// COMMAND ----------

