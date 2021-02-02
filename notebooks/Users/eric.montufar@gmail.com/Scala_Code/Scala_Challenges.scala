// Databricks notebook source
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

