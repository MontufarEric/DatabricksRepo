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

import scala.collection.mutable.ListBuffer

def comparatorValue(a: Seq[Int], b: Seq[Int], d: Int ){
    var c = 0
    for (i <- a){
        var f1 = new ListBuffer[Boolean]()
        for (j <- b){
             if(math.abs(j-i) > d) f1 += true else f1 += false
        }
        if (f1.forall(x => {x == true})) c+=1 else c+=0 
    }
    return c
    
}

// COMMAND ----------

