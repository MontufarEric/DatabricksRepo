// Databricks notebook source
// MAGIC %md 
// MAGIC # Code challenges with scala

// COMMAND ----------

// MAGIC %md 
// MAGIC Here I will add some scala snipets used to solve code challenges

// COMMAND ----------

def restock(itemcount: Seq[Int], target: Int): Int = {
    var count = 0
    for (e <- itemcount){
        count += e
        if(count >= target)
          return math.abs(target- count)
    }
    return math.abs(target - count)  
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
   println(c)
    
}

// COMMAND ----------

import scala.collection.mutable.ListBuffer

def comparatorValue1(a: Seq[Int], b: Seq[Int], d: Int ){
    var count = 0
    for (element <- a){
        var list_1 = new ListBuffer[Boolean]()
        b.foreach(e => if(math.abs(e - element) > d) list_1 += true else list_1 += false)
        if (list_1.exists(_ == false)) count+=0 else count+=1
    }
    println(count) 
}

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

