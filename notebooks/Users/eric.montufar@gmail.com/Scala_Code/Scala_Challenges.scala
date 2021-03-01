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

// MAGIC %md 
// MAGIC ### Binary Search (Recursive)
// MAGIC Code for implmenting the Bunary Search Algorithm in Scala. This answers the question How would you return the position of an element without using the built-in methods in Scala. 

// COMMAND ----------

def RecursiveBinarySearch(arr: Array[Int], 
                          Element_to_Search: Int) 
                         (low: Int = 0, 
                          high: Int = arr.length - 1): Int = 
{ 
      
    // If element not found                                
    if (low > high)  
        return -1
      
    // Getting the middle element 
    var middle = low + (high - low) / 2
      
    // If element found 
    if (arr(middle) == Element_to_Search) 
        return middle 
      
    // Searching in the left half 
    else if (arr(middle) > Element_to_Search) 
        return RecursiveBinarySearch(arr,  
               Element_to_Search)(low, middle - 1) 
      
    // Searching in the right half 
    else
        return RecursiveBinarySearch(arr,  
               Element_to_Search)(middle + 1, high) 
} 
  
// Creating main function 
def main(args: Array[String]){ 
      
    // Calling the binary search function and 
    // storing its result in index variable 
    var index = RecursiveBinarySearch(Array(1, 2, 3, 4, 55,  
                                            65, 75), 4)(0, 6); 
      
    // If value not found  
    if(index == -1) 
       print("Not Found") 
          
    // Else print the index where  
    // the value is found 
    else
       print("Element found at Index " + index) 
} 

// COMMAND ----------



// COMMAND ----------

def IterativeBinarySearch(arr: Array[Int],  
                          Element_to_Search: Int): Int =
{ 
      
    // Creating start variable to 
    // point to the first value 
    var low = 0
      
    // Creating end variable to  
    // point to the last value 
    var high = arr.length - 1
      
    // Finding the value in the  
    // array iteratively 
    while (low <= high) 
    { 
          
        // Getting middle element     
        var middle = low + (high - low) / 2
          
        // If element found in the middle index 
        if (arr(middle) == Element_to_Search) 
            return middle 
          
        // Searching in the first half 
        else if (arr(middle) > Element_to_Search) 
            high = middle - 1
          
        // Searching in the second half   
        else
            low = middle + 1
    } 
      
    // If value not found in the  
    // entire array , return -1  
    -1
} 
  
// Creating main function 
def main(args: Array[String]) 
{ 
      
    // Calling the binary search function and 
    // storing its result in index variable 
    var index = IterativeBinarySearch(Array(1, 2, 3, 4, 55, 
                                            65, 75), 65); 
      
    // If value not found  
    if(index == -1) 
       print("Not Found") 
          
    // Else print the index where  
    // the value is found 
    else
       print("Element found at Index " + index) 
} 