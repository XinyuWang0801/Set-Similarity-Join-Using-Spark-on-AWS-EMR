package comp9313.proj3

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import java.util.function.ToLongFunction
//import scala.reflect.classTag



class SecondarySortKey(val second:Double, val first:Int) extends Ordered[SecondarySortKey] with Serializable
{
  def compare(there:SecondarySortKey):Int =
  {
  if(this.first - there.first != 0)
  {
  this.first.compareTo(there.first)
  }
  else
  {
  this.second.compareTo(there.second)
  }
  }
}



object SetSimJoin{
 
  def roundUp(d: Double) = Math.ceil(d).toInt
  def make_myarray(a: RDD[(String, (String, Array[String]))]) = a.groupByKey().map( x => {(x._1,x._2.toArray)})

  def main(args: Array[String]) {

  val inputFile_1 = args(0)
  val inputFile_2 = args(1)
  val outputFolder = args(2)
  val threshold = args(3).toDouble

  val conf = new SparkConf().setAppName("SetSimJoin")
  //create a scala spark context
  val sc = new SparkContext(conf)
  val lines_1 = sc.textFile(inputFile_1).flatMap(line => line.split("\n"))
  val lines_2 = sc.textFile(inputFile_2).flatMap(line => line.split("\n"))


  //tokens_2.foreach(println)    
    
    
  val Line1_split = lines_1.map { line => line.split(" ") }
  val array_1 = Line1_split.flatMap { x => {
                val pref_length = (x.drop(1).length + 1 - roundUp(x.drop(1).length*threshold))
                for(i <- 0 to pref_length-1)
                    yield(x.drop(1)(i), (x(0), x.drop(1)))
                }
              }


  val Line2_split = lines_2.map { line => line.split(" ") }
  val array_2 = Line2_split.flatMap { x => {
                val pref_length = (x.drop(1).length + 1 - roundUp(x.drop(1).length*threshold))
                for(i <- 0 to pref_length-1)
                    yield(x.drop(1)(i), ("-"+x(0), x.drop(1)))
                }
              }


  val tittle_array_1 = make_myarray(array_1)
  val tittle_array_2 = make_myarray(array_2)                                                                   
  // merge two sortedline to one RDD , and then do 2 for loops to compare one to another in the same sets.
  val merged = tittle_array_1.union(tittle_array_2).map(x=>(x._1,x._2)).reduceByKey(_++_)
  //val merged = sc.makeRDD(sortedLines1_1,sortedLines1_2)

  val sorted_res = merged
        .flatMap(x => {
        // (String, Array[(String, Array[String])])
        // all array with same title are mapped together.
        // use for loop to loop through items in same title.
        var hash_map: Map[(String, String),Double] = Map()
        var length = x._2.size - 1
          for(i <- 0 to length)
          {//first for loop
            for(j <- i to length)
            {//second for loop
                // avoid duplicate self-join
                // recognize sortedLines1_2
              if( x._2(j)._1.startsWith("-")
                  && !x._2(i)._1.startsWith("-")
                  && i!=j )
              {
              // ready to compute
                //print(x._2(j)._1.charAt(0))
                //print(x._2(i)._1)
                //print(x._2(j)._1)
                //print("\n")
                val str1 = x._2(i)._1
                val str2 = x._2(j)._1.substring(1)  // cut the "-" before the title
                val first_set = x._2(i)._2.toSet
                val second_set = x._2(j)._2.toSet
                val Common = (first_set).intersect(second_set).size.toDouble
                val Sum = first_set.toSet.union(second_set).size.toDouble
                val score = Common/Sum
                if(score >= threshold)
                {
                  if(!hash_map.contains((str1,str2)) )      // check if already exists in map
                  {
                      hash_map += ( (str1,str2) -> score )      // if not add to map
                  }}}}}
                  hash_map})

 
   //sortedLines2.foreach(println)  
   //val result = BigDecimal(x._2._2).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
                 
//  val od = new Ordering[((String,String),Double)] {
//  def compare(here:((String,String),Double), there: ((String,String),Double)): Int = {
//    var result = 0
//    if ((here._1._1 compare there._1._1) != 0) {  // first
//      result = here._1._1 compare there._1._1
//    }else {
//      result = here._1._2 compare there._1._2     // second
//    }
//    result
//  }
//  }
//  
//  val sorted = sorted_res.sortBy(x=>x,true,1)(od,classTag[((String,String),Double)])
//  //sorted.foreach(println)
 
  sorted_res
  .distinct()     // still need to remove duplicated ones
                  // different items may have identical similarities.
    .map(line =>
        (new SecondarySortKey(line._1._2.toDouble, line._1._1.toInt),line))
            .sortByKey()
                .map(x =>x._2._1+"\t"+BigDecimal(x._2._2).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble)
                    .saveAsTextFile(outputFolder)
  }
}

//finished






















