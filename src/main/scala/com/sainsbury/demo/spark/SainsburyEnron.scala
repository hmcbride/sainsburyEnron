package com.sainsbury.demo.spark


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.xml.XML


object SainsburyEnron {


  def main(args: Array[String]) {

    val spark = SparkSession.builder.master("local[*]").appName("Enron Test").getOrCreate()

    val sc = spark.sparkContext
    val sparkConf = spark.conf
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryoserializer.buffer.mb", "24")

    val emailxmls = "/Users/hughmcbride/Downloads/cloudguru/zxmls"
    val emailtexts = "/Users/hughmcbride/Downloads/cloudguru/ztexts"

    val emailFiles = sc.wholeTextFiles(emailtexts)

    val counts = emailFiles.map(line => ((line._2.split(" ")).length,1))  // Split email by words and create tuple of (wordcount , 1)
    val total_files  = counts.map(_._2).reduce((x, y) => x + y)    // sum up the total number of files
    val total_words = counts.map(_._1).reduce((x, y) => x + y)     // sump up the total number of words

    val average_email = total_words.toDouble / total_files.toDouble

    System.out.println("the average file size is "+average_email)

    val emailXMLs = sc.wholeTextFiles(emailxmls)

    val toEmails =  getEmailList("#To",1,emailXMLs)
    val ccEmails =  getEmailList("#CC",0.5,emailXMLs)

    val allEmails = toEmails.join(ccEmails)                         // merge the emails
                            .map(y => (y._1, y._2._1 + y._2._2 ))   // aggregate the counts of each
                            .sortBy(x => x._2)                      // sort emails by count

    allEmails.foreach(println)

    sc.stop()

    println("stop")
  }


   def getEmailList(addressee: String, weight: Double, emailXMLs: RDD[(String, String)]) = {

    emailXMLs.map(x => getAddressees(x._2, addressee))
             .map(y => y.split(";")) // Split the email list back up again
             .flatMap(z => z)        // Identity mapping to merge arrays
             .map(r => (r, weight))  // Weight the email
             .reduceByKey(_ + _)     // reduce emails to get total counts for each email
             .sortBy(s => s._2)      // sort emails by count
  }

  def getAddressees(thefile :String, addressee:String): String = {

    val xml = XML.loadString(thefile)                                 // parse the xml string

    val tagXML = xml \\ "Tag"                                             // Xpath to Tag
    val r = """(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}\b""".r    // Regex to match emails of type  name@company.com

    val toValues = tagXML.withFilter( x => x.attributes("TagName").text == addressee)    // Match the To or Cc Tag
                         .map(x => x.attributes("TagValue").text)                        // extract the Tag value attribute i.e email address
                         .toList
                         .flatMap{ r.findAllIn _ }.toList                                // filter emails of type name@company.com

    toValues.map(x => x.toLowerCase()).mkString(";")          // convert all emails to lowercase  ( incresese aggreagation ) and turn into string
                                                              // This is a hack / workaround

  }

}

