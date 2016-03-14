package org.saurabh.tweets

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils



object App {

  def main(args: Array[String]){
    
    if (args.length < 4) {
      System.err.println("Usage: TwitterHashTagJoinSentiments <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }
    
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by Twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
//    System.setProperties("",)
    
    
    val sparkConf = new SparkConf().setAppName("TwitterStreaming").setMaster("local[2]")
    
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    
// Creating RDD of Stopwords
    
    val stopWordsFile = ssc.sparkContext.textFile("stopwords.txt", 4)  // getting a list of stop words
    val stopwords = stopWordsFile.flatMap { stopWordsFile => stopWordsFile.split(" ") }
    val stopWordSet = stopwords.collect().toSet
    val stopWordSetBC = ssc.sparkContext.broadcast(stopWordSet)
    
    
// 1. Getting Data From Twitter  
    val stream = TwitterUtils.createStream(ssc, None, filters)
// 2. Enriching Geolocation {Filtering tweets not containing geo location}    
    val validGeoLocation = stream.filter(status => status.getGeoLocation() != null)
    val geoLocation = validGeoLocation.map(x => x.getLang.toString())
    
    val statuses = stream.map(status => status.getText().toLowerCase())
    val wordsAndNumbers = statuses.flatMap { statuses => statuses.split(" ") }
    val words = wordsAndNumbers.filter { x => x.matches("^[a-zA-Z_]*$") }
// 3. Removing Stop Words    
    val cleanWords = words.mapPartitions{iter =>
                        val stopWordSet = stopWordSetBC.value
                        iter.filter(word => !stopWordSet.contains(word))
                        }
// 4. Counting Words   
    val wordAsCount = cleanWords.map { cleanWords => (cleanWords,1) }
    val wordcount = wordAsCount.reduceByKey(_ + _)
    
    
    
    
    geoLocation.print()
    
    wordcount.print()
    
    
    ssc.start()
    ssc.awaitTermination()
    
  }
  

}