package com.atguigu.structure.streaming.day02.duplicate

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Author lzc
  * Date 2019-09-25 11:38
  */
object DropDumplicate1 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Test")
            .getOrCreate()
        import spark.implicits._
    
        val lines: DataFrame = spark.readStream
            .format("socket")
            .option("host", "hadoop201")
            .option("port", 9999)
            .load()
    
        val words: DataFrame = lines.as[String].map(line => {
            val arr: Array[String] = line.split(",")
            (arr(0), Timestamp.valueOf(arr(1)), arr(2))
        }).toDF("uid", "ts", "word")
    
        val wordCounts: Dataset[Row] = words
            .withWatermark("ts", "2 minutes")
            .dropDuplicates("uid")  // 去重重复数据 uid 相同就是重复.  可以传递多个列
    
        wordCounts.writeStream
            .outputMode("append")
            .format("console")
            .start
            .awaitTermination()
    }
}
