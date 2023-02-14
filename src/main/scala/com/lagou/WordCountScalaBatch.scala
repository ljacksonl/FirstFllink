package com.lagou

import org.apache.flink.api.scala._

object WordCountScalaBatch {
  def main(args: Array[String]): Unit = {
    val inputPath = "D:\\core\\FirstFilnk\\data\\input\\hello.txt"
    val outputPath = "D:\\core\\FirstFilnk\\data\\output"
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val text: DataSet[String] = environment.readTextFile(inputPath)
    val out: AggregateDataSet[(String, Int)] = text.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
    out.writeAsCsv(outputPath,"\n"," ").setParallelism(1)
    environment.execute("scala batch process")
  }
}
