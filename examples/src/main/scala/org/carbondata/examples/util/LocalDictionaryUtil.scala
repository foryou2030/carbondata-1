/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.carbondata.examples.util

import java.io.DataOutputStream

import scala.collection.mutable.{ArrayBuffer, HashSet}

import org.apache.spark.SparkContext

import org.carbondata.core.datastorage.store.impl.FileFactory

object LocalDictionaryUtil {
  def extractDictionary(sc: SparkContext,
                        srcData: String,
                        outputPath: String,
                        fileHeader: String,
                        dictCol: String): Unit = {
    val dictionaryRdd = sc.textFile(srcData).flatMap(x => {
      val fileHeaderArr = fileHeader.split(",")
      val tokens = x.split(",")
      val result = new ArrayBuffer[(Int, String)]()
      for (i <- 0 until fileHeaderArr.length) {
        if (dictCol.contains("|" + fileHeaderArr(i) + "|")) {
          try {
            result += ((i, tokens(i)))
          } catch {
            case ex: ArrayIndexOutOfBoundsException =>
              // scalastyle:off println
              println("#########################")
              println(x)
              println("#########################")
              // scalastyle:on println
          }
        }
      }
      result
    }).groupByKey().flatMap(x => {
      val distinctValues = new HashSet[(Int, String)]()
      for (value <- x._2) {
        distinctValues.add(x._1, value)
      }
      distinctValues
    })
    val dictionaryValues = dictionaryRdd.map(x => x._1 + "," + x._2).collect()
    saveToFile(dictionaryValues, outputPath)
  }

  def cleanDictionary(outputPath: String): Unit = {
    try {
      val fileType = FileFactory.getFileType(outputPath)
      val file = FileFactory.getCarbonFile(outputPath, fileType)
      if (file.exists()) {
        file.delete()
      }
    } catch {
      case ex: Exception =>
        // scalastyle:off println
        println("#########################")
        println(ex.toString)
        println("#########################")
        // scalastyle:on println
    }
  }

  def saveToFile(contents: Array[String], outputPath: String): Unit = {
    var writer: DataOutputStream = null
    try {
      val fileType = FileFactory.getFileType(outputPath)
      val file = FileFactory.getCarbonFile(outputPath, fileType)
      if (!file.exists()) {
        file.createNewFile()
      }
      writer = FileFactory.getDataOutputStream(outputPath, fileType)
      for (content <- contents) {
        writer.writeBytes(content + "\n")
      }
    } catch {
      case ex: Exception =>
        // scalastyle:off println
        println("#########################")
        println(ex.toString)
        println("#########################")
        // scalastyle:on println
    } finally {
      if (writer != null) {
        writer.close()
      }
    }
  }
}
