/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.integration.spark.util

import java.io.File

import org.apache.spark.sql.common.util.CarbonHiveContext.sql
import org.apache.spark.sql.common.util.{CarbonHiveContext, QueryTest}
import org.apache.spark.sql.{CarbonEnv, CarbonRelation}
import org.carbondata.core.carbon.CarbonDataLoadSchema
import org.carbondata.spark.load.CarbonLoadModel
import org.carbondata.spark.util.GlobalDictionaryUtil
import org.scalatest.BeforeAndAfterAll

/**
  * Test Case for org.carbondata.integration.spark.util.GlobalDictionaryUtil
  *
  * @date: Apr 10, 2016 10:34:58 PM
  * @See org.carbondata.integration.spark.util.GlobalDictionaryUtil
  */
class StreamSmartLocalDictionaryTestCase extends QueryTest with BeforeAndAfterAll {

  var pwd: String = _
  var sampleRelation: CarbonRelation = _
  var complexRelation: CarbonRelation = _
  var sampleLocalDictionaryFile: String = _
  var complexLocalDictionaryFile: String = _

  def buildCarbonLoadModel(relation: CarbonRelation,
    filePath: String,
    dimensionFilePath: String,
    header: String,
    localDictFilePath: String): CarbonLoadModel = {
    val carbonLoadModel = new CarbonLoadModel
    carbonLoadModel.setTableName(relation.cubeMeta.carbonTableIdentifier.getDatabaseName)
    carbonLoadModel.setDatabaseName(relation.cubeMeta.carbonTableIdentifier.getTableName)
    // carbonLoadModel.setSchema(relation.cubeMeta.schema)
    val table = relation.cubeMeta.carbonTable
    val carbonSchema = new CarbonDataLoadSchema(table)
    carbonLoadModel.setDatabaseName(table.getDatabaseName)
    carbonLoadModel.setTableName(table.getFactTableName)
    carbonLoadModel.setCarbonDataLoadSchema(carbonSchema)
    carbonLoadModel.setFactFilePath(filePath)
    carbonLoadModel.setDimFolderPath(dimensionFilePath)
    carbonLoadModel.setCsvHeader(header)
    carbonLoadModel.setCsvDelimiter(",")
    carbonLoadModel.setComplexDelimiterLevel1("\\$")
    carbonLoadModel.setComplexDelimiterLevel2("\\:")
    carbonLoadModel.setLocalDictPath(localDictFilePath)
    carbonLoadModel.setDictFileExt(".dictionary")
    carbonLoadModel
  }

  override def beforeAll {
    sql("drop table if exists sample")
    sql("drop table if exists complextypes")
    buildTestData
    // second time comment this line
    buildTable
    buildRelation
  }

  def buildTestData() = {
    pwd = new File(this.getClass.getResource("/").getPath + "/../../").getCanonicalPath
    sampleLocalDictionaryFile = pwd + "/src/test/resources/localdictionary/sample/20160423/1400_1405/"
    complexLocalDictionaryFile = pwd + "/src/test/resources/localdictionary/complex/20160423/1400_1405/"
  }

  def buildTable() = {
    try {
      sql(
        "CREATE TABLE IF NOT EXISTS sample (id STRING, name STRING, city STRING, " +
          "age INT) STORED BY 'org.apache.carbondata.format'"
      )
    } catch {
      case ex: Throwable => logError(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
    try {
      sql(
        "create table complextypes (deviceInformationId INT, channelsId string, " +
          "ROMSize string, purchasedate string, mobile struct<imei: string, imsi: string>, MAC " +
          "array<string>, locationinfo array<struct<ActiveAreaId: INT, ActiveCountry: string, " +
          "ActiveProvince: string, Activecity: string, ActiveDistrict: string, ActiveStreet: " +
          "string>>, proddate struct<productionDate: string,activeDeactivedate: array<string>>, " +
          "gamePointId INT,contractNumber INT) STORED BY 'org.apache.carbondata.format'" +
          "TBLPROPERTIES('DICTIONARY_EXCLUDE'='ROMSize')"
      )
    } catch {
      case ex: Throwable => logError(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
  }

  def buildRelation() = {
    val catalog = CarbonEnv.getInstance(CarbonHiveContext).carbonCatalog
    sampleRelation = catalog.lookupRelation1(Option("default"), "sample", None)(CarbonHiveContext).asInstanceOf[CarbonRelation]
    complexRelation = catalog.lookupRelation1(Option("default"), "complextypes", None)(CarbonHiveContext).asInstanceOf[CarbonRelation]
  }

  test("Support generate global dictionary from streamSmart local dictionary") {
    val header = "id,name,city,age"
    val carbonLoadModel = buildCarbonLoadModel(sampleRelation, null, null, header, sampleLocalDictionaryFile)
    GlobalDictionaryUtil
      .generateGlobalDictionary(CarbonHiveContext,
        carbonLoadModel,
        sampleRelation.cubeMeta.storePath)
  }

  test("Support generate global dictionary from streamSmart local dictionary file for complex type") {
    val header = "deviceInformationId,channelsId,ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber"
    val carbonLoadModel = buildCarbonLoadModel(complexRelation, null, null, header, complexLocalDictionaryFile)
    GlobalDictionaryUtil
      .generateGlobalDictionary(CarbonHiveContext,
      carbonLoadModel,
      complexRelation.cubeMeta.storePath)
  }
  
  override def afterAll {
    sql("drop table if exists sample")
    sql("drop table if exists complextypes")
  }
}
