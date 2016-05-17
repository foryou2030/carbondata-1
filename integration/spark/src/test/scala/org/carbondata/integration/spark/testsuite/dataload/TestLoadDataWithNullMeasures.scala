package org.carbondata.integration.spark.testsuite.dataload

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Created by Administrator on 2016/5/17 0017.
 */
class TestLoadDataWithNullMeasures extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("CREATE CUBE carboncube DIMENSIONS (empno Integer, empname String, designation String, doj String, workgroupcategory Integer, workgroupcategoryname String, deptno Integer, deptname String, projectcode Integer, projectjoindate String, projectenddate String) MEASURES (attendance Integer,utilization Integer,salary Integer) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
  }
  test("test carbon table data loading when there are null measures in data") {
    try {
      sql("LOAD DATA FACT FROM './src/test/resources/datawithnullmsrs.csv' INTO CUBE carboncube OPTIONS(DELIMITER ',')");
    } catch {
      case e : Throwable => e.printStackTrace()
    }
  }

  override def afterAll {
    sql("drop cube carboncube")
  }
}
