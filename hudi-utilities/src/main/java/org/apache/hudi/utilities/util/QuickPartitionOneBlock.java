package org.apache.hudi.utilities.util;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.SqlQueryBuilder;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author yihao
 * @version 1.0
 * @since 2024/1/8 9:32
 */
public class QuickPartitionOneBlock<K extends Comparable> extends PartitionOneBlock<K> {

  private static final Logger LOG = LoggerFactory.getLogger(QuickPartitionOneBlock.class);

  public QuickPartitionOneBlock(TypedProperties props, DataFrameReader dataFrameReader,
                                String filter, String jdbcUrlSchema, SqlType.SqlTypes partitionColumnType,
                                String tableName, long fetchSize, String partitionColumn) {
    super(props, dataFrameReader, filter, jdbcUrlSchema, partitionColumnType, tableName, fetchSize,  partitionColumn);
  }

  @Override
  protected String[] calculateRanges() {
    K maxPartitionVal  =  getMaxOrMinPartitionColumn(true);
    K minPartitionVal = getMaxOrMinPartitionColumn(false);
    Dataset<Row> dataset = dataFrameReader.option("dbtable", String.format(ppdQuery, getFetchSQLFormat())).load();
    long countNum = dataSetNumToDataType(dataset, DataTypes.LongType).getLong(0);
    long newFetchSize = countNum / fetchSize < 1 ? countNum : fetchSize;
    long numPartitions = countNum == newFetchSize ? 1 : (long) Math.ceil((double) countNum / newFetchSize);
    LOG.info("quick split mode lowerBound:{}, upperBound:{}, numPartitions:{}", minPartitionVal, maxPartitionVal, numPartitions);
    setFetchSizeRanges(maxPartitionVal.toString(), minPartitionVal.toString(), Long.toString(numPartitions));
    setParallelism(numPartitions);
    return null;
  }

  @Override
  protected String getFetchSQLFormat() {
    return SqlQueryBuilder.select("count(1) as " + NUMBER_VALUE_ALIAS).from(tableName).toString();
  }
}
