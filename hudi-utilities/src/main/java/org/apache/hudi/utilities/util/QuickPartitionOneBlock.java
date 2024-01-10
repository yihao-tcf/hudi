package org.apache.hudi.utilities.util;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.SqlQueryBuilder;
import org.apache.spark.sql.DataFrameReader;


/**
 * @author yihao
 * @version 1.0
 * @since 2024/1/8 9:32
 */
public class QuickPartitionOneBlock<K extends Comparable> extends PartitionOneBlock<K> {

  public QuickPartitionOneBlock(TypedProperties props, DataFrameReader dataFrameReader,
                                String filter, String jdbcUrlSchema, SqlType.SqlTypes partitionColumnType,
                                String tableName, long fetchSize, String partitionColumn) {
    super(props, dataFrameReader, filter, jdbcUrlSchema, partitionColumnType, tableName, fetchSize,  partitionColumn);
  }

  @Override
  protected String[] calculateRanges() {
    K maxPartitionVal  =  getMaxOrMinPartitionColumn(true);
    K minPartitionVal = getMaxOrMinPartitionColumn(false);
    long countNum = dataFrameReader.option("dbtable", String.format(ppdQuery, getFetchSQLFormat())).load().first().getLong(0);
    long newFetchSize = countNum / fetchSize < 1 ? countNum : fetchSize;
    long numPartitions = countNum == newFetchSize ? 1 : (long) Math.ceil((double) countNum / newFetchSize);
    this.props.setProperty(JDBC_FETCH_SIZE_LOWER_BOUND_KEY, minPartitionVal.toString());
    this.props.setProperty(JDBC_FETCH_SIZE_UPPER_BOUND_KEY, maxPartitionVal.toString());
    this.props.setProperty(JDBC_FETCH_SIZE_NUM_PARTITIONS_KEY, Long.toString(numPartitions));
    setParallelism(numPartitions);
    return null;
  }

  @Override
  protected String getFetchSQLFormat() {
    return SqlQueryBuilder.select("count(1) as cnums").from(tableName).toString();
  }
}
