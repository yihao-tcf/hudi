package org.apache.hudi.utilities.util;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utilities.SqlQueryBuilder;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author yihao
 * @version 1.0
 * @since 2024/1/5 15:09
 */
public class AccuratePartitionOneBlock<K extends Comparable> extends PartitionOneBlock<K> {

  private static final Logger LOG = LoggerFactory.getLogger(AccuratePartitionOneBlock.class);
  AccuratePartitionOneBlock(TypedProperties props, DataFrameReader dataFrameReader,
                            String filter, String jdbcUrlSchema, SqlType.SqlTypes partitionColumnType,
                            String tableName, long fetchSize, String partitionColumn) {
    super(props, dataFrameReader, filter, jdbcUrlSchema, partitionColumnType, tableName, fetchSize,  partitionColumn);
  }

  @Override
  protected  String[] calculateRanges() {
    K maxPartitionVal  =  getMaxOrMinPartitionColumn(true);
    K minPartitionVal = getMaxOrMinPartitionColumn(false);
    K currentMaxPartitionVal  = minPartitionVal;
    K preMaxPartitionVal = minPartitionVal;
    List<String> predicates = new ArrayList<>();
    long numPartitions = 1L;
    while (comparePartitionColumn(currentMaxPartitionVal, maxPartitionVal) < 0) {
      String predicate;
      LOG.info("[FetchSize range query. Table:{}] Execute {}, preMaxPartitionVal:{} current partitionColumnVal:{} maxPartitionVal:{}",
              tableName, getFetchSQLFormat(preMaxPartitionVal), preMaxPartitionVal, currentMaxPartitionVal, maxPartitionVal);
      if (comparePartitionColumn(preMaxPartitionVal, currentMaxPartitionVal) == 0) {
        currentMaxPartitionVal = getOneSplitMaxPartitionColumn(currentMaxPartitionVal);
        predicates.add(String.format("%s <= %s", partitionColumn, generifySqlPramFromValue(preMaxPartitionVal.toString())));

        predicate =  String.format("%s > %s AND %s <= %s", partitionColumn, generifySqlPramFromValue(preMaxPartitionVal.toString()),
                partitionColumn, generifySqlPramFromValue(currentMaxPartitionVal.toString()));
      } else {
        preMaxPartitionVal = currentMaxPartitionVal;
        currentMaxPartitionVal = getOneSplitMaxPartitionColumn(preMaxPartitionVal);
        predicate = String.format("%s > %s AND %s <= %s", partitionColumn, generifySqlPramFromValue(preMaxPartitionVal.toString()),
                partitionColumn, generifySqlPramFromValue(currentMaxPartitionVal.toString()));
      }
      predicates.add(predicate);
      numPartitions++;
    }
    setParallelism(numPartitions);
    return predicates.toArray(new String[0]);
  }

  @Override
  protected String getFetchSQLFormat() {
    return null;
  }

  public K getOneSplitMaxPartitionColumn(K lastPartitionVal) {
    String sql = getFetchSQLFormat(lastPartitionVal);
    Dataset<Row> dataset = dataFrameReader.option("dbtable", String.format(ppdQuery, sql)).load();
    if (!Objects.nonNull(dataset.first().get(0))) {
      return lastPartitionVal;
    }
    return generifyKeyFromSqlResult(dataset);
  }

  public String getFetchSQLFormat(K lastPartitionVal) {
    SqlQueryBuilder subSqlQueryBuilder = SqlQueryBuilder.select(partitionColumn).from(tableName);
    String fetchSizeCondition = new StringBuilder()
            .append(partitionColumn)
            .append(">")
            .append(generifySqlPramFromValue(lastPartitionVal.toString())).toString();

    if (!StringUtils.isNullOrEmpty(filter)) {
      subSqlQueryBuilder.where("(" + filter + ") and " + fetchSizeCondition);
    } else {
      subSqlQueryBuilder.where(fetchSizeCondition);
    }
    subSqlQueryBuilder.orderBy(partitionColumn);
    subSqlQueryBuilder.limit(fetchSize, jdbcUrlSchema);

    String sql = SqlQueryBuilder.select("max(" + partitionColumn + ") as " + NUMBER_VALUE_ALIAS).from("(" + subSqlQueryBuilder).toString();
    LOG.info("accurate partition one block fetchSize sql:" + sql);
    return sql;
  }
}
