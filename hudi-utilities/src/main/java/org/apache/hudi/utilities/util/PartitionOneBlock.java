package org.apache.hudi.utilities.util;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.SqlQueryBuilder;
import org.apache.hudi.utilities.config.JdbcSourceConfig;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;


/**
 * @author yihao
 * @version 1.0
 * @since 2024/1/5 9:07
 */
public abstract class PartitionOneBlock<K extends Comparable> {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionOneBlock.class);
  TypedProperties props;
  String filter;
  String jdbcUrlSchema;
  SqlType.SqlTypes partitionColumnType;
  String tableName;
  String partitionColumn;
  long fetchSize;
  DataFrameReader dataFrameReader;
  static final String JDBC_FETCH_SIZE_LOWER_BOUND_KEY =  JdbcSourceConfig.EXTRA_OPTIONS + "lowerBound";
  static final String JDBC_FETCH_SIZE_UPPER_BOUND_KEY =  JdbcSourceConfig.EXTRA_OPTIONS + "upperBound";
  static final String JDBC_FETCH_SIZE_NUM_PARTITIONS_KEY =  JdbcSourceConfig.EXTRA_OPTIONS + "numPartitions";
  static final long PARALLELISM_THRESHOLD = 50;
  final String ppdQuery = "(%s) rdbms_table";

  public PartitionOneBlock(TypedProperties props, DataFrameReader dataFrameReader,
                           String filter, String jdbcUrlSchema, SqlType.SqlTypes partitionColumnType, String tableName,
                           long fetchSize, String partitionColumn) {
    this.props = props;
    this.filter = filter;
    this.jdbcUrlSchema = jdbcUrlSchema;
    this.partitionColumnType = partitionColumnType;
    this.tableName = tableName;
    this.dataFrameReader = dataFrameReader;
    this.fetchSize = fetchSize;
    this.partitionColumn = partitionColumn;
  }

  public String getMaxPartitionColumnSql(Boolean isMax) {
    SqlQueryBuilder sqlQueryBuilder = SqlQueryBuilder
            .select((isMax ? "max(" : "min(") + partitionColumn + ") as nums")
            .from(tableName);

    if (!StringUtils.isNullOrEmpty(filter)) {
      sqlQueryBuilder.where(filter);
    }
    LOG.info("get max partition column sql:" + sqlQueryBuilder.toString());
    return sqlQueryBuilder.toString();
  }

  public K getMaxOrMinPartitionColumn(Boolean isMax) {
    String sql = getMaxPartitionColumnSql(isMax);
    Dataset<Row> dataset = dataFrameReader.option("dbtable", String.format(ppdQuery, sql)).load();
    if (!Objects.nonNull(dataset.first().get(0))) {
      //for empty table.
      throw new HoodieException("for empty table. table");
    }
    return generifyKeyFromSqlResult(dataset.first());
  }

  protected K generifyKeyFromSqlResult(Row result) {
    switch (partitionColumnType) {
      case Int:
      case BigInt:
      case Short:
      case Long:
      case BigDecimal:
        return (K) Long.valueOf(result.getLong(0));
      case String:
        return (K) result.getString(0);
      default:
        throw new ClassCastException("unsupported class type: " + partitionColumnType);
    }
  }

  protected String generifySqlPramFromValue(String value) {
    String valueQuote =  JdbcDataSourceUtil.getDiffSchemaValueQuote(jdbcUrlSchema);
    switch (partitionColumnType) {
      case Int:
      case BigInt:
      case Short:
      case Long:
      case BigDecimal:
        return value;
      case String:
        return  valueQuote + value + valueQuote;
      default:
        throw new ClassCastException("unsupported class type: " + partitionColumnType);
    }
  }

  protected int comparePartitionColumn(K firstKey, K secondKey) {
    switch (partitionColumnType) {
      case Int:
      case BigInt:
      case Short:
      case Long:
      case BigDecimal:
        return firstKey.compareTo(secondKey);
      case String:
        if (JdbcDataSourceUtil.PGSQL_SCHEMA_NAME.equals(jdbcUrlSchema)) {
          throw new UnsupportedOperationException("partition column can't support " + partitionColumnType + " type for driver " + jdbcUrlSchema);
        }
        String firstString = ((String) firstKey).toUpperCase();
        String secondString = ((String) secondKey).toUpperCase();
        return firstString.compareTo(secondString);
      default:
        throw new ClassCastException("unsupported class type: " + partitionColumnType);
    }
  }

  protected abstract String[] calculateRanges();

  protected abstract String getFetchSQLFormat();

  public void setParallelism(long parallelism) {
    if (parallelism > PARALLELISM_THRESHOLD) {
      parallelism = (long) Math.floor((double) parallelism / 2);
    }
    this.props.setProperty("hoodie.insert.shuffle.parallelism", Long.toString(parallelism));
    this.props.setProperty("hoodie.upsert.shuffle.parallelism", Long.toString(parallelism));
    this.props.setProperty("hoodie.delete.shuffle.parallelism", Long.toString(parallelism));
    this.props.setProperty("hoodie.bulkinsert.shuffle.parallelism", Long.toString(parallelism));
  }

}
