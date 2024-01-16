package org.apache.hudi.utilities.util;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.config.JdbcSourceConfig;

import org.apache.spark.sql.DataFrameReader;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.util.ConfigUtils.DELTA_STREAMER_CONFIG_PREFIX;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;


/**
 * @author yihao
 * @version 1.0
 * @since 2024/1/4 17:53
 */
public class CustomShardSplitProvider<K extends Comparable> {
  private ShardSplitMode shardSplitMode;

  private DataFrameReader dataFrameReader;

  private TypedProperties props;

  String filter;
  String jdbcUrlSchema;
  SqlType.SqlTypes partitionColumnType;
  String tableName;
  String partitionColumn;
  long fetchSize;

  private static final String URI_JDBC_PREFIX = "jdbc:";

  static final String JDBC_FETCH_SIZE_KEY = JdbcSourceConfig.EXTRA_OPTIONS.key() + "fetchSize";

  public CustomShardSplitProvider(DataFrameReader dataFrameReader,String shardSplitMode, TypedProperties props) {
    Map<String, Object> map = new HashMap<>();
    props.forEach((key, val) -> map.put((String) key, val));
    this.dataFrameReader = dataFrameReader;
    this.shardSplitMode = ShardSplitMode.valueOf(shardSplitMode);
    this.jdbcUrlSchema = URI.create(getStringWithAltKeys(props, JdbcSourceConfig.URL).substring(URI_JDBC_PREFIX.length())).getScheme().toLowerCase();
    this.partitionColumnType =  SqlType.getSqlType(getStringWithAltKeys(map, JdbcSourceConfig.PARTITION_COLUMN_TYPE), this.jdbcUrlSchema);
    this.tableName = getStringWithAltKeys(props, JdbcSourceConfig.RDBMS_TABLE_NAME);
    ConfigProperty<String> fetchSize = ConfigProperty.key(JdbcSourceConfig.EXTRA_OPTIONS.key() + "fetchSize")
              .defaultValue("100000")
              .markAdvanced()
              .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "jdbc.extra.options.fetchSize");

    ConfigProperty<String> partitionColumn = ConfigProperty.key(JdbcSourceConfig.EXTRA_OPTIONS.key() + "partitionColumn")
              .noDefaultValue()
              .markAdvanced()
              .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "jdbc.extra.options.partitionColumn");

    this.fetchSize = Long.parseLong(getStringWithAltKeys(map, fetchSize));
    this.partitionColumn = getStringWithAltKeys(map, partitionColumn);
    this.filter =  getStringWithAltKeys(map, JdbcSourceConfig.WHERE_EXPRESSION);
    props.setProperty(JDBC_FETCH_SIZE_KEY, getStringWithAltKeys(map, fetchSize));
    this.props = props;
  }

  public String[] getRangeValues() {
    switch (shardSplitMode) {
      case quick:
        if (partitionColumnType == SqlType.SqlTypes.String) {
          throw new UnsupportedOperationException("string split id with quick split not supported yet...");
        }
        return new QuickPartitionOneBlock<K>(props, dataFrameReader, filter, jdbcUrlSchema, partitionColumnType, tableName, fetchSize,  partitionColumn).calculateRanges();
      case accurate:
        return new AccuratePartitionOneBlock<K>(props, dataFrameReader, filter, jdbcUrlSchema, partitionColumnType, tableName, fetchSize,  partitionColumn).calculateRanges();
      default:
        throw new HoodieException("shard partition mode:" + shardSplitMode.name() + " is not exist");
    }
  }

  /**
    * shar split mode
    */
  public enum ShardSplitMode {
    quick,
    accurate

  }
}
