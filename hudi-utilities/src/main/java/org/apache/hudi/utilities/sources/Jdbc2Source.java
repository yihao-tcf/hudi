/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.sources;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.SqlQueryBuilder;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;

import java.net.URI;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Reads data from RDBMS data sources.
 */

public class Jdbc2Source extends RowSource {

  private static final Logger LOG = LogManager.getLogger(Jdbc2Source.class);
  private static final List<String> DB_LIMIT_CLAUSE = Arrays.asList("mysql", "postgresql", "h2", "db2");
  private static final String URI_JDBC_PREFIX = "jdbc:";

  public Jdbc2Source(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                     SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  /**
   * Validates all user properties and prepares the {@link DataFrameReader} to read from RDBMS.
   *
   * @param session    The {@link SparkSession}.
   * @param properties The JDBC connection properties and data source options.
   * @return The {@link DataFrameReader} to read from RDBMS
   * @throws HoodieException
   */
  private static DataFrameReader validatePropsAndGetDataFrameReader(final SparkSession session,
                                                                    final TypedProperties properties,
                                                                    final Option<String> lastCheckpoint)
      throws HoodieException {
    DataFrameReader dataFrameReader;
    FSDataInputStream passwordFileStream = null;
    try {
      dataFrameReader = session.read().format("jdbc");
      dataFrameReader = dataFrameReader.option(Config.URL_PROP, properties.getString(Config.URL));
      dataFrameReader = dataFrameReader.option(Config.USER_PROP, properties.getString(Config.USER));
      dataFrameReader = dataFrameReader.option(Config.DRIVER_PROP, properties.getString(Config.DRIVER_CLASS));
      dataFrameReader = dataFrameReader
          .option(Config.RDBMS_TABLE_PROP, properties.getString(Config.RDBMS_TABLE_NAME));

      if (properties.containsKey(Config.PASSWORD)) {
        LOG.info("Reading JDBC password from properties file....");
        dataFrameReader = dataFrameReader.option(Config.PASSWORD_PROP, properties.getString(Config.PASSWORD));
      } else if (properties.containsKey(Config.PASSWORD_FILE)
          && !StringUtils.isNullOrEmpty(properties.getString(Config.PASSWORD_FILE))) {
        LOG.info(String.format("Reading JDBC password from password file %s", properties.getString(Config.PASSWORD_FILE)));
        FileSystem fileSystem = FileSystem.get(session.sparkContext().hadoopConfiguration());
        passwordFileStream = fileSystem.open(new Path(properties.getString(Config.PASSWORD_FILE)));
        byte[] bytes = new byte[passwordFileStream.available()];
        passwordFileStream.read(bytes);
        dataFrameReader = dataFrameReader.option(Config.PASSWORD_PROP, new String(bytes));
      } else {
        throw new IllegalArgumentException(String.format("JDBCSource needs either a %s or %s to connect to RDBMS "
            + "datasource", Config.PASSWORD_FILE, Config.PASSWORD));
      }

      if (!lastCheckpoint.isPresent()) {
        addExtraJdbcOptions(properties, dataFrameReader);
      }

      if (properties.getBoolean(Config.IS_INCREMENTAL)) {
        DataSourceUtils.checkRequiredProperties(properties, Collections.singletonList(Config.INCREMENTAL_COLUMN));
      }
      return dataFrameReader;
    } catch (Exception e) {
      throw new HoodieException("Failed to validate properties", e);
    } finally {
      IOUtils.closeStream(passwordFileStream);
    }
  }

  /**
   * get {@link DataFrameReader} to read from RDBMS.
   * @param session    The {@link SparkSession}.
   * @param properties The JDBC connection properties and data source options.
   * @return The {@link DataFrameReader} to read from RDBMS
   * @throws HoodieException
   */
  private static DataFrameReader getDataFrameReader(final SparkSession session,
                                                    final TypedProperties properties)
      throws HoodieException {
    DataFrameReader dataFrameReader;
    FSDataInputStream passwordFileStream = null;
    final String JDBC = "jdbc";
    try {
      dataFrameReader = session.read().format("jdbc");
      dataFrameReader = dataFrameReader.option(Config.URL_PROP, properties.getString(Config.URL));
      dataFrameReader = dataFrameReader.option(Config.USER_PROP, properties.getString(Config.USER));
      dataFrameReader = dataFrameReader.option(Config.DRIVER_PROP, properties.getString(Config.DRIVER_CLASS));
      dataFrameReader = dataFrameReader
                  .option(Config.RDBMS_TABLE_PROP, properties.getString(Config.RDBMS_TABLE_NAME));

      if (properties.containsKey(Config.PASSWORD)) {
        LOG.info("Reading JDBC password from properties file....");
        dataFrameReader = dataFrameReader.option(Config.PASSWORD_PROP, properties.getString(Config.PASSWORD));

      } else if (properties.containsKey(Config.PASSWORD_FILE) && !StringUtils.isNullOrEmpty(properties.getString(Config.PASSWORD_FILE))) {
        LOG.info(String.format("Reading JDBC password from password file %s", properties.getString(Config.PASSWORD_FILE)));
        FileSystem fileSystem = FileSystem.get(session.sparkContext().hadoopConfiguration());
        passwordFileStream = fileSystem.open(new Path(properties.getString(Config.PASSWORD_FILE)));
        byte[] bytes = new byte[passwordFileStream.available()];
        passwordFileStream.read(bytes);
        dataFrameReader = dataFrameReader.option(Config.PASSWORD_PROP, new String(bytes));
      } else {
        throw new IllegalArgumentException(String.format("JDBCSource needs either a %s or %s to connect to RDBMS "
                      + "datasource", Config.PASSWORD_FILE, Config.PASSWORD));
      }
      return dataFrameReader;
    } catch (Exception e) {
      throw new HoodieException("Failed to validate properties", e);
    } finally {
      IOUtils.closeStream(passwordFileStream);
    }
  }

  /**
   *This function is to prompt for the issue of data with the same incremental column in the source table having a data volume greater than the sourceLimit.
   * If the user is not prompted to adjust the correct sourceLimit or fetchSize, using jdbcSource may result in partial data loss.
   *<br/>
   *
   * <p>
   * Example: Assuming the source table order table has a total data volume of 5 million. Synchronize using deltasteamer JdbcSource
   * </p>
   * deltamer conf:<br/>
   * <b>--hoodie-conf hoodie.deltastreamer.jdbc.incr.pull=true</b><br/>
   * <b>--hoodie-conf hoodie.deltastreamer.jdbc.table.incr.column.name=update_date</b><br/>
   * <b>--source-limit 100000</b><br/>
   * <b>--continuous</b><br/>
   *
   * When deltasteamer synchronizes to 40w data, the current <b>lastCheckpoint=2023-08-17 14:55 0:00:00</b>
   * So the SQL for {@link #incrementalFetch} Method to query source data is:<br/>
   * select (select * from order where update_date>"2023-08-17 14:55 0:00:00" order by update_date limit 100000) rdbms_table.<br/>
   * Assuming that there is 200000 data in the updateDate field of my order table, which is equal to "2023-08-17 14:55 1:00:000"
   * will only obtain 100000 rows of data due to sourceLimit=100000, and will also lose 100000 rows of data.
   *
   * @param session {@link SparkSession}
   * @param properties The JDBC connection properties and data source options.
   * @param sourceLimit Limit the amount of query data for tables
   */
  private static void validateTableIncrementalColumnDuplicateNum(SparkSession session,
                                                                 TypedProperties properties,
                                                                 long sourceLimit) {
    String incrementalColumn = properties.getString(Config.INCREMENTAL_COLUMN);
    if (!StringUtils.isNullOrEmpty(incrementalColumn) && properties.getBoolean(Config.IS_INCREMENTAL)) {
      final String ppdQuery = "(%s) rdbms_table";
      final String countColumn = String.format("count(%s) as countNum", incrementalColumn);
      final SqlQueryBuilder queryBuilder = SqlQueryBuilder
              .select(incrementalColumn, countColumn)
              .from(properties.getString(Config.RDBMS_TABLE_NAME))
              .where(String.format("%s is not null", incrementalColumn))
              .groupBy(incrementalColumn)
              .orderBy("countNum desc")
              .limit(1);
      String query = String.format(ppdQuery, queryBuilder.toString());
      LOG.info("validate validateTableIncrementalColumnDuplicateNum query sql:" + query);
      Dataset<Row> dataset = getDataFrameReader(session, properties).option(Config.RDBMS_TABLE_PROP, query).load();
      if (Objects.nonNull(dataset.first().get(1))) {
        long dupCount = dataset.withColumn("countNum", dataset.col("countNum").cast(DataTypes.LongType)).first().getLong(1);
        if (sourceLimit < dupCount) {
          LOG.error(String.format("The incrementalColumn name is:%s "
                          +
                          "the current value grouped by incrementalColumn is:%s, and the number of duplicate checks is:%s."
                          +
                          "The set sourceLimit is greater than the number of duplicate checks",
                  incrementalColumn,
                  dataset.first().get(0),
                  dupCount));
        }
        throw new HoodieException(String.format("Jdbc2Source fetch Failed, sourceLimit:%s < number of duplicate checks:%s",
                sourceLimit,
                dupCount));
      }
    }
  }

  /**
   * Accepts spark JDBC options from the user in terms of EXTRA_OPTIONS adds them to {@link DataFrameReader} Example: In
   * a normal spark code you would do something like: session.read.format('jdbc') .option(fetchSize,1000)
   * .option(timestampFormat,"yyyy-mm-dd hh:mm:ss")
   * <p>
   * The way to pass these properties to HUDI is through the config file. Any property starting with
   * hoodie.deltastreamer.jdbc.extra.options. will be added.
   * <p>
   * Example: hoodie.deltastreamer.jdbc.extra.options.fetchSize=100
   * hoodie.deltastreamer.jdbc.extra.options.upperBound=1
   * hoodie.deltastreamer.jdbc.extra.options.lowerBound=100
   *
   * @param properties      The JDBC connection properties and data source options.
   * @param dataFrameReader The {@link DataFrameReader} to which data source options will be added.
   */
  private static void addExtraJdbcOptions(TypedProperties properties, DataFrameReader dataFrameReader) {
    Set<Object> objects = properties.keySet();
    for (Object property : objects) {
      String prop = property.toString();
      if (prop.startsWith(Config.EXTRA_OPTIONS)) {
        String key = String.join("", prop.split(Config.EXTRA_OPTIONS));
        String value = properties.getString(prop);
        if (!StringUtils.isNullOrEmpty(value)) {
          LOG.info(String.format("Adding %s -> %s to jdbc options", key, value));
          dataFrameReader.option(key, value);
        }
      }
    }
  }

  @Override
  protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) throws HoodieException {
    try {
      DataSourceUtils.checkRequiredProperties(props, Arrays.asList(Config.URL, Config.DRIVER_CLASS, Config.USER, Config.RDBMS_TABLE_NAME, Config.IS_INCREMENTAL));
      return fetch(lastCkptStr, sourceLimit);
    } catch (HoodieException e) {
      LOG.error("Exception while running JDBCSource ", e);
      throw e;
    } catch (Exception e) {
      LOG.error("Exception while running JDBCSource ", e);
      throw new HoodieException("Error fetching next batch from JDBC source. Last checkpoint: " + lastCkptStr.orElse(null), e);
    }
  }

  /**
   * Decide to do a full RDBMS table scan or an incremental scan based on the lastCkptStr. If previous checkpoint
   * value exists then we do an incremental scan with a PPD query or else we do a full scan. In certain cases where the
   * incremental query fails, we fallback to a full scan.
   *
   * @param lastCkptStr Last checkpoint.
   * @return The pair of {@link Dataset} and current checkpoint.
   */
  private Pair<Option<Dataset<Row>>, String> fetch(Option<String> lastCkptStr, long sourceLimit) {
    Dataset<Row> dataset;
    validateTableIncrementalColumnDuplicateNum(sparkSession, props, sourceLimit);
    if (lastCkptStr.isPresent() && !StringUtils.isNullOrEmpty(lastCkptStr.get())) {
      dataset = incrementalFetch(lastCkptStr, sourceLimit);
    } else {
      LOG.info("No checkpoint references found. Doing a full rdbms table fetch");
      dataset = fullFetch(lastCkptStr);
    }
    dataset.persist(StorageLevel.fromString(props.getString(Config.STORAGE_LEVEL, "MEMORY_AND_DISK_SER")));
    boolean isIncremental = props.getBoolean(Config.IS_INCREMENTAL);
    Pair<Option<Dataset<Row>>, String> pair = Pair.of(Option.of(dataset), checkpoint(dataset, isIncremental, lastCkptStr));
    dataset.unpersist();
    return pair;
  }

  /**
   * Does an incremental scan with PPQ query prepared on the bases of previous checkpoint.
   *
   * @param lastCheckpoint Last checkpoint.
   *                       Note that the records fetched will be exclusive of the last checkpoint (i.e. incremental column value > lastCheckpoint).
   * @return The {@link Dataset} after incremental fetch from RDBMS.
   */
  private Dataset<Row> incrementalFetch(Option<String> lastCheckpoint, long sourceLimit) {
    try {
      final String ppdQuery = "(%s) rdbms_table";
      final SqlQueryBuilder queryBuilder = SqlQueryBuilder.select("*")
          .from(props.getString(Config.RDBMS_TABLE_NAME))
          //remedy data for empty fields
          .where(String.format(" %s > '%s' or %s is null",
                  props.getString(Config.INCREMENTAL_COLUMN),
                  lastCheckpoint.get(),
                  props.getString(Config.INCREMENTAL_COLUMN)));

      if (sourceLimit > 0) {
        URI jdbcURI = URI.create(props.getString(Config.URL).substring(URI_JDBC_PREFIX.length()));
        if (DB_LIMIT_CLAUSE.contains(jdbcURI.getScheme())) {
          queryBuilder.orderBy(props.getString(Config.INCREMENTAL_COLUMN)).limit(sourceLimit);
        }
      }
      String query = String.format(ppdQuery, queryBuilder.toString());
      LOG.info("PPD QUERY: " + query);
      LOG.info(String.format("Referenced last checkpoint and prepared new predicate pushdown query for jdbc pull %s", query));
      return validatePropsAndGetDataFrameReader(sparkSession, props, lastCheckpoint).option(Config.RDBMS_TABLE_PROP, query).load();
    } catch (Exception e) {
      LOG.error("Error while performing an incremental fetch. Not all database support the PPD query we generate to do an incremental scan", e);
      if (props.containsKey(Config.FALLBACK_TO_FULL_FETCH) && props.getBoolean(Config.FALLBACK_TO_FULL_FETCH)) {
        LOG.warn("Falling back to full scan.");
        return fullFetch(lastCheckpoint);
      }
      throw e;
    }
  }

  /**
   * Does a full scan on the RDBMS data source.
   *
   * @return The {@link Dataset} after running full scan.
   */
  private Dataset<Row> fullFetch(Option<String> lastCheckpoint) {
    //remedy data for empty fields
    Dataset<Row> fillEmptyDataset = null;
    String ppdQuery = "(%s) rdbms_table";
    String partitionColumnKey = Config.EXTRA_OPTIONS + "partitionColumn";
    String partitionColumnValue = this.props.getString(partitionColumnKey);
    if (!StringUtils.isNullOrEmpty(partitionColumnValue)) {
      SqlQueryBuilder sqlQueryBuilder = SqlQueryBuilder.select("*")
              .from(props.getString(Config.RDBMS_TABLE_NAME))
              .where(MessageFormat.format("{0} is null", partitionColumnValue));
      String query = String.format(ppdQuery, sqlQueryBuilder.toString());
      LOG.info(String.format("remedy data for empty fields query sql:%s", query));
      fillEmptyDataset =  getDataFrameReader(this.sparkSession, this.props)
              .option(Config.RDBMS_TABLE_PROP, query).load();
      LOG.info(String.format("fullFetch: Quantity of residual data for empty fields:%s", fillEmptyDataset.count()));
    }

    Dataset<Row> notNullDataset =  validatePropsAndGetDataFrameReader(this.sparkSession, this.props, lastCheckpoint).load();
    return fillEmptyDataset == null ? notNullDataset : notNullDataset.unionAll(fillEmptyDataset);
  }

  /**
   * Does a full scan on the RDBMS data source.
   *
   * @return The {@link Dataset} after running full scan.
   */
  private Dataset<Row> fullFetch(long sourceLimit) {
    final String ppdQuery = "(%s) rdbms_table";
    final SqlQueryBuilder queryBuilder = SqlQueryBuilder.select("*")
        .from(props.getString(Config.RDBMS_TABLE_NAME));
    if (sourceLimit > 0) {
      URI jdbcURI = URI.create(props.getString(Config.URL).substring(URI_JDBC_PREFIX.length()));
      if (DB_LIMIT_CLAUSE.contains(jdbcURI.getScheme())) {
        if (props.containsKey(Config.INCREMENTAL_COLUMN)) {
          queryBuilder.orderBy(props.getString(Config.INCREMENTAL_COLUMN)).limit(sourceLimit);
        } else {
          queryBuilder.limit(sourceLimit);
        }
      }
    }
    String query = String.format(ppdQuery, queryBuilder.toString());
    return validatePropsAndGetDataFrameReader(sparkSession, props, null).option(Config.RDBMS_TABLE_PROP, query).load();
  }  

  private String checkpoint(Dataset<Row> rowDataset, boolean isIncremental, Option<String> lastCkptStr) {
    try {
      if (isIncremental) {
        Column incrementalColumn = rowDataset.col(props.getString(Config.INCREMENTAL_COLUMN));
        final String max = rowDataset.agg(functions.max(incrementalColumn).cast(DataTypes.StringType)).first().getString(0);
        LOG.info(String.format("Checkpointing column %s with value: %s ", incrementalColumn, max));
        if (max != null) {
          return max;
        }
        return lastCkptStr.isPresent() && !StringUtils.isNullOrEmpty(lastCkptStr.get()) ? lastCkptStr.get() : StringUtils.EMPTY_STRING;
      } else {
        return StringUtils.EMPTY_STRING;
      }
    } catch (Exception e) {
      LOG.error("Failed to checkpoint");
      throw new HoodieException("Failed to checkpoint. Last checkpoint: " + lastCkptStr.orElse(null), e);
    }
  }

  /**
   * Inner class with config keys.
   */
  protected static class Config {

    /**
     * {@value #URL} is the jdbc url for the Hoodie datasource.
     */
    private static final String URL = "hoodie.deltastreamer.jdbc.url";

    private static final String URL_PROP = "url";

    /**
     * {@value #USER} is the username used for JDBC connection.
     */
    private static final String USER = "hoodie.deltastreamer.jdbc.user";

    /**
     * {@value #USER_PROP} used internally to build jdbc params.
     */
    private static final String USER_PROP = "user";

    /**
     * {@value #PASSWORD} is the password used for JDBC connection.
     */
    private static final String PASSWORD = "hoodie.deltastreamer.jdbc.password";

    /**
     * {@value #PASSWORD_FILE} is the base-path for the JDBC password file.
     */
    private static final String PASSWORD_FILE = "hoodie.deltastreamer.jdbc.password.file";

    /**
     * {@value #PASSWORD_PROP} used internally to build jdbc params.
     */
    private static final String PASSWORD_PROP = "password";

    /**
     * {@value #DRIVER_CLASS} used for JDBC connection.
     */
    private static final String DRIVER_CLASS = "hoodie.deltastreamer.jdbc.driver.class";

    /**
     * {@value #DRIVER_PROP} used internally to build jdbc params.
     */
    private static final String DRIVER_PROP = "driver";

    /**
     * {@value #RDBMS_TABLE_NAME} RDBMS table to pull.
     */
    private static final String RDBMS_TABLE_NAME = "hoodie.deltastreamer.jdbc.table.name";

    /**
     * {@value #RDBMS_TABLE_PROP} used internally for jdbc.
     */
    private static final String RDBMS_TABLE_PROP = "dbtable";

    /**
     * {@value #INCREMENTAL_COLUMN} if ran in incremental mode, this field will be used to pull new data incrementally.
     */
    private static final String INCREMENTAL_COLUMN = "hoodie.deltastreamer.jdbc.table.incr.column.name";

    /**
     * {@value #IS_INCREMENTAL} will the JDBC source do an incremental pull?
     */
    private static final String IS_INCREMENTAL = "hoodie.deltastreamer.jdbc.incr.pull";

    /**
     * {@value #EXTRA_OPTIONS} used to set any extra options the user specifies for jdbc.
     */
    private static final String EXTRA_OPTIONS = "hoodie.deltastreamer.jdbc.extra.options.";

    /**
     * {@value #STORAGE_LEVEL} is used to control the persistence level. Default value: MEMORY_AND_DISK_SER.
     */
    private static final String STORAGE_LEVEL = "hoodie.deltastreamer.jdbc.storage.level";

    /**
     * {@value #FALLBACK_TO_FULL_FETCH} is a boolean, which if set true, makes incremental fetch to fallback to full fetch in case of any error.
     */
    private static final String FALLBACK_TO_FULL_FETCH = "hoodie.deltastreamer.jdbc.incr.fallback.to.full.fetch";
  }
}
