package org.apache.hudi.utilities.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author yihao
 * @version 1.0
 * @since 2024/1/5 9:28
 */
public class JdbcDataSourceUtil {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcDataSourceUtil.class);
  public static final String MYSQL_SCHEMA_NAME = "mysql";
  public static final String PGSQL_SCHEMA_NAME = "postgresql";
  public static final String ORACLE_SCHEMA_NAME = "oracle";
  public static final String DB2_SCHEMA_NAME = "db2";
  public static final String SQLSERVER_SCHEMA_NAME = "sqlserver";

  public static String getDiffSchemaValueQuote(String jdbcUrlSchema) {
    switch (jdbcUrlSchema) {
      case JdbcDataSourceUtil.MYSQL_SCHEMA_NAME:
        return "\"";
      case JdbcDataSourceUtil.ORACLE_SCHEMA_NAME:
      case JdbcDataSourceUtil.DB2_SCHEMA_NAME:
      case JdbcDataSourceUtil.PGSQL_SCHEMA_NAME:
      case JdbcDataSourceUtil.SQLSERVER_SCHEMA_NAME:
        return "'";
      default:
        throw new UnsupportedOperationException("current scheme value quote not supported. Only support mysql and oracle and postgresql and db2");
    }
  }

}
