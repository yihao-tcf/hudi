package org.apache.hudi.utilities.util;

import org.apache.hudi.exception.HoodieException;

/**
 * @author yihao
 * @version 1.0
 * @since 2024/1/4 16:58
 */
public class SqlType {

  public static SqlTypes getSqlType(String type, String jdbcUrlSchema) {
    String lowercaseType =  type.toLowerCase();
    switch (jdbcUrlSchema) {
      case JdbcDataSourceUtil.MYSQL_SCHEMA_NAME:
        return getSqlTypeFromMysql(lowercaseType);
      case JdbcDataSourceUtil.ORACLE_SCHEMA_NAME:
        return getSqlTypeFromOracle(lowercaseType);
      case JdbcDataSourceUtil.DB2_SCHEMA_NAME:
        return getSqlTypeFromDb2(lowercaseType);
      case JdbcDataSourceUtil.PGSQL_SCHEMA_NAME:
        return getSqlTypeFromPostgresql(lowercaseType);
      case JdbcDataSourceUtil.SQLSERVER_SCHEMA_NAME:
        return getSqlTypeFromSqlServer(lowercaseType);
      default:
        throw new UnsupportedOperationException("current scheme  not supported. Only support mysql and oracle and postgresql and db2 and mssql");
    }
  }

  private static SqlTypes getSqlTypeFromDb2(String lowercaseType) {
    switch (lowercaseType) {
      case "smallint":
        return SqlTypes.Short;
      case "integer":
      case "int":
        return SqlTypes.Int;
      case "bigint":
        return SqlTypes.Long;
      case "timestamp":
        return SqlTypes.Timestamp;
      case "date":
        return SqlTypes.Date;
      case "time":
        return SqlTypes.Time;
      case "float":
        return SqlTypes.Float;
      case "double":
      case "real":
        return SqlTypes.Double;
      case "numeric":
      case "decimal":
      case "decfloat":
        return SqlTypes.BigDecimal;
      case "char":
      case "nchar":
      case "varchar":
      case "nvarchar":
      case "graphic":
      case "vargraphic":
      case "clob":
      case "nclob":
      case "dbclob":
      case "character":
      case "rowid":
      case "xml":
        return SqlTypes.String;
      case "blob":
      case "binary":
      case "varbinary":
        return SqlTypes.Bytes;
      default:
        throw new HoodieException(
            String.format("DB2: The column data type in your configuration is not support. Column type:[%s]."
                    +
                " Please try to change the column data type or don't transmit this column.", lowercaseType)
        );
    }
  }

  private static SqlTypes getSqlTypeFromOracle(String lowercaseType) {
    switch (lowercaseType) {
      case "bit":
      case "bool":
        return SqlTypes.Boolean;
      case "smallint":
        return SqlTypes.Short;
      case "integer":
      case "int":
        return SqlTypes.Int;
      case "number":
        return SqlTypes.BigInt;
      case "timestamp":
      case "datetime":
        return SqlTypes.Timestamp;
      case "date":
        return SqlTypes.Date;
      case "float":
      case "binary_float":
        return SqlTypes.Float;
      case "double":
      case "double precision":
      case "real":
      case "binary_double":
        return SqlTypes.Double;
      case "numeric":
      case "decimal":
        return SqlTypes.BigDecimal;
      case "char":
      case "varchar":
      case "varchar2":
      case "nchar":
      case "nvarchar2":
      case "long":
      case "clob":
      case "nclob":
      case "string":
      case "character":
      case "rowid":
      case "urowid":
      case "xmltype":
        return SqlTypes.String;
      case "blob":
      case "bfile":
      case "raw":
      case "long raw":
        return SqlTypes.Bytes;
      default:
        throw new  HoodieException(
            String.format("Oracle: The column data type in your configuration is not support. Column type:[%s]."
                    +
                " Please try to change the column data type or don't transmit this column.", lowercaseType)
        );
    }
  }

  private static SqlTypes getSqlTypeFromMysql(String lowercaseType) {
    switch (lowercaseType) {
      case "bit":
        return SqlTypes.Boolean;
      case "tinyint":
      case "tinyint unsigned":
        return SqlTypes.Short;
      case "smallint":
      case "smallint unsigned":
      case "mediumint":
      case "mediumint unsigned":
      case "int":
      case "int unsigned":
        return SqlTypes.Long;
      case "bigint":
      case "bigint unsigned":
        return SqlTypes.BigInt;
      case "datetime":
      case "timestamp":
        return SqlTypes.Timestamp;
      case "time":
        return SqlTypes.Time;
      case "date":
        return SqlTypes.Date;
      case "year":
        return SqlTypes.Year;
      case "float":
        return SqlTypes.Float;
      case "double":
      case "real":
        return SqlTypes.Double;
      case "decimal":
        return SqlTypes.BigDecimal;
      case "enum":
      case "char":
      case "nvar":
      case "varchar":
      case "nvarchar":
      case "longnvarchar":
      case "longtext":
      case "text":
      case "mediumtext":
      case "string":
      case "longvarchar":
      case "tinytext":
      case "json":
        return SqlTypes.String;
      case "tinyblob":
      case "blob":
      case "mediumblob":
      case "longblob":
      case "binary":
      case "longvarbinary":
      case "varbinary":
        return SqlTypes.Bytes;
      default:
        throw new HoodieException(
            String.format("MySQL: The column data type in your configuration is not support. Column type:[%s]."
                    +
                " Please try to change the column data type or don't transmit this column.", lowercaseType)
        );
    }
  }

  public static SqlTypes getSqlTypeFromPostgresql(final String lowercaseType)  {
    switch (lowercaseType) {
      case "boolean":
        return SqlTypes.Boolean;
      case "smallserial":
      case "smallint":
      case "int2":
        return SqlTypes.Short;
      case "integer":
      case "int4":
      case "int":
      case "serial":
        return SqlTypes.Int;
      case "int8":
      case "bigint":
      case "bigserial":
        return SqlTypes.Long;
      case "timestamp":
        return SqlTypes.Timestamp;
      case "time":
        return SqlTypes.Time;
      case "date":
        return SqlTypes.Date;
      case "real":
      case "float":
      case "float4":
        return SqlTypes.Float;
      case "double":
      case "float8":
      case "double precision":
        return SqlTypes.Double;
      case "money":
        return SqlTypes.Money;
      case "decimal":
      case "numeric":
        return SqlTypes.BigDecimal;
      case "char":
      case "varchar":
      case "text":
      case "character varying":
      case "string":
      case "character":
        return SqlTypes.String;
      case "bit":
      case "bit varying":
      case "varbit":
      case "uuid":
      case "cidr":
      case "xml":
      case "macaddr":
      case "json":
      case "enum":
        return SqlTypes.OtherTypeString;
      case "bytea":
        return SqlTypes.Bytes;
      default:
        throw new HoodieException(
            String.format("PostgreSQL: The column data type in your configuration is not support. Column type:[%s]."
                    +
                " Please try to change the column data type or don't transmit this column.", lowercaseType)
        );
    }
  }

  public static SqlTypes getSqlTypeFromSqlServer(String lowercaseType) {
    switch (lowercaseType) {
      case "bit" :
        return SqlTypes.Boolean;
      case "smallint":
      case "tinyint":
        return SqlTypes.Short;
      case "int":
      case "int identity":
      case "integer":
        return SqlTypes.Int;
      case "bigint":
        return SqlTypes.Long;
      case "timestamp":
      case "datetime":
      case "datetime2":
        return SqlTypes.Timestamp;
      case "date":
        return SqlTypes.Date;
      case "time":
        return SqlTypes.Time;
      case "float":
        return SqlTypes.Float;
      case "double precision":
      case "real":
        return SqlTypes.Double;
      case "numeric":
      case "decimal":
        return SqlTypes.BigDecimal;
      case "char":
      case "varchar":
      case "text":
      case "nchar":
      case "nvarchar":
      case "ntext":
        return SqlTypes.String;
      case "binary":
      case "varbinary":
      case "image":
        return SqlTypes.Bytes;
      default:
        throw new HoodieException(
                  String.format("SqlServer: The column data type in your configuration is not support. Column type:[%s]."
                          +
                          " Please try to change the column data type or don't transmit this column.", lowercaseType)
          );
    }
  }

  /** sql type enum
   *
   */
  public enum SqlTypes {
      Boolean,
      Short,
      Int,
      Long,
      Timestamp,
      Time,
      Date,
      Year,
      Float,
      Double,
      Money,
      OtherTypeString,
      String,
      Bytes,
      BigInt,
      BigDecimal,
      Array
  }
}
