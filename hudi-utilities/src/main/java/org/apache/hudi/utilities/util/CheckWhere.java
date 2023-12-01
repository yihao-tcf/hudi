package org.apache.hudi.utilities.util;

import org.apache.hudi.exception.HoodieException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * check sql where Prevent SQL injection
 * @author yihao
 * @version 1.0
 * @since 2023/11/30 9:28
 */
public class CheckWhere {
  public static final String CWF_DISABLE_EQUATION_EXP_REGEX = "((\'[^\']*\'\\s*|\"[^\"]*\\\"\\s*)+\\s*=\\s*(\'[^\']*\'\\s*|\"[^\"]*\"\\s*)+|([+-]?(?:\\d*\\.)?\\d+)"
            +
            "\\s*=\\s*[+-]?(?:\\d*\\.)?\\d+|([^\'\"\\s]+)\\s*=\\s*\\5\\b|([+-]?(?:\\d*\\.)?\\d+)\\s*=\\s*(\'|\")"
            +
            "[+-]?(?:\\d*\\.)?\\d+\\s*\\7|(\'|\")([+-]?(?:\\d*\\.)?\\d+)\\s*\\8\\s*=\\s*[+-]?(?:\\d*\\.)?\\d+)";

  public static final String CWF_DISABLE_CONST_EXP_REGEX = "((?:where|or)\\s+)(not\\s+)?(false|true|(\'|\")([+-]?\\d+(\\.\\d+)?).*\\4)";
  public static final String CWF_DISABLE_IN_EXP_REGEX = "(((\'|\")[^\']*\\3\\s*)|[\\d\\.+-]+\\s*)\\s+IN\\s+\\(.*\\)";

  public static final String CWF_DISABLE_SQL_KEY_REGEX = "\\b(exec|insert|delete|update|join|union|master|truncate|"
            +
            "sleep|benchmark|extractvalue|updatexml|ST_LatFromGeoHash|ST_LongFromGeoHash|"
            +
            "GTID_SUBSET|GTID_SUBTRACT|floor|ST_Pointfromgeohash|geometrycollection|multipoint|"
            +
            "polygon|multipolygon|linestring|multilinestring)\\b";

  /**
    * @param regex regex
     * @param flags  {@link Pattern#compile(String, int)}
     * @param input sql String
     * @return Matcher
     */
  public static Matcher regexMatcher(String regex, int flags, String input) {
    return Pattern.compile(regex, flags).matcher(input);
  }

  public static void checkMatchConst(String where) {
    Matcher matcher = regexMatcher(CWF_DISABLE_CONST_EXP_REGEX,
                Pattern.CASE_INSENSITIVE,
              where);
    while (matcher.find()) {
      boolean not = null != matcher.group(2);
      String g3 = matcher.group(3);
      Boolean isTrue;
      if (g3.equalsIgnoreCase("true")) {
        isTrue = true;
      } else if (g3.equalsIgnoreCase("false")) {
        isTrue = false;
      } else {
        String g5 = matcher.group(5);
        isTrue = 0 != Double.valueOf(g5);
      }
      if (not) {
        isTrue = ! isTrue;
      }
      if (isTrue) {
        throw new HoodieException(String.format("Illegal WHERE const true  expression '%s'",matcher.group()));
      }
    }
  }

  /**
     *Check if the input string specifies the specified regular expression. If a match is found, throw {@link IllegalArgumentException} exception
     * @param regex regex
     * @param flags  {@link Pattern#compile(String, int)}
     * @param input sql String
     * @param errMsg err msg
     */
  public static void checkMatchFind(String regex, int flags, String input, String errMsg) {
    Matcher matcher = regexMatcher(regex, flags, input);
    if (matcher.find()) {
      throw new HoodieException(String.format(" conditions:%s, please check the where condition of the SQL statement as it carries the risk of SQL injection."
              + errMsg + " '%s'", input, matcher.group()));
    }
  }

  public static String checkWhere(String where) {
    where = null == where ? "" : where.trim();
    if (!where.isEmpty()) {
      if (!where.toUpperCase().startsWith("WHERE")) {
        throw new HoodieException("WHERE expression must start with 'WHERE'(case insensitive)");
      }

      checkMatchFind(CWF_DISABLE_EQUATION_EXP_REGEX,
          0,
           where,
           "Illegal where equation expression");

      checkMatchConst(where);

      checkMatchFind(CWF_DISABLE_IN_EXP_REGEX,
                      Pattern.CASE_INSENSITIVE,
                    where,
                    "Illegal in expression");


      String nonestr = where.replaceAll("(\'[^\']*\'|\"[^\"]*\")", "");
      checkMatchFind(CWF_DISABLE_SQL_KEY_REGEX,
                      Pattern.CASE_INSENSITIVE,
                    nonestr,
                    "Illegal SQL key");
    }
    return where;
  }
}
