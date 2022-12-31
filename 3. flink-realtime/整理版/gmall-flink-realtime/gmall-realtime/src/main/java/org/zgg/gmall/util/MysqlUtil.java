package org.zgg.gmall.util;

public class MysqlUtil {
    public static String getBaseDicLookUpDDL() {
        return "create table `base_dic`( " +
                "`dic_code` string, " +
                "`dic_name` string, " +
                "`parent_code` string, " +
                "`create_time` timestamp, " +
                "`operate_time` timestamp, " +
                "primary key(`dic_code`) not enforced " +
                ")" + MysqlUtil.mysqlLookUpTableDDL("base_dic");
    }

    public static String mysqlLookUpTableDDL(String tableName){
        return " with ( " +
                " 'connector' = 'jdbc', " +
                " 'url' = 'jdbc:mysql://bigdata102:3306/gmall-flink', " +
                " 'table-name' = " + tableName + "'," +
                " 'lookup.cache.max-rows' = '10'," +
                " 'lookup.cache.ttl' = '1 hour'," +
                " 'username' = 'root'," +
                " 'password' = 'root'," +
                " 'driver' = 'com.mysql.cj.jdbc.Driver' " +
                ")";
    }
}
