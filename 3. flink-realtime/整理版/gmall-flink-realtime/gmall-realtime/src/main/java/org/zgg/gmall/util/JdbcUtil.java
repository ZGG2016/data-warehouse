package org.zgg.gmall.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 当前工具类可以适用于任何JDBC方式访问的数据库中的任何查询语句
 * 单行单列：select count(*) from t;
 * 单行多列：select * from t where id='1001'; id为主键
 * 多行单列：select name from t;
 * 多行多列：select * from t;
 * Map<String,List<Object>>
 * List<JSON>
 * List<Map>
 */
public class JdbcUtil {

    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clz, boolean underScoreToCamel) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {

        //创建集合用于存放结果数据
        ArrayList<T> result = new ArrayList<>();

        //编译SQL语句
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();

        //获取查询的元数据信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        //遍历结果集,将每行数据转换为T对象并加入集合   行遍历
        while (resultSet.next()) {

            //创建T对象
            T t = clz.newInstance();

            //列遍历,并给T对象赋值
            for (int i = 0; i < columnCount; i++) {

                //获取列名与列值
                String columnName = metaData.getColumnName(i + 1);
                Object value = resultSet.getObject(columnName);

                //判断是否需要进行下划线与驼峰命名转换
                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                //赋值
                BeanUtils.setProperty(t, columnName, value);
            }

            //将T对象放入集合
            result.add(t);
        }

        resultSet.close();
        preparedStatement.close();

        //返回集合
        return result;
    }

    public static void main(String[] args) throws Exception {

        DruidDataSource dataSource = DruidDSUtil.createDataSource();
        DruidPooledConnection connection = dataSource.getConnection();

        List<JSONObject> jsonObjects = queryList(connection,
                "select * ct from GMALL211027_REALTIME.DIM_BASE_TRADEMARK where id='1'",
                JSONObject.class,
                true);

        for (JSONObject jsonObject : jsonObjects) {
            System.out.println(jsonObject);
        }

        connection.close();

    }

}
