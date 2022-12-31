package org.zgg.gmall.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.zgg.gmall.bean.TableProcess;
import org.zgg.gmall.common.GmallConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection connection;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //value:{"before":null,"after":{"source_table":"aa","sink_table":"bb","sink_columns":"cc","sink_pk":"id","sink_extend":"xxx"},
    // "source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1652513039549,"snapshot":"false",
    // "db":"gmall-211126-config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,
    // "thread":null,"query":null},"op":"r","ts_ms":1652513039551,"transaction":null}
    @Override
    public void processBroadcastElement(String s, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

        //1.获取并解析数据
        JSONObject jsonObject = JSON.parseObject(s);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //2.校验并建表
        checkTable(tableProcess.getSinkTable(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkExtend());

        //3.写入状态,广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(), tableProcess);

    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend){
        PreparedStatement preparedStatement = null;

        try {
            //处理特殊字段
            if (sinkPk == null || "".equals(sinkPk)) {
                sinkPk = "id";
            }

            if (sinkExtend == null) {
                sinkExtend = "";
            }

            //拼接SQL:create table if not exists db.tn(id varchar primary key,bb varchar,cc varchar) xxx
            StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                //取出字段
                String column = columns[i];
                //判断是否为主键
                if (sinkPk.equals(column)) {
                    createTableSql.append(column).append(" varchar primary key");
                } else {
                    createTableSql.append(column).append(" varchar");
                }
                //判断是否为最后一个字段
                if (i < columns.length - 1) {
                    createTableSql.append(",");
                }
            }

            createTableSql.append(")").append(sinkExtend);

            //编译SQL
            System.out.println("建表语句为：" + createTableSql);
            preparedStatement = connection.prepareStatement(createTableSql.toString());

            //执行SQL,建表
            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException("建表失败：" + sinkTable);
        } finally {
            //释放资源
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //value:{"database":"gmall-211126-flink","table":"base_trademark","type":"update","ts":1652499176,"xid":188,"commit":true,
    // "data":{"id":13,"tm_name":"atguigu","logo_url":"/bbb/bbb"},"old":{"logo_url":"/aaa/aaa"}}
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

        //1.获取广播的配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String table = jsonObject.getString("table");
        TableProcess tableProcess = broadcastState.get(table);

        if (tableProcess != null){
            //2.过滤字段
            filterColumn(jsonObject.getJSONObject("data"), tableProcess.getSinkColumns());

            //3.补充SinkTable并写出到流中
            jsonObject.put("sinkTable", tableProcess.getSinkTable());
            collector.collect(jsonObject);
        }
    }

    private void filterColumn(JSONObject data, String sinkColumns){
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !columnList.contains(next.getKey()));
    }

}
