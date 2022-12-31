package org.zgg.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {
    public String evaluate(String line, String key) throws JSONException {
        String[] log = line.split("\\|");

        if (log.length != 2 || StringUtils.isBlank(log[1])) {
            return "";
        }
        String result = "";
        JSONObject jsonObject = new JSONObject(log[1].trim());

        // 获取服务器时间
        if ("st".equals(key)) {
            result = log[0].trim();
        } else if ("et".equals(key)) {
            // 获取事件数组
            if (jsonObject.has("et")) {
                result = jsonObject.getString("et");
            }
        } else {
            JSONObject cm = jsonObject.getJSONObject("cm");
            // 获取 key 对应公共字段的 value
            if (cm.has(key)) {
                result = cm.getString(key);
            }
        }
        return result;
    }

    // 测试
    public static void main(String[] args) {
        String line = "1663668007041|{\"cm\":{\"ln\":\"-102.1\",\"sv\":\"V2.8.2\",\"os\":\"8.1.4\",\"g\":\"Z6Q4O8C1@gmail.com\",\"mid\":\"2\",\"nw\":\"4G\",\"l\":\"es\",\"vc\":\"16\",\"hw\":\"640*1136\",\"ar\":\"MX\",\"uid\":\"2\",\"t\":\"1663593171908\",\"la\":\"-42.5\",\"md\":\"HTC-15\",\"vn\":\"1.3.2\",\"ba\":\"HTC\",\"sr\":\"H\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1663655211488\",\"en\":\"display\",\"kv\":{\"goodsid\":\"0\",\"action\":\"1\",\"extend1\":\"2\",\"place\":\"5\",\"category\":\"92\"}},{\"ett\":\"1663573907700\",\"en\":\"newsdetail\",\"kv\":{\"entry\":\"2\",\"goodsid\":\"1\",\"news_staytime\":\"6\",\"loading_time\":\"10\",\"action\":\"1\",\"showtype\":\"0\",\"category\":\"52\",\"type1\":\"325\"}},{\"ett\":\"1663599878859\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"12\",\"action\":\"1\",\"extend1\":\"\",\"type\":\"3\",\"type1\":\"\",\"loading_way\":\"1\"}},{\"ett\":\"1663641341983\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1663643754584\",\"action\":\"3\",\"type\":\"4\",\"content\":\"\"}},{\"ett\":\"1663624769593\",\"en\":\"error\",\"kv\":{\"errorDetail\":\"at cn.lift.dfdfdf.control.CommandUtil.getInfo(CommandUtil.java:67)\\\\n at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\\\n at java.lang.reflect.Method.invoke(Method.java:606)\\\\n\",\"errorBrief\":\"at cn.lift.appIn.control.CommandUtil.getInfo(CommandUtil.java:67)\"}},{\"ett\":\"1663605244013\",\"en\":\"comment\",\"kv\":{\"p_comment_id\":4,\"addtime\":\"1663662857848\",\"praise_count\":168,\"other_id\":0,\"comment_id\":9,\"reply_count\":98,\"userid\":2,\"content\":\"亏贾河晕跋袭谊\"}},{\"ett\":\"1663597493600\",\"en\":\"praise\",\"kv\":{\"target_id\":4,\"id\":4,\"type\":1,\"add_time\":\"1663570034907\",\"userid\":1}}]}";
        String mid = new BaseFieldUDF().evaluate(line, "mid");
        System.out.println(mid);
    }

}
