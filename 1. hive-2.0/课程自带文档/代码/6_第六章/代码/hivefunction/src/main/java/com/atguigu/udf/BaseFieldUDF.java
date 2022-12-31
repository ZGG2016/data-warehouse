package com.atguigu.udf;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {

    public String evaluate(String line, String key) throws JSONException {

        // 1按"\\|"对日志line进行切割
        String[] log = line.split("\\|");

        // 2 合法性校验
        if (log.length != 2 || StringUtils.isBlank(log[1])) {
            return "";
        }

        // 3 开始处理JSON
        JSONObject baseJson = new JSONObject(log[1].trim());

        String result = "";

        // 4 根据传进来的key，查找对应的value值
        if ("et".equals(key)) {
            if (baseJson.has("et")) {
                result = baseJson.getString("et");
            }
        } else if ("st".equals(key)) {
            result = log[0].trim();
        } else {
            JSONObject cm = baseJson.getJSONObject("cm");
            if (cm.has(key)) {
                result = cm.getString(key);
            }
        }

        return result;
    }

    }
