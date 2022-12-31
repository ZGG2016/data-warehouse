package org.zgg.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;

public class LogUtils {
    public static boolean validateStart(String log) {
        if (log == null){
            return false;
        }
        // 校验 json
        return log.trim().startsWith("{") && log.trim().endsWith("}");
    }

    public static boolean validateEvent(String log) {
        // 服务器时间 | json
        // 1 切割
        String[] logContents = log.split("\\|");
        // 2 校验
        if(logContents.length != 2){
            return false;
        }
        //3 校验服务器时间
        if (logContents[0].length()!=13 || !NumberUtils.isDigits(logContents[0])){
            return false;
        }
        // 4 校验 json
        return logContents[1].trim().startsWith("{") && logContents[1].trim().endsWith("}");
    }
}
