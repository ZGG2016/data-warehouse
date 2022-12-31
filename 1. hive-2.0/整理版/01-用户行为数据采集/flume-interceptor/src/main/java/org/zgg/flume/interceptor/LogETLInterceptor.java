package org.zgg.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class LogETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        // 1.获取数据
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);

        // 2. 根据日志类型，分别进行验证
        if (log.contains("start")){
            // 验证启动日志
            if (LogUtils.validateStart(log)){
                return event;
            }
        }else{
            // 验证事件日志
            if (LogUtils.validateEvent(log)){
                return event;
            }
        }
        // 3. 返回校验结果
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        ArrayList<Event> interceptors = new ArrayList<>();
        for (Event event:events){
            Event intercept = intercept(event);
            if (intercept != null) {
                interceptors.add(intercept);
            }
        }
        return interceptors;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
