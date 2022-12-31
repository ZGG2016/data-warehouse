package org.zgg.gmall.app.func;

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {

    String getKey(T input);

    void join(T input, JSONObject dimInfo);
}
