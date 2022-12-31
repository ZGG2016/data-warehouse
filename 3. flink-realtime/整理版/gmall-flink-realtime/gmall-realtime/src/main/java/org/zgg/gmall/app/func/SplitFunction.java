package org.zgg.gmall.app.func;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.zgg.gmall.util.KeywordUtil;

import java.io.IOException;
import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {
    public void eval(String str) {

        List<String> list = null;
        try {
            list = KeywordUtil.splitKeyword(str);
            for (String word : list) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            collect(Row.of(str));
        }
    }
}
