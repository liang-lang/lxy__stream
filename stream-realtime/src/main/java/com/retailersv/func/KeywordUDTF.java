package com.retailersv.func;

import com.stream.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;


@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String test) {
        for (String keyword : KeywordUtil.analyze(test)) {
            collect(Row.of(keyword));
        }
    }
}
