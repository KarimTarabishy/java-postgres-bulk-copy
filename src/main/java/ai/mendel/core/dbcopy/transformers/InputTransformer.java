package ai.mendel.core.dbcopy.transformers;

import ai.mendel.core.dbcopy.input.InputItem;

import java.util.function.Function;

public class InputTransformer {
    private int columnIndex;
    private Function<String, String> transformer;

    public InputTransformer(int columnIndex, Function<String, String> transformer) {
        this.columnIndex = columnIndex;
        this.transformer = transformer;
    }

    public void transform(InputItem item){
        item.getData().set(columnIndex, this.transformer.apply(item.getData().get(columnIndex)));
    }
}
