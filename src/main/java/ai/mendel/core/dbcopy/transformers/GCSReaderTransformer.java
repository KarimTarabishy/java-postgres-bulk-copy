package ai.mendel.core.dbcopy.transformers;

import ai.mendel.core.utils.GCStorage;

import java.util.function.Function;

public class GCSReaderTransformer implements Function<String, String> {
    @Override
    public String apply(String s) {
        return new GCStorage().object(s).getContent(true);
    }
}
