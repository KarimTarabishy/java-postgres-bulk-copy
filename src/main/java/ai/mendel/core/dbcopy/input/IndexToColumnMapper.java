package ai.mendel.core.dbcopy.input;

public class IndexToColumnMapper {
    private int index;
    private String dbColumnName;

    public IndexToColumnMapper(int index, String dbColumnName) {
        this.index = index;
        this.dbColumnName = dbColumnName;
    }

    public int getInputIndex(){return index;}
    public String getDBColumnName(){return dbColumnName;}
}
