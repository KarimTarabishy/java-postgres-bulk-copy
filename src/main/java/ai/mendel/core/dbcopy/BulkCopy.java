package ai.mendel.core.dbcopy;


import ai.mendel.core.dbcopy.input.IndexToColumnMapper;
import ai.mendel.core.dbcopy.transformers.InputTransformer;
import ai.mendel.core.utils.DataStore;
import ai.mendel.core.utils.GCStorage;

import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;

class BulkCopyBuilder {
    String inputTSVPath, tableName, outputPath, url, user, password;
    String nullString = null;
    boolean copyAsTSV, dropConstraints = false;
    int threadsMultiplier = 1;
    String constrainsAddQuery = null;
    ArrayList<InputTransformer> inputTransformers = new ArrayList<>();
    ArrayList<String> escapedColumns = new ArrayList<>();
    ArrayList<IndexToColumnMapper> fieldsMapper = null;
    String createQuery = null;
    Integer pointerTSVColIndex = null;
    Function<String, String> pointerTSVColTransform = null;
    public BulkCopyBuilder(String outputPath, String inputTSVPath, String tableName, boolean copyAsTSV,
                           String url, String user, String password){
        this.inputTSVPath = inputTSVPath;
        this.tableName = tableName;
        this.outputPath = outputPath;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    /**
     * Adds a transformation to apply on input column values
     * @param inputColumnIndex index of the input column
     * @param transformer transformation
     */
    public BulkCopyBuilder addInputTransformer(int inputColumnIndex, Function<String, String> transformer){
        inputTransformers.add(new InputTransformer(inputColumnIndex, transformer));
        return this;
    }

    /**
     * Thread multiplier is used to increase the amount of threads opened for transformations, this can be
     * really helpful when having transformations reading from google storage.
     * @param multiplier number to multiple by available cores.
     */
    public BulkCopyBuilder setThreadsMultiplier(int multiplier){
        this.threadsMultiplier = multiplier;
        return this;
    }

    /**
     * Used to select specific fields from the input data tsv and relate them
     * to db columns name, if not specified then order of elements in input data tsv should match that in the database.
     * @param fieldsMapper mappers from index columns in input to database column names
     */
    public BulkCopyBuilder selectFields(ArrayList<IndexToColumnMapper> fieldsMapper){
        this.fieldsMapper = fieldsMapper;
        return this;
    }

    /**
     * Adds a create query, indicating that the table should be dropped and recreated each time. This can be
     * beneficial if database doesnt have the table already.
     * @param query database create query.
     */
    public BulkCopyBuilder addCreateQuery(String query){
        this.createQuery = query;
        return this;
    }

    /**
     * indicates which database columns needs to be escaped, this will only execute if selectFields is used and mappers
     * are given from input columns to database columns.
     * @param columnNames list of database column names to escape
     * @return
     */
    public BulkCopyBuilder addEscapedColumns(List<String> columnNames){
        this.escapedColumns.addAll(columnNames);
        return this;
    }

    /**
     * Treats input tsv as just a point to the actual data tsv. This indicates that actual data should be fetched from
     * storage.
     * @param pointerTSVColIndex the column index in the pointer tsv that points to the actual data tsv
     */
    public BulkCopyBuilder treatInputTSVAsPointer(int pointerTSVColIndex){
        return treatInputTSVAsPointer(pointerTSVColIndex, (s-> s));
    }

    /**
     * Treats input tsv as just a pointer to the actual data tsv. This indicates that actual data should be fetched from
     * storage.
     * @param pointerTSVColIndex the column index in the pointer tsv that points to the actual data tsv
     * @param preTransform any transformation to do on this value before fetching from storage
     */
    public BulkCopyBuilder treatInputTSVAsPointer(int pointerTSVColIndex, Function<String, String> preTransform){
        this.pointerTSVColIndex = pointerTSVColIndex;
        this.pointerTSVColTransform = preTransform;
        return this;
    }

    /**
     * Used to enable dropping constraints and indices before copy then recreating them afterwards.
     * You should use this carefully as this wont work if the indices or primary keys of the table has
     * any dependencies, in this instance you might be able to delete tables dependent tables if you can.
     */
    public BulkCopyBuilder dropConstraints(){
        this.dropConstraints = true;
        return this;
    }

    /**
     * Set the string to be used as NULL database value when found in input data.
     * @param nullString the null string
     */
    public BulkCopyBuilder setNullString(String nullString){
        this.nullString = nullString;
        return this;
    }
    public BulkCopy build(){
        return new BulkCopy(this);
    }
}


public class BulkCopy {
    private static final Logger logger = Logger.getLogger(BulkCopy.class.getName());
    private static final String CONSTRAINTS_FILE = "constraints.txt", INDEX_FILE = "index.txt";
    private final BulkCopyBuilder configs;
    BulkCopy(BulkCopyBuilder configs){
        this.configs = configs;
    }

    public static BulkCopyBuilder build(String outputPath, String inputTSVPath, String tableName,
                                          boolean copyAsTSV, String url, String user, String password){
        return new BulkCopyBuilder(outputPath, inputTSVPath, tableName, copyAsTSV, url, user, password);
    }

    /**
     * Drop some tables. This is specially handy if you are going to be copying multiple tables that depends on each
     * other. So you can delete all of them in the beginning og your copy process so as to be able to use dropConstaints
     * without errors. You need to make sure to delete table in correct order so no dependency errors occur.
     * @param tableNames list of table names
     * @throws SQLException
     */
    public void dropTables(List<String> tableNames) throws SQLException {
        try(DataStore store = new DataStore(getMultiQueryURL(configs.url), configs.user,
                configs.password, 3)){
            store.retriableSqlBlock(conn -> {
                try(Statement stmt = conn.createStatement()){
                    stmt.execute(tableNames.stream().map(Queries::drop).collect(Collectors.joining(";")));
                }
            });
        }
    }

    /**
     * Start the copy process.
     * @throws SQLException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void copy() throws SQLException, ExecutionException, InterruptedException {
        if(configs.createQuery != null && !configs.createQuery.isEmpty()){
            logger.info("Dropping and recreating table...");
            recreateTable();
        }
        else{
            logger.info("Truncating table...");
            truncateTable();
        }

        if(configs.dropConstraints) {
            logger.info("Dropping constraints and indices...");
            dropConstraints();
        }

        logger.info("Start  table copy...");
        CopyManager.startCopyToTable(configs);

        if(configs.dropConstraints){
            logger.info("restoring constraints and indices");
            restoreConstraints();
        }
        logger.info("Done");
    }

    private void restoreConstraints() throws SQLException {
        GCStorage storage = new GCStorage();
        String constraints = storage.object(
                GCStorage.fixURI(Paths.get(configs.outputPath, CONSTRAINTS_FILE).toString()))
                .getContent(true);
        String indices = storage.object(
                GCStorage.fixURI(Paths.get(configs.outputPath, INDEX_FILE).toString()))
                .getContent(true);

        try(DataStore store = new DataStore(getMultiQueryURL(configs.url), configs.user, configs.password, 3)){
            store.retriableSqlBlock(conn -> {
                try (Statement stmt = conn.createStatement()) {
                   stmt.execute(constraints);
                }
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(indices);
                }
            });
        }

    }

    private void dropConstraints() throws SQLException {

        try(DataStore store = new DataStore(getMultiQueryURL(configs.url), configs.user, configs.password, 3)){
           saveConstraints(store);
            store.retriableSqlBlock(conn -> {
                StringBuilder builder = new StringBuilder();
                try(Statement stmt = conn.createStatement()){
                    ResultSet resultSet = stmt.executeQuery(Queries.dropConstraints(configs.tableName,
                            "cmd"));
                    while(resultSet.next()){
                        builder.append(resultSet.getString("cmd"));
                    }
                }
                try(Statement stmt = conn.createStatement()){
                    stmt.execute(builder.toString());
                }

                builder = new StringBuilder();
                try(Statement stmt = conn.createStatement()){
                    ResultSet resultSet = stmt.executeQuery(Queries.dropIndices(configs.tableName,
                            "cmd"));
                    while(resultSet.next()){
                        builder.append(resultSet.getString("cmd"));
                    }
                }
                try(Statement stmt = conn.createStatement()){
                    stmt.execute(builder.toString());
                }


            });
        }
    }

    private void saveConstraints(DataStore store) throws SQLException {
        GCStorage storage = new GCStorage();
        // try to fetch already
        boolean constraintsExists = storage.object(
                GCStorage.fixURI(Paths.get(configs.outputPath, CONSTRAINTS_FILE).toString()))
                .exists(true);
        boolean indicesExists = storage.object(
                GCStorage.fixURI(Paths.get(configs.outputPath, INDEX_FILE).toString()))
                .exists(true);

        // if constraints not found then save them to gs
        if(!constraintsExists){
            store.retriableSqlBlock(conn -> {
                try(Statement stmt = conn.createStatement()){
                    ResultSet resultSet = stmt.executeQuery(Queries.createconstraints(configs.tableName,
                            "cmd"));
                    StringBuilder builder = new StringBuilder();
                    while(resultSet.next()){
                        builder.append(resultSet.getString("cmd"));
                    }
                    storage.object(
                            GCStorage.fixURI(Paths.get(configs.outputPath, CONSTRAINTS_FILE).toString())
                    ).writeUTF8(builder.toString(), true);
                }
            });
        }

        if(!indicesExists){
            store.retriableSqlBlock(conn -> {
                try(Statement stmt = conn.createStatement()){
                    ResultSet resultSet = stmt.executeQuery(Queries.createIndices(configs.tableName,
                            "cmd"));
                    StringBuilder builder = new StringBuilder();
                    while(resultSet.next()){
                        builder.append(resultSet.getString("cmd"));
                    }
                    storage.object(
                            GCStorage.fixURI(Paths.get(configs.outputPath, INDEX_FILE).toString())
                    ).writeUTF8(builder.toString(), true);
                }
            });
        }
    }

    private void truncateTable() throws SQLException {
        try(DataStore store = new DataStore(configs.url, configs.user, configs.password, 3)){
            store.retriableSqlBlock(conn -> {
                try(Statement stmt = conn.createStatement()){
                    stmt.execute(Queries.truncate(configs.tableName));
                }
            });
        }
    }

    private void recreateTable() throws SQLException {
        try(DataStore store = new DataStore(configs.url, configs.user, configs.password, 3)){
            store.retriableSqlBlock(conn -> {

                try(Statement stmt = conn.createStatement()){
                    stmt.execute(Queries.drop(configs.tableName));
                }

                try(Statement stmt = conn.createStatement()){
                    stmt.execute(configs.createQuery+";");
                }
            });
        }
    }

    private static String getMultiQueryURL(String url){
        return url + "?allowMultiQueries=true";
    }

    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {

        String url = args[0], user = args[1], password = args[2];
        BulkCopy bulkCopy = BulkCopy.build(
                "gs://pipeline_output/expert/3-5-2020/test_new_copy",
                "gs://pipeline_output/expert/3-5-2020/clues-ke-7/listing.tsv",
                "clues_ke.entities",
                true,
                url, user, password)
                .addCreateQuery("create table if not exists clues_ke.entities\n" +
                        "(\n" +
                        "\tpatient_id bigint not null\n" +
                        "\t\tconstraint entities_patient_id_fk\n" +
                        "\t\t\treferences emr.patient,\n" +
                        "\tmedical_record_id bigint not null\n" +
                        "\t\tconstraint entities_medical_record_id_fk\n" +
                        "\t\t\treferences emr.medical_record,\n" +
                        "\tid varchar(255) not null\n" +
                        "\t\tconstraint entities_pk\n" +
                        "\t\t\tprimary key,\n" +
                        "\ttype varchar not null,\n" +
                        "\ttext text not null,\n" +
                        "\tstart integer not null,\n" +
                        "\t\"end\" integer not null\n" +
                        ");\n" +
                        "\n" +
                        "\n" +
                        "create index if not exists entities_type_index\n" +
                        "\ton clues_ke.entities (type);\n" +
                        "\n" +
                        "create index if not exists entities_medical_record_id_index\n" +
                        "\ton clues_ke.entities (medical_record_id);\n" +
                        "\n" +
                        "create index if not exists entities_patient_id_index\n" +
                        "\ton clues_ke.entities (patient_id);\n" +
                        "\n")
                .treatInputTSVAsPointer(2,
                        s -> GCStorage.fixURI(Paths.get(s, "entities.tsv").toString()))
                .selectFields(new ArrayList<>(Arrays.asList(
                        new IndexToColumnMapper(0, "patient_id"),
                        new IndexToColumnMapper(1, "medical_record_id"),
                        new IndexToColumnMapper(2, "id"),
                        new IndexToColumnMapper(3, "type"),
                        new IndexToColumnMapper(4, "text"),
                        new IndexToColumnMapper(5, "start"),
                        new IndexToColumnMapper(6, "\"end\"")
                )))
                .dropConstraints()
                .setNullString("null")
                .setThreadsMultiplier(6)
                .build();

        bulkCopy.dropTables(Arrays.asList("clues_ke.temporal", "clues_ke.spatial", "clues_ke.object_properties",
                "clues_ke.objects", "clues_ke.relations", "clues_ke.entities"));
        bulkCopy.copy();
    }



}
