package ai.mendel.core.dbcopy;


import ai.mendel.core.dbcopy.input.IndexToColumnMapper;
import ai.mendel.core.dbcopy.transformers.InputTransformer;
import ai.mendel.core.utils.DataStore;
import ai.mendel.core.utils.GCStorage;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.properties.EncryptableProperties;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.logging.Logger;

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
    public BulkCopyBuilder(String outputPath, String inputTSVPath, String tableName, boolean copyAsTSV){
        this.inputTSVPath = inputTSVPath;
        this.tableName = tableName;
        this.outputPath = outputPath;
        this.copyAsTSV = copyAsTSV;
    }

    public BulkCopyBuilder setDBCredentials(String url, String user, String password){
        this.url = url;
        this.user = user;
        this.password = password;
        return this;
    }

    /**
     * Get database credentials from a propertiesfile on storage
     * @param gcs_property_file_path fully qualified path for property file
     * @param property_name database prefix property name
     * @param gcs_key_path fully qualified path for decryption key file used to decrypt password properties
     * @throws IOException
     */
    public BulkCopyBuilder setDBCredentialsFromPropertyFile(String gcs_property_file_path, String property_name,
                                                            String gcs_key_path) throws IOException {
        GCStorage storage = new GCStorage();
        String key =  storage.object(gcs_key_path).getContent(true);
        if (key == null) {
            throw new RuntimeException("Couldn't find key file: " + gcs_key_path);
        }

        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
        encryptor.setPassword(key);
        Properties props = new EncryptableProperties(encryptor);

        String properties_file = storage.object(gcs_property_file_path).getContent(true);

        if (properties_file == null) {
            throw new RuntimeException("Couldn't find properties file: " + gcs_property_file_path);
        }

        InputStream properties_stream = new ByteArrayInputStream(properties_file.getBytes(StandardCharsets.UTF_8));
        props.load(properties_stream);
        url = props.getProperty(property_name + ".url");
        user = props.getProperty(property_name + ".user");
        password = props.getProperty(property_name + ".pass");
        return this;
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
    public BulkCopyBuilder setEscapedColumns(List<String> columnNames){
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
                                          boolean copyAsTSV){
        return new BulkCopyBuilder(outputPath, inputTSVPath, tableName, copyAsTSV);
    }

    /**
     * Drop some tables. This is specially handy if you are going to be copying multiple tables that depends on each
     * other. So you can delete all of them in the beginning og your copy process so as to be able to use dropConstaints
     * without errors. You need to make sure to delete table in correct order so no dependency errors occur.
     * @param tableNames list of table names
     * @throws SQLException
     */
    public void dropTables(List<String> tableNames) throws SQLException {
        try(DataStore store = new DataStore(configs.url, configs.user,
                configs.password, 3)){
            store.retriableSqlBlock(conn -> {
                for(String tn : tableNames) {
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute(Queries.drop(tn));
                    }
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

        try(DataStore store = new DataStore(configs.url, configs.user, configs.password, 0)){
            store.retriableSqlBlock(conn -> {
                ArrayList<String> cmds = new ArrayList<>(Arrays.asList(constraints.split(";")));
                cmds.addAll(Arrays.asList(indices.split(";")));

                for(String cmd : cmds) {
                    try (Statement stmt = conn.createStatement()) {
                        logger.info("Restoring Constraints: "+ cmd);
                        stmt.execute(cmd);
                    }
                }
            });
        }
    }

    private void dropConstraints() throws SQLException {

        try(DataStore store = new DataStore(configs.url, configs.user, configs.password, 3)){
           saveConstraints(store);
            store.retriableSqlBlock(conn -> {
                ArrayList<String> cmds = new ArrayList<>();
                try(Statement stmt = conn.createStatement()){
                    ResultSet resultSet = stmt.executeQuery(Queries.dropConstraints(configs.tableName,
                            "cmd"));
                    while(resultSet.next()){
                        cmds.add(resultSet.getString("cmd"));
                    }
                }
                try(Statement stmt = conn.createStatement()){
                    ResultSet resultSet = stmt.executeQuery(Queries.dropIndices(configs.tableName,
                            "cmd"));
                    while(resultSet.next()){
                        cmds.add(resultSet.getString("cmd"));
                    }
                }
                for(String cmd : cmds) {
                    logger.info("Drop contraints: " + cmd);
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute(cmd);
                    }
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
                        builder.append(resultSet.getString("cmd").replace("DROP CONSTRAINT", "DROP CONSTRAINT IF EXISTS"));
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


    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {

        String url = args[0], user = args[1], password = args[2];

        BulkCopy bulkCopy = BulkCopy.build(
            "gs://pipeline_output/expert/3-5-2020/test_new_copy/relations",
            "gs://pipeline_output/expert/3-5-2020/clues-ke-8/listing.tsv",
            "clues_ke.objects",
            true)
            .setDBCredentials(url, user, password)
            .addCreateQuery("create table if not exists clues_ke.relations\n" +
                    "(\n" +
                    "\tid bigserial not null\n" +
                    "\t\tconstraint relations_pk\n" +
                    "\t\t\tprimary key,\n" +
                    "\tparent_entity_id varchar not null\n" +
                    "\t\tconstraint relations_entities_id_fk\n" +
                    "\t\t\treferences clues_ke.entities,\n" +
                    "\tparent_entity_type varchar not null,\n" +
                    "\trelation_type varchar not null,\n" +
                    "\tchild_entity_id varchar not null\n" +
                    "\t\tconstraint relations_entities_id_fk_2\n" +
                    "\t\t\treferences clues_ke.entities,\n" +
                    "\tchild_entity_type varchar not null\n" +
                    ");\n" +
                    "\n" +
                    "\n" +
                    "create index if not exists relations_child_entity_id_index\n" +
                    "\ton clues_ke.relations (child_entity_id);\n" +
                    "\n" +
                    "create index if not exists relations_child_entity_type_index\n" +
                    "\ton clues_ke.relations (child_entity_type);\n" +
                    "\n" +
                    "create index if not exists relations_parent_entity_id_index\n" +
                    "\ton clues_ke.relations (parent_entity_id);\n" +
                    "\n" +
                    "create unique index if not exists relations_parent_entity_id_relation_type_child_entity_id_uindex\n" +
                    "\ton clues_ke.relations (parent_entity_id, relation_type, child_entity_id);\n" +
                    "\n" +
                    "create index if not exists relations_parent_entity_type_index\n" +
                    "\ton clues_ke.relations (parent_entity_type);\n")
            .treatInputTSVAsPointer(2,
                    s -> GCStorage.fixURI(Paths.get(s, "relations.tsv").toString()))
            .selectFields(new ArrayList<>(Arrays.asList(
                    new IndexToColumnMapper(0, "entity_id"),
                    new IndexToColumnMapper(1, "id"),
                    new IndexToColumnMapper(2, "name")
            )))
            .dropConstraints()
            .setNullString("\\N")
            .setThreadsMultiplier(6)
            .build();

//        BulkCopy bulkCopy = BulkCopy.build(
//                "gs://pipeline_output/expert/3-5-2020/test_new_copy/objects",
//                "gs://pipeline_output/expert/3-5-2020/clues-ke-8/listing.tsv",
//                "clues_ke.objects",
//                true,
//                url, user, password)
//                .addCreateQuery("create table if not exists clues_ke.objects\n" +
//                        "(\n" +
//                        "\tid varchar not null\n" +
//                        "\t\tconstraint objects_pk\n" +
//                        "\t\t\tprimary key,\n" +
//                        "\tname varchar not null,\n" +
//                        "\tentity_id varchar\n" +
//                        "\t\tconstraint objects_entities_id_fk\n" +
//                        "\t\t\treferences clues_ke.entities\n" +
//                        ");")
//                .treatInputTSVAsPointer(2,
//                        s -> GCStorage.fixURI(Paths.get(s, "objects.tsv").toString()))
//                .selectFields(new ArrayList<>(Arrays.asList(
//                        new IndexToColumnMapper(0, "entity_id"),
//                        new IndexToColumnMapper(1, "id"),
//                        new IndexToColumnMapper(2, "name")
//                )))
//                .dropConstraints()
//                .setNullString("\\N")
//                .setThreadsMultiplier(6)
//                .build();
//
//        bulkCopy.copy();
//
//        logger.info("objects props");
//
//        bulkCopy = BulkCopy.build(
//                "gs://pipeline_output/expert/3-5-2020/test_new_copy/objects_prop",
//                "gs://pipeline_output/expert/3-5-2020/clues-ke-8/listing.tsv",
//                "clues_ke.object_properties",
//                true,
//                url, user, password)
//                .addCreateQuery("create table if not exists clues_ke.object_properties\n" +
//                        "(\n" +
//                        "\tobject_id varchar not null\n" +
//                        "\t\tconstraint object_properties_objects_id_fk\n" +
//                        "\t\t\treferences clues_ke.objects,\n" +
//                        "\tid bigserial not null\n" +
//                        "\t\tconstraint object_properties_pk\n" +
//                        "\t\t\tprimary key,\n" +
//                        "\tconceme_id varchar,\n" +
//                        "\tvalue varchar not null,\n" +
//                        "\tevidence_start integer not null,\n" +
//                        "\tevidence_end integer not null,\n" +
//                        "\tevidence_entity_id varchar\n" +
//                        "\t\tconstraint object_properties_entities_id_fk\n" +
//                        "\t\t\treferences clues_ke.entities,\n" +
//                        "\tname varchar not null\n" +
//                        ");\n" +
//                        "\n" +
//                        "\n" +
//                        "create index if not exists object_properties_pk_5\n" +
//                        "\ton clues_ke.object_properties (conceme_id);\n" +
//                        "\n" +
//                        "create index if not exists object_properties_pk_4\n" +
//                        "\ton clues_ke.object_properties (evidence_entity_id);\n" +
//                        "\n" +
//                        "create index if not exists object_properties_pk_2\n" +
//                        "\ton clues_ke.object_properties (name);\n" +
//                        "\n" +
//                        "create index if not exists object_properties_pk_3\n" +
//                        "\ton clues_ke.object_properties (object_id);\n" +
//                        "\n" +
//                        "create index if not exists objects_entity_id_index\n" +
//                        "\ton clues_ke.objects (entity_id);\n" +
//                        "\n" +
//                        "create index if not exists objects_name_index\n" +
//                        "\ton clues_ke.objects (name);\n")
//                .treatInputTSVAsPointer(2,
//                        s -> GCStorage.fixURI(Paths.get(s, "object_properties.tsv").toString()))
//                .selectFields(new ArrayList<>(Arrays.asList(
//                        new IndexToColumnMapper(0, "object_id"),
//                        new IndexToColumnMapper(1, "name"),
//                        new IndexToColumnMapper(2, "conceme_id"),
//                        new IndexToColumnMapper(3, "value"),
//                        new IndexToColumnMapper(4, "evidence_start"),
//                        new IndexToColumnMapper(5, "evidence_end"),
//                        new IndexToColumnMapper(6, "evidence_entity_id")
//                )))
//                .dropConstraints()
//                .setNullString("\\N")
//                .setThreadsMultiplier(6)
//                .build();
//
//        bulkCopy.copy();
//
//
//        logger.info("spatial");
//        bulkCopy = BulkCopy.build(
//                "gs://pipeline_output/expert/3-5-2020/test_new_copy/objects_prop",
//                "gs://pipeline_output/expert/3-5-2020/t-ke-8/listing.tsv",
//                "clues_ke.object_properties",
//                true,
//                url, user, password)
//                .addCreateQuery("create table if not exists clues_ke.object_properties\n" +
//                        "(\n" +
//                        "\tobject_id varchar not null\n" +
//                        "\t\tconstraint object_properties_objects_id_fk\n" +
//                        "\t\t\treferences clues_ke.objects,\n" +
//                        "\tid bigserial not null\n" +
//                        "\t\tconstraint object_properties_pk\n" +
//                        "\t\t\tprimary key,\n" +
//                        "\tconceme_id varchar,\n" +
//                        "\tvalue varchar not null,\n" +
//                        "\tevidence_start integer not null,\n" +
//                        "\tevidence_end integer not null,\n" +
//                        "\tevidence_entity_id varchar\n" +
//                        "\t\tconstraint object_properties_entities_id_fk\n" +
//                        "\t\t\treferences clues_ke.entities,\n" +
//                        "\tname varchar not null\n" +
//                        ");\n" +
//                        "\n" +
//                        "\n" +
//                        "create index if not exists object_properties_pk_5\n" +
//                        "\ton clues_ke.object_properties (conceme_id);\n" +
//                        "\n" +
//                        "create index if not exists object_properties_pk_4\n" +
//                        "\ton clues_ke.object_properties (evidence_entity_id);\n" +
//                        "\n" +
//                        "create index if not exists object_properties_pk_2\n" +
//                        "\ton clues_ke.object_properties (name);\n" +
//                        "\n" +
//                        "create index if not exists object_properties_pk_3\n" +
//                        "\ton clues_ke.object_properties (object_id);\n" +
//                        "\n" +
//                        "create index if not exists objects_entity_id_index\n" +
//                        "\ton clues_ke.objects (entity_id);\n" +
//                        "\n" +
//                        "create index if not exists objects_name_index\n" +
//                        "\ton clues_ke.objects (name);\n")
//                .treatInputTSVAsPointer(2,
//                        s -> GCStorage.fixURI(Paths.get(s, "object_properties.tsv").toString()))
//                .selectFields(new ArrayList<>(Arrays.asList(
//                        new IndexToColumnMapper(0, "object_id"),
//                        new IndexToColumnMapper(1, "name"),
//                        new IndexToColumnMapper(2, "conceme_id"),
//                        new IndexToColumnMapper(3, "value"),
//                        new IndexToColumnMapper(4, "evidence_start"),
//                        new IndexToColumnMapper(5, "evidence_end"),
//                        new IndexToColumnMapper(6, "evidence_entity_id")
//                )))
//                .dropConstraints()
//                .setNullString("\\N")
//                .setThreadsMultiplier(6)
//                .build();
//
//        bulkCopy.copy();
    }



}
