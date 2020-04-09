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




public class BulkCopy {
    private static final Logger logger = Logger.getLogger(BulkCopy.class.getName());
    private static final String CONSTRAINTS_FILE = "constraints.txt", INDEX_FILE = "index.txt";
    private final BulkCopyBuilder configs;
    BulkCopy(BulkCopyBuilder configs){
        this.configs = configs;
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

        BulkCopy bulkCopy = new BulkCopyBuilder(
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
