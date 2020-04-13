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
        if(configs.initQuery != null && !configs.initQuery.isEmpty()){
            logger.info("Running init query: " + configs.initQuery);
            try(DataStore store = new DataStore(configs.url, configs.user, configs.password, -1)){
                store.retriableSqlBlock(conn -> {
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute(configs.initQuery);
                    }
                });
            }
        }
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

        try(DataStore store = new DataStore(configs.url, configs.user, configs.password, -1)){
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
                        String r = resultSet.getString("cmd");
                        if(r != null && !r.isEmpty()){
                            cmds.add(r);
                        }
                    }
                }
                try(Statement stmt = conn.createStatement()){
                    ResultSet resultSet = stmt.executeQuery(Queries.dropIndices(configs.tableName,
                            "cmd"));
                    while(resultSet.next()){
                        String r = resultSet.getString("cmd");
                        if(r != null && !r.isEmpty()){
                            cmds.add(r);
                        }
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
        String c = "";
        String t = "sdlksd;";
        ArrayList<String> cmds = new ArrayList<>(Arrays.asList(c.split(";")));
        cmds.addAll(Arrays.asList(t.split(";")));
        for(String cc : cmds){
            System.out.println(cc);
        }

    }



}
