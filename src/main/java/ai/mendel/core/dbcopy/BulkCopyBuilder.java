package ai.mendel.core.dbcopy;

import ai.mendel.core.dbcopy.input.IndexToColumnMapper;
import ai.mendel.core.dbcopy.transformers.InputTransformer;
import ai.mendel.core.utils.GCStorage;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.properties.EncryptableProperties;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

public class BulkCopyBuilder {
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