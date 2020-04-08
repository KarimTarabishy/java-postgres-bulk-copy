package ai.mendel.core.dbcopy.tasks;

import ai.mendel.core.dbcopy.input.IndexToColumnMapper;
import ai.mendel.core.dbcopy.input.InputItem;
import ai.mendel.core.utils.MendelUtils;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Copy implements Callable<Long> {
    private final BlockingQueue<InputItem> pendingFiles;
    private final String url, username, password, tableName, nullString;
    private  MendelUtils.VolatileWrapper<Integer> writtenFiles;
    private int inputThreadsCount;
    private AtomicBoolean forceStop;
    private ArrayList<String> escapedColumns;
    private ArrayList<IndexToColumnMapper> selectedColumns;
    private boolean isTSV;


    public Copy(BlockingQueue<InputItem> pendingFiles, ArrayList<String> escapedColumns,
                ArrayList<IndexToColumnMapper> selectedColumns, boolean isTSV, String nullString,
                String url, String username,
                String password, String tableName, int inputThreadsCount,
                MendelUtils.VolatileWrapper<Integer> writtenFiles, AtomicBoolean forceStop) {
        this.pendingFiles = pendingFiles;
        this.isTSV = isTSV;
        this.url = url;
        this.username = username;
        this.password = password;
        this.inputThreadsCount = inputThreadsCount;
        this.writtenFiles = writtenFiles;
        this.forceStop = forceStop;
        this.escapedColumns = escapedColumns;
        this.selectedColumns = selectedColumns;
        this.tableName = tableName;
        this.nullString = nullString;

    }

    @Override
    public Long call() throws SQLException {
        CopyIn copyIn = null;
        long rowsAffected = -1L;
        try (Connection con = DriverManager.getConnection(url, username, password)) {

            copyIn = new CopyManager((BaseConnection) con)
                    .copyIn(getCopyCommand(tableName,
                            Optional.of(selectedColumns).map(s ->
                                    s.stream().map(IndexToColumnMapper::getDBColumnName)
                                            .collect(Collectors.toCollection(ArrayList::new)))
                            .orElse(null),
                            isTSV, nullString
                            ));

            while (true) {
                // if thread is requested to stop, cancel copy and stop it
                if (Thread.currentThread().isInterrupted() || forceStop.get()) {
                    System.out.println("Interrupted");
                    copyIn.cancelCopy();
                    break;
                }
                //try to read data
                InputItem item = null;
                try {
                    item = pendingFiles.poll(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {  // if thread is requested to stop, , cancel copy and stop it
                    Thread.currentThread().interrupt();
                    copyIn.cancelCopy();
                    break;
                }

                if (item == null) {
                    continue;
                }

                if (item.getData() == null){
                    inputThreadsCount--;
                    if (inputThreadsCount <= 0){
                        copyIn.flushCopy();
                        rowsAffected = copyIn.endCopy();
                        break;
                    }
                    continue;
                }
                ArrayList<String> data = item.getData();
                String row = "";
                if(selectedColumns != null && !selectedColumns.isEmpty()){
                    StringBuilder rowBuilder = new StringBuilder();
                    for(IndexToColumnMapper mapper : selectedColumns){
                        String d = data.get(mapper.getInputIndex());
                        if(escapedColumns.contains(mapper.getDBColumnName())){
                            d = d.replaceAll("\u0000", "");
                            if(!isTSV) {
                                d = "\"" + d.replaceAll("\"", "\"\"") + "\"";
                            }else{
                                d = d.replace("\\", "\\\\");
                            }
                        }
                        rowBuilder.append(d).append(isTSV ? '\t':',') ;
                    }
                    rowBuilder.deleteCharAt(row.length()-1);
                    row  = rowBuilder.toString();
                }
                else {
                    row = String.join(isTSV ? "\t":",", data);
                }
                if(row.length() == 0){
                    continue;
                }

                // we have data lets write it
                try {
                    byte[] data_bytes = row.getBytes();
                    copyIn.writeToCopy(data_bytes, 0, data_bytes.length);
                    writtenFiles.setValue(writtenFiles.getValue() + 1);
                } catch (Exception x) {
                    throw new RuntimeException("Failed in copy: \n " + data + "\n", x);
                }
            }


        }
        return rowsAffected;
    }

    public static String getCopyCommand(String tableName, ArrayList<String> columns,
                                        boolean isTSV, String nullString){
        String query = "COPY " + tableName + " ";
        if(columns != null && columns.size() > 0){
            query += "(" + String.join(",", columns) + ") ";
        }

        query += " FROM STDIN (format " + (isTSV ? "text":"csv") + " ";
        if(nullString != null && !nullString.isEmpty()){
            query += " , NULL '" + nullString + "' ";
        }
        query += ");";

        return query;
    }

}
