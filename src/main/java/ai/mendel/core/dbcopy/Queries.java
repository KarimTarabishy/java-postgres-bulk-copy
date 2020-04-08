package ai.mendel.core.dbcopy;

public class Queries {

    public static String truncate(String tableName){
        return "Truncate " + tableName + ";";
    }

    public static String drop(String tableName){
        return "Drop Table if exists " + tableName + ";";
    }

    public static String createconstraints(String tableName, String resultColName){
        String [] parts = tableName.split(".");
        String schema = "public";
        String rel = parts[0];
        if(parts.length == 2){
            schema = parts[0];
            rel = parts[1];
        }

        return "SELECT 'ALTER TABLE '||nspname||'.'||relname||'\n" +
                "ADD CONSTRAINT '||conname||' '|| pg_get_constraintdef(pg_constraint.oid)||';' " +
                "as "+resultColName+"\n" +
                "FROM pg_constraint\n" +
                "INNER JOIN pg_class ON conrelid=pg_class.oid\n" +
                "INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace\n" +
                "where nspname = '"+schema+"' AND relname='"+rel+"\n" +
                "ORDER BY CASE WHEN contype='f' THEN 0 ELSE 1 END DESC," +
                "contype DESC,nspname DESC,relname DESC,conname DESC;";
    }

    public static String dropConstraints(String tableName, String resultColName){
        String [] parts = tableName.split(".");
        String schema = "public";
        String rel = parts[0];
        if(parts.length == 2){
            schema = parts[0];
            rel = parts[1];
        }

        return "SELECT 'ALTER TABLE '||nspname||'.'||relname||' DROP CONSTRAINT '||conname||';' " +
                "as "+resultColName+"\n" +
                "FROM pg_constraint\n" +
                "INNER JOIN pg_class ON conrelid=pg_class.oid\n" +
                "INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace\n" +
                "where nspname = '"+schema+"' AND relname='"+rel+"\n" +
                "ORDER BY CASE WHEN contype='f' THEN 0 ELSE 1 END,contype,nspname,relname,conname;";
    }

    public static String createIndices(String tableName, String resultColName){
        return "select  indexdef as " + resultColName +"\n" +
                "from pg_index pgi\n" +
                "  join pg_class idx on idx.oid = pgi.indexrelid\n" +
                "  join pg_class tbl on tbl.oid = pgi.indrelid\n" +
                "  join pg_namespace tnsp on tnsp.oid = tbl.relnamespace\n" +
                "  join pg_indexes pgidxs on pgidxs.tablename = tbl.relname " +
                "       and pgidxs.schemaname = tnsp.nspname and idx.relname = pgidxs.indexname\n" +
                "  LEFT JOIN pg_depend d ON d.objid = pgi.indexrelid AND d.deptype = 'i'\n" +
                "where pgi.indisunique AND pgi.indrelid = '"+tableName+"'::regclass\n" +
                "    AND d.objid IS NULL;";
    }

    public static String dropIndices(String tableName, String resultColName){
        return "SELECT 'DROP INDEX ' || string_agg(indexrelid::regclass::text,', ') || ';' as "+resultColName+"\n" +
                "   FROM   pg_index  i\n" +
                "   LEFT   JOIN pg_depend d ON d.objid = i.indexrelid\n" +
                "                          AND d.deptype = 'i'\n" +
                "   WHERE  i.indrelid = '"+tableName+"'::regclass  -- possibly schema-qualified\n" +
                "   AND    d.objid IS NULL;";
    }


}
