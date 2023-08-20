package simpledb.common;

import simpledb.storage.DbFile;

public class TableInfo {

    private int    tableId;
    private String tableName;
    private DbFile dbFile;
    private String primaryKeyName;

    public TableInfo(final int tableId, final String tableName, final DbFile dbFile, final String primaryKeyName) {
        this.tableId = tableId;
        this.tableName = tableName;
        this.dbFile = dbFile;
        this.primaryKeyName = primaryKeyName;
    }

    public int getTableId() {
        return tableId;
    }

    public void setTableId(final int tableId) {
        this.tableId = tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(final String tableName) {
        this.tableName = tableName;
    }

    public DbFile getDbFile() {
        return dbFile;
    }

    public void setDbFile(final DbFile dbFile) {
        this.dbFile = dbFile;
    }

    public String getPrimaryKeyName() {
        return primaryKeyName;
    }

    public void setPrimaryKeyName(final String primaryKeyName) {
        this.primaryKeyName = primaryKeyName;
    }
}
