package io.burt.athena;

import org.slf4j.LoggerFactory;

import java.sql.*;

class AthenaDatabaseMetaData implements DatabaseMetaData {
    org.slf4j.Logger log = LoggerFactory.getLogger(AthenaDatabaseMetaData.class);

    public static void main(String[] args) {
        AthenaDatabaseMetaData meta = new AthenaDatabaseMetaData();
        try {
            ResultSet resultSet = meta.getTableTypes();
            while(resultSet.next()) {
                System.out.println(resultSet.getString(0));
                System.out.println(resultSet.getString("TABLE_TYPE"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private final AthenaConnection connection;

    AthenaDatabaseMetaData(Connection connection) {
        this.connection = (AthenaConnection) connection;
    }

    AthenaDatabaseMetaData() {
        this.connection = null;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return iface.cast(this);
        } else {
            throw new SQLException(String.format("%s is not a wrapper for %s", this.getClass().getName(), iface.getName()));
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public String getDriverName() {
        return AthenaDriver.JDBC_SUBPROTOCOL;
    }

    @Override
    public String getDriverVersion() {
        return AthenaDriverInfo.getDriverVersion();
    }

    @Override
    public int getDriverMajorVersion() {
        return AthenaDriverInfo.getDriverMajorVersion();
    }

    @Override
    public int getDriverMinorVersion() {
        return AthenaDriverInfo.getDriverMinorVersion();
    }

    @Override
    public String getDatabaseProductName() {
        return "Amazon Athena";
    }

    @Override
    public String getDatabaseProductVersion() {
        return null;
    }

    @Override
    public int getDatabaseMajorVersion() {
        return 0;
    }

    @Override
    public int getDatabaseMinorVersion() {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 2;
    }

    @Override
    public String getUserName() {
        return null;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return AthenaDriver.createURL(connection.getSchema());
    }

    @Override
    public boolean usesLocalFiles() {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    @Override
    public boolean allProceduresAreCallable() {
        return true;
    }

    @Override
    public boolean allTablesAreSelectable() {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() {
        return true;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() {
        return "\"";
    }

    @Override
    public String getSQLKeywords() {
        return "";
    }

    @Override
    public String getNumericFunctions() {
        return "";
    }

    @Override
    public String getStringFunctions() {
        return "";
    }

    @Override
    public String getSystemFunctions() {
        return "";
    }

    @Override
    public String getTimeDateFunctions() {
        return "";
    }

    @Override
    public String getSearchStringEscape() {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        return true;
    }

    @Override
    public boolean supportsConvert() {
        return true;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 0: supportsConvert");
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return true;
    }

    @Override
    public boolean supportsGroupBy() {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() {
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        return true;
    }

    @Override
    public String getSchemaTerm() {
        return "schema";
    }

    @Override
    public String getProcedureTerm() {
        return "";
    }

    @Override
    public String getCatalogTerm() {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart() {
        return true;
    }

    @Override
    public String getCatalogSeparator() {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return true;
    }

    @Override
    public boolean supportsUnion() {
        return true;
    }

    @Override
    public boolean supportsUnionAll() {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        return true;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        return true;
    }

    @Override
    public int getMaxBinaryLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() {
        return 255;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() {
        return 0;
    }

    @Override
    public int getMaxConnections() {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    @Override
    public int getMaxIndexLength() {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return 255;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() {
        return 0;
    }

    @Override
    public int getMaxRowSize() {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return true;
    }

    @Override
    public int getMaxStatementLength() {
        return 262144;
    }

    @Override
    public int getMaxStatements() {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() {
        return 255;
    }

    @Override
    public int getMaxTablesInSelect() {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        return level == Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) {
//        ListResultSet retVal = new ListResultSet();
//        retVal.setColumnNames("PROCEDURE_CAT", "PROCEDURE_SCHEMA", "PROCEDURE_NAME", "REMARKS",
//                "PROCEDURE_TYPE", "SPECIFIC_NAME");
//        return retVal;
        return empty();
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) {
//        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 678");
        return empty();
    }

    private static ResultSet empty() {
        return new ListResultSet();
    }

    private String[] createTableRow(String catalogName, String tableName, String schemaPattern, String tableNamePattern, String[] types) {
        String[] data = new String[10];
        data[0] = "AwsDataCatalog"; // TABLE_SCHEM
        data[1] = schemaPattern; // TABLE_CAT
        data[2] = tableName; // TABLE_NAME
        data[3] = "TABLE"; // TABLE_TYPE
        data[4] = String.format("r: %s, %s, %s, %s", catalogName, schemaPattern, tableNamePattern, String.join(":", types)); // REMARKS
        data[5] = ""; // TYPE_CAT
        data[6] = ""; // TYPE_SCHEM
        data[7] = ""; // TYPE_NAME
        data[8] = ""; // SELF_REFERENCING_COL_NAME
        data[9] = ""; // REF_GENERATION
        return data;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) {
        ListResultSet listResultSet = new ListResultSet();
        listResultSet.setColumnNames("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME",
                "REF_GENERATION");
        try(Statement statement = connection.createStatement()) {
            String schemaPatternFormat = schemaPattern.replaceAll("\\\\", "");
            log.info(String.format("getTables, catalog:%s, schemaPattern:%s, tableNamePattern:%s", catalog, schemaPattern, tableNamePattern));
            String sql = String.format("show TABLES in %s;", schemaPatternFormat);
            log.info(sql);

            try(ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    listResultSet.addRow(createTableRow(catalog, resultSet.getString(1), schemaPatternFormat, tableNamePattern, types));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e + String.format(" getTables %s, %s, %s", catalog, schemaPattern, tableNamePattern));
        }

        return listResultSet;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
//        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: getSchemas");
        Statement statement = connection.createStatement();
        return statement.executeQuery("SELECT schema_name as TABLE_SCHEM, catalog_name as TABLE_CATALOG FROM INFORMATION_SCHEMA.SCHEMATA;");
    }

    @Override
    public ResultSet getCatalogs() {
//        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 5");
        ListResultSet result = new ListResultSet();
        result.setColumnNames("TABLE_CAT");
        log.info("getCatalogs, ");

        try(Statement statement = connection.createStatement()) {
            String sql = "SELECT distinct(catalog_name) FROM information_schema.schemata;";
            log.info(sql);

            try(ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    result.addRow(new String[] {resultSet.getString(1)});
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    @Override
    public ResultSet getTableTypes() {
        ListResultSet result = new ListResultSet();
        result.setColumnNames("TABLE_TYPE");
        result.addRow(new String[]{"TABLE"});
        result.addRow(new String[]{"tables"});
        result.addRow(new String[]{"table"});
        result.addRow(new String[]{"VIEW"});
        result.addRow(new String[]{"SYSTEM TABLE"});
        result.addRow(new String[]{"GLOBAL TEMPORARY"});
        result.addRow(new String[]{"LOCAL TEMPORARY"});
        result.addRow(new String[]{"ALIAS"});
        result.addRow(new String[]{"SYNONYM"});
        return result;
    }

    protected int convertColumnTypeToEnum(String columnType) {
        switch (columnType){
            case "varchar":
                return Types.VARCHAR;
            case "integer":
                return Types.INTEGER;
            case "bigint":
                return Types.BIGINT;
            case "boolean":
                return Types.BOOLEAN;
            default:
                return Types.VARCHAR;
        }
    }

    protected Object[] createColumnsRow(String columnName, String columnType, String position, String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
    {
        int sqlDataType = convertColumnTypeToEnum(columnType);
        Object[] data = new Object[24];
        data[0] = this.connection.getCatalog(); // TABLE_CAT
        data[1] = schemaPattern; // TABLE_SCHEM
        data[2] = tableNamePattern; // TABLE_NAME
        data[3] = columnName; // COLUMN_NAME
        data[4] = sqlDataType; // DATA_TYPE
        data[5] = columnType; // TYPE_NAME
        data[6] = 255; // COLUMN_SIZE
        data[7] = 255; // BUFFER_LENGTH
        data[8] = 32; // DECIMAL_DIGITS
        data[9] = 32; // NUM_PREC_RADIX
        data[10] = columnNullable; // NULLABLE
        data[11] = String.format("%s, %s, %s, %s", catalog, schemaPattern, tableNamePattern, columnNamePattern); // Remarks
        data[12] = null; // COLUMN_DEF
        data[13] = sqlDataType; // SQL_DATA_TYPE
        data[14] = sqlDataType; // SQL_DATETIME_SUB
        data[15] = 16; // CHAR_OCTET_LENGTH
        data[16] = Integer.parseInt(position); // ORDINAL_POSITION
        data[17] = "YES"; // IS_NULLABLE
        data[18] = null; // SCOPE_CATLOG
        data[19] = null; // SCOPE_SCHEMA
        data[20] = null; // SCOPE_TABLE
        data[21] = columnType; // SOURCE_DATA_TYPE
        data[22] = ""; // IS_AUTOINCREMENT
        data[23] = ""; // IS_GENERATEDCOLUMN
        return data;
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
        ListResultSet listResultSet = new ListResultSet();
        listResultSet.setColumnNames("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
                "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
                "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
                "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATLOG", "SCOPE_SCHEMA",
                "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"
        );

        String tableName = tableNamePattern.replaceAll("\\\\", "");
        String schemaName = schemaPattern.replaceAll("\\\\", "");
        try(Statement statement = connection.createStatement()) {
            String sql = String.format("SELECT * FROM information_schema.columns WHERE table_schema = '%s' and table_name = '%s';", schemaName, tableName);
            if (schemaName == null || schemaName == "") {
                sql = String.format("SELECT * FROM information_schema.columns WHERE table_name = '%s';", tableName);
            }

            log.info(sql);
            try(ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    String dataType = resultSet.getString("data_type");
                    String extraInfo = resultSet.getString("extra_info");   // partition key
                    String position = resultSet.getString("ordinal_position");
                    listResultSet.addRow(createColumnsRow(resultSet.getString("column_name"), dataType, position, catalog, schemaPattern, tableNamePattern, columnNamePattern));
                }
            }
            log.info(listResultSet.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e + String.format("getColumns %s, %s, %s", catalog, schemaPattern, tableNamePattern));
        }
        return listResultSet;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) {
        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 8");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) {
        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 9");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) {
        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 10");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) {
        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 11");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) {
//        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 12");
        return empty();
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) {
//        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 13");
        return empty();
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) {
        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 14");
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) {
        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 15");
    }

    @Override
    public ResultSet getTypeInfo() {
        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 16");
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) {
//        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 17");
        return empty();
    }

    @Override
    public boolean supportsResultSetType(int type) {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) {
        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 18");
    }

    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) {
        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 19");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) {
        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 20");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) {
        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 21");
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return false;
    }

    @Override
    public int getResultSetHoldability() {
        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 22");
    }

    @Override
    public int getSQLStateType() {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) {
        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 23");
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() {
        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 24");
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) {
//        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 25");
        return empty();
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) {
//        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 26");
        return empty();
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
        throw new UnsupportedOperationException("DatabaseMetaData Not implemented: 27");
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false;
    }
}
