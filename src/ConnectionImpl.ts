// ConnectionImpl ------------------------------------------------------------

// Implementation of Connection from ts-database for PostgreSQL.

// External Modules ----------------------------------------------------------

import {
    ColumnAttributes,
    ColumnName,
    ColumnNotFoundError,
    Connection,
    ConnectionAttributes,
    ConnectionURI,
    ConstraintName,
    DatabaseError,
    DatabaseName,
    DataObject,
    DataType,
    DuplicateColumnError,
    DuplicateIndexError,
    DuplicateTableError,
    ForeignKeyAttributes,
    IndexAttributes,
    IndexName,
    IndexNotFoundError,
    NotConnectedError,
    NotSupportedError,
    SelectCriteria,
    TableAttributes,
    TableName,
    TableNotFoundError,
    WhereCriteria,
} from "@craigmcc/ts-database";

const { Client } = require("pg");
const format = require("pg-format");

// Internal Modules ----------------------------------------------------------

// Public Objects ------------------------------------------------------------

// TODO - we are assuming the "public" schema throughout.  OK?

export class ConnectionImpl implements Connection {

    // Constructor
    constructor(params: ConnectionURI | ConnectionAttributes) {
        this.params = params;
    }

    // Private Members -------------------------------------------------------

    private client = new Client();
    private params: ConnectionURI | ConnectionAttributes;

    private checkConnected = (): void => {
        if (!this.connected) {
            throw new NotConnectedError("TsDatabasePostgres: Not connected");
        }
    }

    // ConnectionOperations Methods ------------------------------------------

    addDatabase
        = async (databaseName: DatabaseName, options?: object | undefined)
        : Promise<void> => {
        throw new NotSupportedError("addDatabase");
    }

    connect = async (): Promise<void> => {
        if (typeof this.params === "string") {
            this.client = new Client({connectionString: this.params});
        } else {
            this.client = new Client(this.params);
        }
        await this.client.connect();
        this.connected = true;
    }

    connected: boolean = false;

    disconnect
        = async ()
        : Promise<void> => {
        this.checkConnected();
        await this.client.end();
        this.connected = false;
    }

    dropDatabase
        = async (databaseName: DatabaseName, options?: object | undefined)
        : Promise<void> => {
        throw new NotSupportedError("dropDatabase");
    }

    // DdlOperations Methods -------------------------------------------------

    /**
     * Postgres-specific options:
     *   ifNotExists: boolean       Silently ignore if column already exists [false]
     */
    addColumn
        = async (tableName: TableName, attributes: ColumnAttributes | ColumnAttributes[], options?: object | undefined)
        : Promise<void> => {
        this.checkConnected();
        let query = `ALTER TABLE`;
        // @ts-ignore
        if (options && options.ifNotExists) {
            query += ` IF NOT EXISTS`;
        }
        query += ` ${format("%I", tableName)} `;
        const inputColumns: ColumnAttributes[] = Array.isArray(attributes)
            ? attributes : [ attributes ];
        const inputActions: string[] = [];
        inputColumns.forEach(inputColumn => {
            inputActions.push(` ADD COLUMN ${this.toColumnClause(inputColumn)}`);
        });
        query += inputActions.join(", ");
        try {
            await this.client.query(query);
        } catch (error) {
            this.throwError(error, "addColumn");
        }
    }

    addForeignKey
        = async (tableName: TableName, columnName: ColumnName, attributes: ForeignKeyAttributes, options?: object | undefined)
        : Promise<ConstraintName> => {
        this.checkConnected();
        const constraintName = attributes.name
            ? attributes.name
            : this.toConstraintName(tableName, columnName);
        // TODO - For now, deal with attributes.columnName allowing multiples
        const foreignColumnName = Array.isArray(attributes.columnName)
                ? attributes.columnName[0]
                : attributes.columnName;
        let query = `ALTER TABLE ${format("%I", tableName)}`
            + ` ADD CONSTRAINT ${format("%I", constraintName)}`
            + ` FOREIGN KEY ( ${format("%I", columnName)} )`
            + ` REFERENCES ${format("%I", attributes.tableName)}`
            + ` ( ${format("%I", foreignColumnName)} )`;
        if (attributes.onDelete) {
            query += ` ON DELETE ${attributes.onDelete}`;
        }
        if (attributes.onUpdate) {
            query += ` ON UPDATE ${attributes.onUpdate}`;
        }
        try {
            await this.client.query(query);
        } catch (error) {
            this.throwError(error, "addForeignKey");
        }
        return constraintName;
    }

    /**
     * Postgres-specific options:
     *   concurrently: boolean      Drop index without locks [false]
     *   ifNotExists: boolean       Silently ignore if index already exists [false]
     */
    addIndex
        = async (tableName: TableName, attributes: IndexAttributes, options?: object | undefined)
        : Promise<IndexName> => {
        this.checkConnected();
        const indexName: IndexName = attributes.name
            ? attributes.name
            : this.toIndexName(tableName, attributes.columnName);
        let query = "CREATE";
        if (attributes.unique) {
            query += " UNIQUE";
        }
        query += " INDEX";
        // @ts-ignore
        if (options && options.concurrently) {
            query += " CONCURRENTLY";
        }
        // @ts-ignore
        if (options && options.ifNotExists) {
            query += " IF NOT EXISTS";
        }
        query += format(" %I", indexName);
        query += ` ON ${format("%I", tableName)} (`;
        const names: string[] = [];
        if (Array.isArray(attributes.columnName)) {
            attributes.columnName.forEach(name => {
                names.push(format("%I", name));
            });
        } else {
            names.push(format("%I", attributes.columnName));
        }
        query += names.join(", ");
        query += ")";
        try {
            await this.client.query(query);
        } catch (error) {
            this.throwError(error, "addIndex");
        }
        return indexName;
    }

    /**
     * Postgres-specific options:
     *   ifNotExists: boolean       Silently ignore if table already exists [false]
     */
    addTable
        = async (tableName: TableName, attributes: ColumnAttributes[], options?: object | undefined)
        : Promise<void> =>
    {
        this.checkConnected();
        let query = `CREATE TABLE`;
        // @ts-ignore
        if (options && options.ifNotExists) {
            query += ` IF NOT EXISTS`;
        }
        query += ` ${format("%I", tableName)} (`;
        attributes.forEach((columnAttribute, index) => {
            if (index > 0) {
                query += ", ";
            }
            query += this.toColumnClause(columnAttribute);
        });
        query += ")";
        // TODO - global table attributes?
        try {
            await this.client.query(query);
        } catch (error) {
            this.throwError(error, "addTable");
        }
    }

    describeTable
        = async (tableName: TableName, options?: object | undefined)
        : Promise<TableAttributes> =>
    {
        const returning: TableAttributes = {
            columns: [],
            name: tableName,
        }
        let query = format("SELECT * FROM information_schema.tables"
            + " WHERE table_name = %L"
            + " AND table_schema = 'public'"
            , tableName);
        let results;
        try {
            results = await this.client.query(query);
        } catch (error) {
            this.throwError(error, "describeTable");
        }
        if (results.rowCount === 0) {
            throw new TableNotFoundError(`tableName: Missing Table '${tableName}'`);
        }
        query = format("SELECT * FROM information_schema.columns"
            + " WHERE table_name = %L"
            + " AND table_schema = 'public'"
            , tableName);
        try {
            results = await this.client.query(query);
        } catch (error) {
            this.throwError(error, "describeTable");
        }
        const columns = [];
        results.rows.forEach((row: any) => {
            returning.columns.push(this.toColumnAttributes(row));
        });
        // TODO - indexes - https://stackoverflow.com/questions/6777456/list-all-index-names-column-names-and-its-table-name-of-a-postgresql-database
        // TODO - Can also parse column names from pg_indexes.indexdef
        // TODO - foreign key constraints also
        return returning;
    }

    /**
     * Postgres-specific options:
     *   cascade: boolean           Drop objects that depend on this index [false]
     *   ifExists: boolean          Silently ignore if column does not exist [false]
     */
    dropColumn
        = async (tableName: TableName, columnName: ColumnName, options?: object | undefined)
        : Promise<void> => {
        this.checkConnected();
        let query = `ALTER TABLE ${format("%I", tableName)}`
            + ` DROP COLUMN`;
        // @ts-ignore
        if (options && options.ifExists) {
            query += ` IF EXISTS`
        }
        query += ` ${format("%I", columnName)}`;
        // @ts-ignore
        if (options && options.cascade) {
            query += ` CASCADE`;
        }
        try {
            await this.client.query(query);
        } catch (error) {
            this.throwError(error, "dropColumn");
        }
    }

    /**
     * Postgres-specific options:
     *   cascade: boolean           Drop objects that depend on this index [false]
     *   ifExists: boolean          Silently ignore if foreign key does not exist [false]
     */
    dropForeignKey
        = async (tableName: TableName, foreignKey: ColumnName, options?: object | undefined)
        : Promise<void> => {
        this.checkConnected();
        let query = `ALTER TABLE ${format("%I", tableName)} DROP CONSTRAINT`;
        // @ts-ignore
        if (options && options.ifExists) {
            query += ` IF EXISTS`;
        }
        query += ` ${format("%I", foreignKey)}`;
        // @ts-ignore
        if (options && options.cascade) {
            query += ` CASCADE`;
        }
        try {
            await this.client.query(query);
        } catch (error) {
            this.throwError(error, "dropForeignKey");
        }
    }

    /**
     * Postgres-specific options:
     *   cascade: boolean           Drop objects that depend on this index [false]
     *   concurrently: boolean      Drop index without locks [false]
     *   ifExists: boolean          Silently ignore if index does not exist [false]
     */
    dropIndex
        = async (tableName: TableName, indexName: IndexName, options?: object | undefined)
        : Promise<void> =>
    {
        this.checkConnected();
        // TODO - tableName should be removed from parameters
        let query = "DROP INDEX";
        // @ts-ignore
        if (options && options.concurrently) {
            query += " CONCURRENTLY ";
        }
        // @ts-ignore
        if (options && options.ifExists) {
            query += " IF EXISTS";
        }
        query += ` ${format("%I", indexName)}`;
        // @ts-ignore
        if (options && options.cascade) {
            query += " CASCADE";
        }
        try {
            await this.client.query(query);
        } catch (error) {
            this.throwError(error, "dropIndex");
        }
    }

    /**
     * Postgres-specific options:
     *   cascade: boolean           Drop objects that depend on this index [false]
     *   ifExists: boolean          Silently ignore if table does not exist [false]
     */
    dropTable
        = async (tableName: TableName, options?: object | undefined)
        : Promise<void> =>
    {
        this.checkConnected();
        let query = `DROP TABLE`;
        // @ts-ignore
        if (options && options.ifExists) {
            query += ` IF EXISTS`;
        }
        query += ` ${format("%I", tableName)}`;
        // @ts-ignore
        if (options && options.cascade) {
            query += ` CASCADE`;
        }
        try {
            await this.client.query(query);
        } catch (error) {
            this.throwError(error, "dropTable");
        }
    }

    dropTables
        = async (options?: object | undefined)
        : Promise<void> =>
    {
        this.checkConnected();
        throw new NotSupportedError("dropTables");
    }

    // DmlOperations Methods -------------------------------------------------

    delete
        = async (tableName: TableName, where: WhereCriteria, options?: object | undefined)
        : Promise<number> =>
    {
        this.checkConnected();
        const query = `DELETE FROM ${format("%I", tableName)} WHERE ${where.clause}`;
        const hasValues = where.values && (where.values.length > 0);
        let result;
        try {
            result = hasValues
                ? await this.client.query(query, where.values)
                : await this.client.query(query);
        } catch (error) {
            this.throwError(error, "delete");
        }
        return result.rowCount;
    }

    insert
        = async (tableName: TableName, rows: DataObject | DataObject[], options?: object | undefined)
        : Promise<number> =>
    {
        this.checkConnected();
        const inputRows: DataObject[] = Array.isArray(rows)
            ? rows : [ rows ];
        // Outer loop for each row to be inserted
        inputRows.forEach(async inputRow => {
            let query = `INSERT INTO ${format("%I", tableName)} (`;
            // Inner loop for column names
            const columns: string[] = [];
            for (const [key, value] of Object.entries(inputRow)) {
                columns.push(format("%I", key));
            }
            query += columns.join(", ") + ") VALUES (";
            // Inner loop for column values
            const values: string[] = [];
            for (const [key, value] of Object.entries(inputRow)) {
                values.push(format("%L", value));
            }
            query += values.join(", ") + ")";
            try {
                const output = await this.client.query(query);
            } catch (error) {
                this.throwError(error, "insert");
            }
        });
        return inputRows.length;
    }

    select
        = async (tableName: TableName, criteria: SelectCriteria, options?: object | undefined)
        : Promise<DataObject[]> =>
    {
        this.checkConnected();
        let query = "SELECT ";
        if (criteria.columns && (criteria.columns.length > 0)) {
            const names: string[] = [];
            criteria.columns.forEach(column => {
                names.push(format("%I", column));
            })
            query += names.join(", ");
        } else {
            query += "*";
        }
        query += ` FROM ${format("%I", tableName)}`;
        if (criteria.where && criteria.where.clause) {
            query += ` WHERE ${criteria.where.clause}`;
        }
        if (criteria.orderBy && (criteria.orderBy.length > 0)) {
            const columns: string[] = [];
            criteria.orderBy.forEach(column => {
                columns.push(format("%I", column));
            })
            query += ` ORDER BY ${columns.join(", ")}`;
        }
        if (criteria.limit) {
            query += ` LIMIT ${criteria.limit}`;
        }
        if (criteria.offset) {
            query += ` OFFSET ${criteria.offset}`;
        }
        const hasValues = criteria.where && criteria.where.values
            && (criteria.where.values.length > 0);
        let result;
        try {
            result = hasValues
                // @ts-ignore
                ? await this.client.query(query, criteria.where.values)
                : await this.client.query(query);
        } catch (error) {
            this.throwError(error, "select");
        }
        return result.rows;
    }

    truncate
        = async (tableName: TableName, options?: object | undefined)
        : Promise<void> =>
    {
        this.checkConnected();
        let query = `TRUNCATE ${format("%I", tableName)}`;
        try {
            await this.client.query(query);
        } catch (error) {
            this.throwError(error, "truncate");
        }
    }

    update
        = async (tableName: TableName, values: DataObject, where: WhereCriteria, options?: object | undefined)
        : Promise<number> =>
    {
        this.checkConnected();
        let query = `UPDATE ${format("%I", tableName)} SET `;
        const updates: string[] = [];
        for (const [key, value] of Object.entries(values)) {
            updates.push(format("%I = %L", key, value));
        }
        query += updates.join(", ") + ` WHERE ${where.clause}`;
        const hasValues = where.values && (where.values.length > 0);
        let result;
        try {
            result = hasValues
                ? await this.client.query(query, where.values)
                : await this.client.query(query);
        } catch (error) {
            this.throwError(error, "update");
        }
        return result.rowCount;
    }

    // Private Methods -------------------------------------------------------

    /**
     * Rethrow the specified error as a context-specific one (if it includes
     * a Postgres "code" field that maps to such a class.  Otherwise, just
     * throw DatabaseError and let the caller figure it out.
     *
     * Postgres Error Codes Documentation:
     * https://www.postgresql.org/docs/current/errcodes-appendix.html
     *
     * @param error Error thrown by an operation
     *
     * @throws DatabaseError or a more specific subclass, as appropriate
     */
    private throwError = (error: Error, context?: string) : void => {

        // Simply pass on errors we cannot directly deal with
        if (error instanceof DatabaseError) {
            throw error;
        }
        // @ts-ignore
        if (!error["code"]) {
            throw error; // TODO - make DatabaseError not abstract?
        }
        // @ts-ignore
        const code: string = error["code"];

        // Attempt to dispatch based on known error codes
        switch (code) {
            case "23502":           // not_null_violation
                throw error;
            case "23503":           // foreign_key_violation
                throw error;
            case "23505":           // unique_violation
                throw error;
            case "42701":           // duplicate_column
                throw new DuplicateColumnError(error, context);
            case "42703":           // undefined_column
                throw new ColumnNotFoundError(error, context);
            case "42704":           // undefined_object (TODO - hmmm)
                throw new IndexNotFoundError(error, context);
            case "42P01":           // undefined_table
                throw new TableNotFoundError(error, context);
            case "42P07":           // duplicate_table (or duplicate index)
                if (context === "addIndex") {
                    throw new DuplicateIndexError(error, context);
                } else if (context === "addTable") {
                    throw new DuplicateTableError(error, context);
                } else {
                    throw error;
                }

                throw new DuplicateTableError(error, context);
            default:                // no specific error class to use
                throw error;
        }

    }

    /**
     * Convert a row (from information_schema.columns) into a ColumnAttributes.
     */
    private toColumnAttributes
        = (row: any)
        : ColumnAttributes => {
        // TODO - calculated value for defaultValue and primaryKey are a little funky
        const columnAttributes: ColumnAttributes = {
            allowNull: row.is_nullable === "YES",
            autoIncrement: row.column_default && row.column_default.startsWith("nextval"),
            defaultValue: row.column_default && !row.column_default.startsWith("nextval")
                ? row.column_default
                : undefined,
            name: row.column_name,
            primaryKey: row.column_default && row.column_default.startsWith("nextval"),
            type: this.toDataType(row.data_type),
        }
        return columnAttributes;
    }

    /**
     * Convert a ColumnAttributes into the column description that will be
     * used in a CREATE TABLE or ALTER TABLE ADD COLUMN statement.
     */
    private toColumnClause = (columnAttributes: ColumnAttributes): string => {
        // Special handling for columns marked as primary key
        let resolvedType = this.toSqlType(columnAttributes.type);
        if (columnAttributes.primaryKey) {
            switch (columnAttributes.type) {
                case DataType.BIGINT: resolvedType = "bigserial"; break;
                case DataType.SMALLINT: resolvedType = "smallserial"; break;
                default: resolvedType = "serial"; break;
            }
        }
        let result = format("%I", columnAttributes.name)
            + " "
            + resolvedType;
        if (!columnAttributes.allowNull) {
            result += " NOT NULL";
        }
        // NOTE - we are ignoring any columnAttributes.autoIncrement
        if (columnAttributes.primaryKey) {
            result += " PRIMARY KEY";
        } else if (columnAttributes.defaultValue) {
            result += format(" DEFAULT %L", columnAttributes.defaultValue);
        }
        return result;
    }

    private toDataType = (data_type: string): DataType => {
        if (data_type.startsWith("timestamp")) {
            return DataType.DATETIME;
        } else if (data_type.startsWith("time")) {
            return DataType.TIME;
        }
        switch (data_type) {
            case "bigint":
            case "int8":
                return DataType.BIGINT
            case "boolean":
                return DataType.BOOLEAN;
            case "date":
                return DataType.DATE;
            case "character varying":
                return DataType.STRING;
            case "integer":
            case "int":
            case "int4":
                return DataType.INTEGER;
            case "smallint":
            case "int2":
                return DataType.SMALLINT;
            case "time":
                return DataType.TIME;
            case "timestamp":
                return DataType.DATETIME;
            // TODO - deal better with unknown types?
            default:
                return DataType.STRING;
        }
    }

    // NOTE - returned name has not been passed through format()
    private toConstraintName
        = (fromTableName: TableName, fromColumnName: ColumnName)
        : ConstraintName =>
    {
        const elements: string[] = [];
        elements.push(fromTableName);
        elements.push(fromColumnName);
        elements.push("fkey");
        return elements.join("_");
    }

    // NOTE - returned name has not been passed through format()
    private toIndexName
        = (tableName: TableName, columnName: ColumnName | ColumnName[])
        : IndexName =>
    {
        const names: string[] = [];
        names.push(tableName);
        if (Array.isArray(columnName)) {
            columnName.forEach(name => {
                names.push(name);
            })
        } else {
            names.push(columnName);
        }
        names.push("idx");
        return names.join("_");
    }

    private toSqlType = (dataType: DataType): string => {
        // TODO - More data type conversions
        switch (dataType) {
            case DataType.BIGINT:
                return "bigint";
            case DataType.BOOLEAN:
                return "boolean";
            case DataType.DATE:
                return "date";
            case DataType.DATETIME:
                return "timestamp with time zone";
            case DataType.INTEGER:
                return "integer";
            case DataType.SMALLINT:
                return "smallint";
            case DataType.STRING:
                return "character varying (255)";
            case DataType.TIME:
                return "time without time zone";
            case DataType.TINYINT:
                return "smallint"; // Postgres does not support?
            // TODO - deal better with unknown types?
            default:
                return "character varying (255)";
        }
    }

    /**
     * Convert a TableAttributes into the table description (just the part
     * before the column definitions) that will be used in a CREATE TABLE
     * statement.
     */
    private toTableClause = (tableAttributes: TableAttributes): string => {
        let result = format("%I", tableAttributes.name);
        return result;
    }

}

export default ConnectionImpl;
