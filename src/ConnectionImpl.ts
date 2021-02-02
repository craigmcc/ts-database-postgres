// ConnectionImpl ------------------------------------------------------------

// Implementation of Connection from ts-database for PostgreSQL.

// External Modules ----------------------------------------------------------

import {
    ColumnAttributes,
//    ColumnNotFoundError,
    Connection,
    ConnectionAttributes,
    ConnectionURI,
    DataObject,
    DataType,
    ForeignKeyAttributes,
    IndexAttributes,
    NotConnectedError,
    NotSupportedError,
    TableName,
//    TableNotFoundError,
} from "@craigmcc/ts-database";
const { Client } = require("pg");
const format = require("pg-format");

// TODO - should be in @craigmcc/ts-database directly
import {ColumnNotFoundError, TableNotFoundError} from "./errors";

// Internal Modules ----------------------------------------------------------

// TODO - add to ts-database/src/types.ts
export interface TableAttributes {
    columns: ColumnAttributes[];    // Column attributes of this table
    name: TableName;                // Name of this table
}

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
        = async (databaseName: string, options?: object | undefined)
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
        = async (databaseName: string, options?: object | undefined)
        : Promise<void> => {
        throw new NotSupportedError("dropDatabase");
    }

    // DdlOperations Methods -------------------------------------------------

    addColumn
        = async (tableName: string, attributes: ColumnAttributes | ColumnAttributes[], options?: object | undefined)
        : Promise<void> => {
        this.checkConnected();
        throw new NotSupportedError("addColumn");
    }

    addForeignKey
        = async (tableName: string, columnName: string, attributes: ForeignKeyAttributes, options?: object | undefined)
        : Promise<string> => {
        this.checkConnected();
        throw new NotSupportedError("addForeignKey");
    }

    addIndex
        = async (tableName: string, attributes: IndexAttributes, options?: object | undefined)
        : Promise<string> => {
        this.checkConnected();
        throw new NotSupportedError("addIndex");
    }

    addTable
        = async (tableName: string, attributes: ColumnAttributes[], options?: object | undefined)
        : Promise<void> =>
    {
        this.checkConnected();
        let query = "CREATE TABLE " + format("%I", tableName) + " (";
        attributes.forEach((columnAttribute, index) => {
            if (index > 0) {
                query += ", ";
            }
            query += this.toColumnClause(columnAttribute);
        });
        query += ")";
//        console.debug("addTable QUERY:   ", query);
        const results = await this.client.query(query);
//        console.debug("addTable RESULTS: ", results);
    }

    // TODO - add to Connection
    describeTable
        = async (tableName: string, options?: object | undefined)
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
        let results = await this.client.query(query);
        if (results.rowCount === 0) {
            throw new TableNotFoundError(`tableName: Missing Table '${tableName}'`);
        }
        query = format("SELECT * FROM information_schema.columns"
            + " WHERE table_name = %L"
            + " AND table_schema = 'public'"
            , tableName)
        results = await this.client.query(query);
        const columns = [];
        results.rows.forEach((row: any) => {
            returning.columns.push(this.toColumnAttributes(row));
        });
        // TODO - indexes - https://stackoverflow.com/questions/6777456/list-all-index-names-column-names-and-its-table-name-of-a-postgresql-database
        // Can also parse column names from pg_indexes.indexdef
        return returning;
    }

    dropColumn
        = async (tableName: string, columnName: string, options?: object | undefined)
        : Promise<void> => {
        this.checkConnected();
        throw new NotSupportedError("dropColumn");
    }

    dropForeignKey
        = async (tableName: string, foreignKey: string, options?: object | undefined)
        : Promise<void> => {
        this.checkConnected();
        throw new NotSupportedError("dropForeignKey");
    }

    dropIndex
        = async (tableName: string, indexName: string, options?: object | undefined)
        : Promise<void> => {
        this.checkConnected();
        throw new NotSupportedError("dropIndex");
    }

    dropRows
        = async (tableName: string, options?: object | undefined)
        : Promise<void> => {
        this.checkConnected();
        throw new NotSupportedError("dropRows");
    }

    dropTable
        = async (tableName: string, options?: object | undefined)
        : Promise<void> =>
    {
        this.checkConnected();
        const query = format("DROP TABLE " + "%I CASCADE", tableName);
        try {
            await this.client.query(query);
        } catch (error) {
            throw new TableNotFoundError(error);
        }
    }

    dropTables
        = async (options?: object | undefined)
        : Promise<void> => {
        this.checkConnected();
        throw new NotSupportedError("dropTables");
    }

    // DmlOperations Methods -------------------------------------------------

    insert
        = async (tableName: string, row: DataObject, options?: object | undefined)
        : Promise<DataObject> => {
        this.checkConnected();
        throw new NotSupportedError("insert");
    };

    inserts
        = async (tableName: string, row: DataObject[], options?: object | undefined)
        : Promise<DataObject[]> => {
        this.checkConnected();
        throw new NotSupportedError("inserts");
    }

    // Private Methods -------------------------------------------------------

    /**
     * Convert a row (from information_schema.columns) into a ColumnAttributes.
     */
    private toColumnAttributes
        = (row: any)
        : ColumnAttributes => {
        // TODO - calculated value for primaryKey is a little funky
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
        let result = format("%I", columnAttributes.name)
            + " "
            + this.toSqlType(columnAttributes.type);
        if (!columnAttributes.allowNull) {
            result += " NOT NULL";
        }
        if (columnAttributes.defaultValue) {
            result += format(" DEFAULT %L", columnAttributes.defaultValue);
        }
        if (columnAttributes.primaryKey) {
            result += " PRIMARY KEY";
            // TODO - need default value expression?
        }
        // TODO - columnAttributes.autoIncrement?
        return result;
    }

    /**
     * Convert a TableAttributes into the table description (just the part
     * before the column definitions) that will be used in a CREATE TABLE
     * statement.
     */
    private toTableClause = (tableAttributes: TableAttributes): string => {
        let result = format("I", tableAttributes.name);
        return result;
    }

    private toDataType = (data_type: string): DataType => {
        // TODO - More data type conversions
        switch (data_type) {
            case "boolean":
                return DataType.BOOLEAN;
            case "character varying":
                return DataType.STRING;
            case "integer":
                return DataType.INTEGER;
            // TODO - deal better with unknown types?
            default:
                return DataType.STRING;
        }
    }

    private toSqlType = (dataType: DataType): string => {
        // TODO - More data type conversions
        switch (dataType) {
            case DataType.BOOLEAN:
                return "boolean";
            case DataType.INTEGER:
                return "integer";
            case DataType.STRING:
                return "character varying (255)";
            // TODO - deal better with unknown types?
            default:
                return "character varying (255)";
        }
    }

}

export default ConnectionImpl;
