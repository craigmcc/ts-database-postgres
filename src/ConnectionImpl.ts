// ConnectionImpl ------------------------------------------------------------

// Implementation of Connection from ts-database for PostgreSQL.

// External Modules ----------------------------------------------------------

import {
    ColumnAttributes,
    Connection,
    ConnectionAttributes,
    ConnectionURI,
    DataObject,
    DataType,
    ForeignKeyAttributes,
    IndexAttributes,
    NotConnectedError,
    NotSupportedError,
    SelectCriteria,
    TableName,
    TableNotFoundError,
    WhereCriteria,
} from "@craigmcc/ts-database";

const { Client } = require("pg");
const format = require("pg-format");

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
        let query = `ALTER TABLE ${format("%I", tableName)} `;
        const inputColumns: ColumnAttributes[] = Array.isArray(attributes)
            ? attributes : [ attributes ];
        const inputActions: string[] = [];
        inputColumns.forEach(inputColumn => {
            inputActions.push(` ADD COLUMN ${this.toColumnClause(inputColumn)}`);
        });
        query += inputActions.join(", ");
        await this.client.query(query);
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
        // TODO - global table attributes?
        const results = await this.client.query(query);
    }

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
        // TODO - Can also parse column names from pg_indexes.indexdef
        return returning;
    }

    dropColumn
        = async (tableName: string, columnName: string, options?: object | undefined)
        : Promise<void> => {
        this.checkConnected();
        const query = `ALTER TABLE ${format("%I", tableName)}`
            + ` DROP COLUMN ${format("%I", columnName)}`
        await this.client.query(query);
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
        const result = hasValues
            ? await this.client.query(query, where.values)
            : await this.client.query(query);
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
            const output = await this.client.query(query);
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
        const result = hasValues
            // @ts-ignore
            ? await this.client.query(query, criteria.where.values)
            : await this.client.query(query);
        return result.rows;
    }

    truncate
        = async (tableName: TableName, options?: object | undefined)
        : Promise<void> =>
    {
        this.checkConnected();
        let query = `TRUNCATE ${format("%I", tableName)}`;
        await this.client.query(query);
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
        const result = hasValues
            ? await this.client.query(query, where.values)
            : await this.client.query(query);
        return result.rowCount;
    }

    // Private Methods -------------------------------------------------------

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
        let result = format("I", tableAttributes.name);
        return result;
    }

}

export default ConnectionImpl;
