// ddl.test ------------------------------------------------------------------

// Data Definition Language (DDL) tests for @craigmcc/ts-database-postgres

// External Modules ----------------------------------------------------------

const chai = require("chai");
require("custom-env").env(true);
const expect = chai.expect;
import {
    ColumnAttributes,
    DataObject,
    DataType,
    TableAttributes,
} from "@craigmcc/ts-database";

// Internal Modules ----------------------------------------------------------

import ConnectionImpl from "../ConnectionImpl";

const CONNECTION_URI = process.env.CONNECTION_URI ? process.env.CONNECTION_URI : "UNKNOWN";
const db = new ConnectionImpl(CONNECTION_URI);

const TEST_COLUMNS: ColumnAttributes[] = [
    { name: "id", primaryKey: true, type: DataType.INTEGER, allowNull: false },
    { name: "first_name", type: DataType.STRING, allowNull: false },
    { name: "last_name", type: DataType.STRING, allowNull: false },
    { name: "active", type: DataType.BOOLEAN, allowNull: false, defaultValue: "true" },
    { name: "age", type: DataType.SMALLINT, allowNull: true },
    { name: "comments", type: DataType.STRING, allowNull: true },
    { name: "created", type: DataType.DATETIME, allowNull: true },
    { name: "started_date", type: DataType.DATE, allowNull: true },
    { name: "started_time", type: DataType.TIME, allowNull: true },
];
const TEST_COLUMNS_ADD: ColumnAttributes[] = [
    { name: "first_add", type: DataType.BIGINT, allowNull: true },
    { name: "second_add", type: DataType.STRING, allowNull: false, defaultValue: "Second" },
];
const TEST_TABLE = "test_table";

const INSERT_ROWS: DataObject[] = [
    {
        first_name: "Barney",
        last_name: "Rubble",
        comments: "This is Barney",
    },
    {
        first_name: "Fred",
        last_name: "Flintstone",
        comments: "This is Fred",
    },
    {
        first_name: "Bam Bam",
        last_name: "Rubble",
        active: false,
    }
]

// Private Functions ---------------------------------------------------------

const doAfter = async () => {
    try {
        await db.disconnect();
        expect(db.connected).equals(false);
    } catch (error) {
        expect.fail(`doAfter/ddl: Should not have thrown '${error.message}'`);
    }
}

const doBefore = async () => {
    try {
        await db.connect();
        expect(db.connected).equals(true);
    } catch (error) {
        expect.fail(`doBefore/ddl: Should not have thrown '${error.message}'`);
    }
}

const doBeforeEachCreateTable = async () => {
    try {
        await db.dropTable(TEST_TABLE);
    } catch (error) {
        // Ignore any errors from dropping the table that might not be there
    }
    try {
        await db.addTable(TEST_TABLE, TEST_COLUMNS);
    } catch (error) {
        expect.fail(`beforeEachCreateTable/ddl: addTable should not have thrown '${error.message}'`);
    }
}

const doBeforeEachDropTable = async () => {
    try {
        await db.dropTable(TEST_TABLE);
    } catch (error) {
        // Ignore any errors from dropping the table that might not be there
    }
}

const doBeforeEachInsertRows = async () => {
    await doBeforeEachCreateTable();
    try {
        await db.insert(TEST_TABLE, INSERT_ROWS);
    } catch (error) {
        expect.fail(`beforeEachInsertRows/ddl: insert should not have thrown '${error.message}'`);
    }
}

// Test Suite ----------------------------------------------------------------

describe("ddlTests", () => {

    // Lifecycle Hooks -------------------------------------------------------

    before("before", async () => {
        await doBefore();
    })

    after("after", async () => {
        await doAfter();
    })

    // Test Modules ----------------------------------------------------------

    describe("addColumn", () => {

        beforeEach("beforeEach/addColumn", async () => {
            await doBeforeEachCreateTable();
            await doBeforeEachInsertRows();
        })

        it("should fail on duplicate column", async () => {
            try {
                await db.addColumn(TEST_TABLE, TEST_COLUMNS[2]);
                expect.fail("Should have thrown error for duplicate column");
            } catch (error) {
                // Expected result
            }
        })

        it("should pass on one valid column", async () => {
            await db.addColumn(TEST_TABLE, TEST_COLUMNS_ADD[0]);
        })

        it("should pass on two valid columns", async () => {
            await db.addColumn(TEST_TABLE, TEST_COLUMNS_ADD);
        })

    })

    describe("addIndex", () => {

        it.skip("should fail on duplicate index", async () => {
            // TODO - pending addIndex() implementation
        })

        it.skip("should pass on valid non-unique index", async () => {
            // TODO - pending addIndex() implementation
        })

        it.skip("should pass on valid unique index", async () => {
            // TODO - pending addIndex() implementation
        })

    })

    describe("addTable", () => {

        beforeEach("beforeEach/addTable", async () => {
            await doBeforeEachDropTable();
        })

        it("should fail on duplicate table", async () => {
            await db.addTable(TEST_TABLE, TEST_COLUMNS);
            try {
                await db.addTable(TEST_TABLE, TEST_COLUMNS);
                expect.fail("Should have thrown error on duplicate table");
            } catch (error) {
                // Expected result
            }
        })

        it("should pass on valid table", async () => {
            await db.addTable(TEST_TABLE, TEST_COLUMNS);
        })

    });

    describe("describeTable", () => {

        beforeEach("beforeEach/describeTable", async () => {
            await doBeforeEachCreateTable();
        })

        it("should fail on non-existing table", async () => {
            const NON_EXISTING_TABLE = "non_existing_table";
            try {
                await db.describeTable(NON_EXISTING_TABLE);
                expect.fail("Should have thrown error on missing table");
            } catch (error) {
                // Expected result
            }
        })

        it ("should pass on existing table", async () => {
            const tableAttributes = await db.describeTable(TEST_TABLE);
            expect(tableAttributes.name).equals(TEST_TABLE);
            TEST_COLUMNS.forEach(testColumn => {
                const tableColumn = lookupColumn(tableAttributes, testColumn.name);
                expect(tableColumn.allowNull).equals(testColumn.allowNull);
                // TODO - Ignoring autoIncrement
                if (testColumn.defaultValue !== undefined) {
                    expect(tableColumn.defaultValue).equals(testColumn.defaultValue);
                }
                if (testColumn.primaryKey !== undefined) {
                    expect(tableColumn.primaryKey).equals(testColumn.primaryKey);
                }
                expect(tableColumn.type).equals(testColumn.type);
            })
        })

        const lookupColumn
            = (tableAttributes: TableAttributes, columnName: string)
            : ColumnAttributes =>
        {
            let foundColumn: ColumnAttributes | null = null;
            tableAttributes.columns.forEach(tableColumn => {
                if (tableColumn.name === columnName) {
                    foundColumn = tableColumn;
                }
            });
            if (foundColumn) {
                return foundColumn;
            }
            throw new Error(`columnName: Missing column '${columnName}'`);
        }

    })

    describe("dropColumn", () => {

        beforeEach("beforeEach/dropColumn", async () => {
            await doBeforeEachCreateTable();
        })

        it("should fail on invalid column", async () => {
            try {
                await db.dropColumn(TEST_TABLE, TEST_COLUMNS_ADD[0].name);
                expect.fail("Should have thrown error on missing column");
            } catch (error) {
                // Expected result
            }
        })

        it("should pass on valid column", async () => {
            await db.dropColumn(TEST_TABLE, TEST_COLUMNS[4].name);
        })

    })

    describe("dropIndex", () => {

        beforeEach("beforeEach/dropIndex", async () => {
            await doBeforeEachCreateTable();
        })

        it("should fail on invalid index", async () => {
            const INVALID_INDEX = "invalid_index_name";
            try {
                await db.dropIndex(TEST_TABLE, INVALID_INDEX);
                expect.fail("Should have thrown error on missing index");
            } catch (error) {
                // Expected result
            }
        })

        it.skip("should pass on valid index", async () => {
            // TODO - pending addIndex() implementation
        })

    })

    describe("dropTable", () => {

        beforeEach("beforeEach/dropTable", async () => {
            await doBeforeEachCreateTable();
        })

        it("should fail on non-existing table", async () => {
            const NON_EXISTING_TABLE = "non_existing_table";
            try {
                await db.dropTable(NON_EXISTING_TABLE);
                expect.fail("Should have thrown error on missing table");
            } catch (error) {
                // Expected result
            }
        })

        it ("should pass on existing table", async () => {
            await db.dropTable(TEST_TABLE);
        })

    })

})
