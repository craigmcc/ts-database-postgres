// dml.test ------------------------------------------------------------------

// Data Manipulation Language (DML) tests for @craigmcc/ts-database-postgres

// External Modules ----------------------------------------------------------

const chai = require("chai");
require("custom-env").env(true);
const expect = chai.expect;
import {
    ColumnAttributes,
    DataObject,
    DataType, TableNotFoundError, WhereCriteria,
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
        console.debug("doAfter: Disconnected");
        expect(db.connected).equals(false);
    } catch (error) {
        expect.fail(`doAfter: Should not have thrown '${error.message}'`);
    }
}

const doBefore = async () => {
    try {
        await db.connect();
        console.debug("doBefore: Connected");
        expect(db.connected).equals(true);
    } catch (error) {
        expect.fail(`doBefore: Should not have thrown '${error.message}'`);
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
//        console.debug(`beforeEachCreateTable: Created table '${TEST_TABLE}'`);
    } catch (error) {
        expect.fail(`beforeEachCreateTable: addTable should not have thrown '${error.message}'`);
    }
}

const doBeforeEachInsertRows = async () => {
    await doBeforeEachCreateTable();
    try {
        await db.insert(TEST_TABLE, INSERT_ROWS);
//        console.debug(`beforeEachInsertRows: Inserted ${INSERT_ROWS.length} rows`);
    } catch (error) {
        expect.fail(`beforeEachInsertRows: insert should not have thrown '${error.message}'`);
    }
}

// Test Lifecycle Hooks ------------------------------------------------------

before("before", async () => {
    await doBefore();
})

after("after", async () => {
    await doAfter();
})

// Test Suites ---------------------------------------------------------------

describe("delete", () => {

    beforeEach("beforeEach/delete", async () => {
        await doBeforeEachCreateTable();
        await doBeforeEachInsertRows();
    })

    it("should delete matching rows on match", async () => {

        const count = await db.delete(TEST_TABLE, {
            clause: "last_name = $1",
            values: [ "Rubble" ],
        });

        expect(count).equals(2);

    })

    it("should delete no rows on mismatch", async () => {

        const count = await db.delete(TEST_TABLE, {
                clause: "comments = 'nonexistent comment'",
        });

        expect(count).equals(0);

    })

})

describe("insert", () => {

    beforeEach("beforeEach/insert", async () => {
        await doBeforeEachCreateTable();
    })

    it("should pass inserting one row", async () => {

        const count = await db.insert(TEST_TABLE, INSERT_ROWS[0]);
        expect(count).equals(1);
        const results: DataObject[] = await db.select(TEST_TABLE, {});
        expect(results.length).equals(1);

        expect(results[0].first_name).equals(INSERT_ROWS[0].first_name);
        expect(results[0].last_name).equals(INSERT_ROWS[0].last_name);
        expect(results[0].active).equals(true);
        expect(results[0].age).to.be.null;
        expect(results[0].comments).equals(INSERT_ROWS[0].comments);
        expect(results[0].created).to.be.null;
        expect(results[0].started_date).to.be.null;
        expect(results[0].started_time).to.be.null;

    })

    it("should pass inserting three rows", async () => {

        const count = await db.insert(TEST_TABLE, INSERT_ROWS);
        expect(count).equals(3);
        const results: DataObject[] = await db.select(TEST_TABLE, {});
        expect(results.length).equals(3);

        expect(results[0].first_name).equals(INSERT_ROWS[0].first_name);
        expect(results[0].last_name).equals(INSERT_ROWS[0].last_name);
        expect(results[0].active).equals(true);
        expect(results[0].age).to.be.null;
        expect(results[0].comments).equals(INSERT_ROWS[0].comments);
        expect(results[0].created).to.be.null;
        expect(results[0].started_date).to.be.null;
        expect(results[0].started_time).to.be.null;

        expect(results[1].first_name).equals(INSERT_ROWS[1].first_name);
        expect(results[1].last_name).equals(INSERT_ROWS[1].last_name);
        expect(results[1].active).equals(true);
        expect(results[1].age).to.be.null;
        expect(results[1].comments).equals(INSERT_ROWS[1].comments);
        expect(results[1].created).to.be.null;
        expect(results[1].started_date).to.be.null;
        expect(results[1].started_time).to.be.null;

        expect(results[2].first_name).equals(INSERT_ROWS[2].first_name);
        expect(results[2].last_name).equals(INSERT_ROWS[2].last_name);
        expect(results[2].active).equals(false);
        expect(results[2].age).to.be.null;
        expect(results[2].comments).to.be.null;
        expect(results[2].created).to.be.null;
        expect(results[2].started_date).to.be.null;
        expect(results[2].started_time).to.be.null;

    })

})

describe("select", () => {

    beforeEach("beforeEach/select", async () => {
        await doBeforeEachCreateTable();
        await doBeforeEachInsertRows();
    })

    it("should order by specified columns", async () => {

        const rows = await db.select(TEST_TABLE, {
            columns: [ "id", "first_name", "last_name" ],
            orderBy: [ "last_name", "first_name" ],
        });

        expect(rows.length).equals(INSERT_ROWS.length);
        let previous = "";
        rows.forEach(row => {
            const current: string = row.last_name + "|" + row.first_name;
            if (previous !== "") {
                expect(current > previous).to.be.true;
            }
            previous = current;
        });

    })

    it("should return only the specified columns", async () => {

        const rows = await db.select(TEST_TABLE, {
            columns: [ "id", "first_name", "last_name"]
        });

        expect(rows.length).equals(INSERT_ROWS.length);
        rows.forEach((row, index) => {
            expect(row.id).not.to.be.null;
            expect(row.first_name).equals(INSERT_ROWS[index].first_name);
            expect(row.last_name).equals(INSERT_ROWS[index].last_name);
            expect(row.active).to.be.undefined;
            expect(row.age).to.be.undefined;
            expect(row.comments).to.be.undefined;
        })

    })

    it("should select no rows on mismatch", async () => {

        const rows = await db.select(TEST_TABLE, {
            where: {
                clause: "comments = 'nonexistent comment'",
            }
        })

        expect(rows.length).equals(0);

    })

    it("should select only active rows", async () => {

        const rows = await db.select(TEST_TABLE, {
            where: {
                clause: "active = $1",
                values: [ true ],
            }
        })

        rows.forEach(row => {
            expect(row.active).to.be.true;
        })

    })

    it("should select only the matching row", async () => {

        const index = 2;
        const rows = await db.select(TEST_TABLE, {
            where: {
                clause: "first_name = $1 AND last_name = $2",
                values: [ INSERT_ROWS[index].first_name, INSERT_ROWS[index].last_name ],
            }
        })

        expect(rows.length).equals(1);
        expect(rows[0].first_name).equals(INSERT_ROWS[index].first_name);
        expect(rows[0].last_name).equals(INSERT_ROWS[index].last_name);

    })

    it("should select only the second row", async () => {

        const rows = await db.select(TEST_TABLE, {
            limit: 1,
            offset: 1,
        });

        expect(rows.length).equals(1);
        expect(rows[0].first_name).equals(INSERT_ROWS[1].first_name);
        expect(rows[0].last_name).equals(INSERT_ROWS[1].last_name);

    })

})

describe("truncate", () => {

    beforeEach("beforeEach/update", async () => {
        await doBeforeEachCreateTable();
        await doBeforeEachInsertRows();
    })

    it("should truncate all rows", async () => {

        const rowsFirst = await db.select(TEST_TABLE, {});
        expect(rowsFirst.length).equals(INSERT_ROWS.length);
        await db.truncate(TEST_TABLE);
        const rowsSecond = await db.select(TEST_TABLE, {});
        expect(rowsSecond.length).equals(0);

    })

})

describe("update", () => {

    beforeEach("beforeEach/update", async () => {
        await doBeforeEachCreateTable();
        await doBeforeEachInsertRows();
    })

    it("should update matching rows on match", async () => {

        const LAST_NAME = "Rubble";
        const STARTED_TIME = "15:00:00";
        const count = await db.update(TEST_TABLE, {
            started_time: STARTED_TIME,
        }, {
            clause: "last_name = $1",
            values: [ LAST_NAME ],
        });
        expect(count).equals(2);

        const rows = await db.select(TEST_TABLE, {
            where: {
                clause: "last_name = $1",
                values: [ LAST_NAME ],
            }
        })
        expect(count).equals(2);
        rows.forEach(row => {
            expect(row.last_name).equals(LAST_NAME);
            expect(row.started_time).equals(STARTED_TIME);
        })

    })

    it("should update one row on exact match", async () => {

        const AGE = 42;
        const FIRST_NAME = "Fred";
        const LAST_NAME = "Flintstone";
        const WHERE: WhereCriteria = {
            clause: "first_name = $1 AND last_name = $2",
            values: [ FIRST_NAME, LAST_NAME ],
        }
        const count = await db.update(TEST_TABLE,
            { age: AGE },
            WHERE);
        expect(count).equals(1);

        const rows = await db.select(TEST_TABLE, {
            where: WHERE,
        });
        expect(count).equals(1);
        expect(rows[0].first_name).equals(FIRST_NAME);
        expect(rows[0].last_name).equals(LAST_NAME);
        expect(rows[0].age).equals(AGE);

    })

    it("should update no rows on mismatch", async () => {

        const count = await db.update(TEST_TABLE, {
            started_date: "2020-07-04"
        }, {
            clause: "last_name = 'whoareyou'"
        } );

        expect(count).equals(0);

    })


})
