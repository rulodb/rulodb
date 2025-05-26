import { DatabaseBuilder, db } from "../src/db";
import { TableBuilder } from "../src/table";
import { TermType } from "../src/terms";

describe("DatabaseBuilder", () => {
  it("should build a database term (currently Table term)", () => {
    const builder = new DatabaseBuilder("testdb");
    expect(builder.build()).toEqual([TermType.Table, ["testdb"], {}]);
  });

  it("should build a TableCreate term", () => {
    const builder = new DatabaseBuilder("testdb");
    const tableCreate = builder.createTable("users");
    expect(tableCreate.build()).toEqual([TermType.TableCreate, ["users"], {}]);
  });

  it("should build a TableList term", () => {
    const builder = new DatabaseBuilder("testdb");
    const tableList = builder.listTables();
    expect(tableList.build()).toEqual([TermType.TableList, [], {}]);
  });

  it("should build a TableDrop term", () => {
    const builder = new DatabaseBuilder("testdb");
    const tableDrop = builder.dropTable("users");
    expect(tableDrop.build()).toEqual([TermType.TableDrop, ["users"], {}]);
  });

  it("should return a TableBuilder from table()", () => {
    const builder = new DatabaseBuilder("testdb");
    const tableBuilder = builder.table("users");
    expect(tableBuilder).toBeInstanceOf(TableBuilder);
    expect(tableBuilder.build()).toEqual([TermType.Table, ["users"], {}]);
  });
});

describe("db factory", () => {
  it("should return a DatabaseBuilder", () => {
    const builder = db("mydb");
    expect(builder).toBeInstanceOf(DatabaseBuilder);
    expect(builder.build()).toEqual([TermType.Table, ["mydb"], {}]);
  });
});
