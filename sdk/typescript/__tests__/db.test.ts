import { createDatabase, DatabaseBuilder, db, dropDatabase, listDatabases } from '../src/db';
import { TableBuilder } from '../src/table';
import { TermType } from '../src/terms';

describe('DatabaseBuilder', () => {
  it('should build a database term (currently Table term)', () => {
    const builder = new DatabaseBuilder('testdb');
    expect(builder.build()).toEqual([TermType.Database, ['testdb']]);
  });

  it('should build a DatabaseCreate term', () => {
    const databaseCreate = createDatabase('newdb');
    expect(databaseCreate.build()).toEqual([TermType.DatabaseCreate, ['newdb']]);
  });

  it('should build a DatabaseDrop term', () => {
    const databaseDrop = dropDatabase('newdb');
    expect(databaseDrop.build()).toEqual([TermType.DatabaseDrop, ['newdb']]);
  });

  it('should build a DatabaseList term', () => {
    const databaseList = listDatabases();
    expect(databaseList.build()).toEqual([TermType.DatabaseList, []]);
  });

  it('should build a TableCreate term', () => {
    const builder = new DatabaseBuilder('testdb');
    const tableCreate = builder.createTable('users');
    expect(tableCreate.build()).toEqual([
      TermType.TableCreate,
      [[TermType.Database, ['testdb']], 'users']
    ]);
  });

  it('should build a TableList term', () => {
    const builder = new DatabaseBuilder('testdb');
    const tableList = builder.listTables();
    expect(tableList.build()).toEqual([TermType.TableList, [[TermType.Database, ['testdb']]]]);
  });

  it('should build a TableDrop term', () => {
    const builder = new DatabaseBuilder('testdb');
    const tableDrop = builder.dropTable('users');
    expect(tableDrop.build()).toEqual([
      TermType.TableDrop,
      [[TermType.Database, ['testdb']], 'users']
    ]);
  });

  it('should return a TableBuilder from table()', () => {
    const builder = new DatabaseBuilder('testdb');
    const tableBuilder = builder.table('users');
    expect(tableBuilder).toBeInstanceOf(TableBuilder);
    expect(tableBuilder.build()).toEqual([
      TermType.Table,
      [[TermType.Database, ['testdb']], 'users']
    ]);
  });
});

describe('db factory', () => {
  it('should return a DatabaseBuilder', () => {
    const builder = db('mydb');
    expect(builder).toBeInstanceOf(DatabaseBuilder);
    expect(builder.build()).toEqual([TermType.Database, ['mydb']]);
  });

  it("should properly chain db and table for r.db('__system__').table('users')", () => {
    const dbTable = db('__system__').table('users');
    expect(dbTable).toBeInstanceOf(TableBuilder);
    expect(dbTable.build()).toEqual([
      TermType.Table,
      [[TermType.Database, ['__system__']], 'users']
    ]);
  });
});
