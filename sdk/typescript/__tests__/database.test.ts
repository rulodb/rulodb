import { Client, r, TermType } from '../src/index';

// Mock the client
jest.mock('../src/client');

describe('Database Operations', () => {
  let mockClient: jest.Mocked<Client>;

  beforeEach(() => {
    mockClient = {
      send: jest.fn(),
      close: jest.fn()
    } as any;
  });

  describe('Database creation and management', () => {
    it('should create r.db() with default database name', () => {
      const db = r.db();
      expect(db).toBeDefined();
      expect(db._context).toBe('database');

      const ast = db.toAST();
      expect(ast[0]).toBe(TermType.Database);
      expect(ast[1]).toEqual(['default']);
    });

    it('should create r.db() with specified database name', () => {
      const db = r.db('mydb');
      expect(db).toBeDefined();
      expect(db._context).toBe('database');

      const ast = db.toAST();
      expect(ast[0]).toBe(TermType.Database);
      expect(ast[1]).toEqual(['mydb']);
    });

    it('should create database creation term', () => {
      const createDb = r.createDatabase('newdb');
      expect(createDb).toBeDefined();
      expect(createDb._context).toBe('database');

      const ast = createDb.toAST();
      expect(ast[0]).toBe(TermType.DatabaseCreate);
      expect(ast[1]).toEqual(['newdb']);
    });

    it('should create database drop term', () => {
      const dropDb = r.dropDatabase('olddb');
      expect(dropDb).toBeDefined();
      expect(dropDb._context).toBe('database');

      const ast = dropDb.toAST();
      expect(ast[0]).toBe(TermType.DatabaseDrop);
      expect(ast[1]).toEqual(['olddb']);
    });

    it('should create database list term', () => {
      const listDbs = r.listDatabases();
      expect(listDbs).toBeDefined();
      expect(listDbs._context).toBe('database');

      const ast = listDbs.toAST();
      expect(ast[0]).toBe(TermType.DatabaseList);
      expect(ast[1]).toEqual([]);
    });
  });

  describe('Database methods', () => {
    let db: ReturnType<typeof r.db>;

    beforeEach(() => {
      db = r.db('testdb');
    });

    it('should create table reference', () => {
      const table = db.table('users');
      expect(table).toBeDefined();
      expect(table._context).toBe('table');

      const ast = table.toAST();
      expect(ast[0]).toBe(TermType.Table);
      expect(ast[1]).toHaveLength(2);
      expect(ast[1][1]).toBe('users');
    });

    it('should create table list query', () => {
      const listTables = db.listTables();
      expect(listTables).toBeDefined();
      expect(typeof listTables.run).toBe('function');
      expect(typeof listTables.debug).toBe('function');

      const ast = listTables.toAST();
      expect(ast[0]).toBe(TermType.TableList);
    });

    it('should create table creation query', () => {
      const createTable = db.createTable('newtable');
      expect(createTable).toBeDefined();
      expect(typeof createTable.run).toBe('function');
      expect(typeof createTable.debug).toBe('function');

      const ast = createTable.toAST();
      expect(ast[0]).toBe(TermType.TableCreate);
      expect(ast[1]).toHaveLength(2);
      expect(ast[1][1]).toBe('newtable');
    });

    it('should create table drop query', () => {
      const dropTable = db.dropTable('oldtable');
      expect(dropTable).toBeDefined();
      expect(typeof dropTable.run).toBe('function');
      expect(typeof dropTable.debug).toBe('function');

      const ast = dropTable.toAST();
      expect(ast[0]).toBe(TermType.TableDrop);
      expect(ast[1]).toHaveLength(2);
      expect(ast[1][1]).toBe('oldtable');
    });

    it('should execute database query', async () => {
      mockClient.send.mockResolvedValue({ result: 'success' });

      const result = await db.run(mockClient);
      expect(result).toEqual({ result: 'success' });
      expect(mockClient.send).toHaveBeenCalledWith(db.toAST());
    });

    it('should debug database query', () => {
      const consoleSpy = jest.spyOn(console, 'dir').mockImplementation();

      const debugResult = db.debug();
      expect(debugResult).toBe(db);
      expect(consoleSpy).toHaveBeenCalledWith(db.toAST(), { depth: null });

      consoleSpy.mockRestore();
    });
  });

  describe('Table queries execution', () => {
    let db: ReturnType<typeof r.db>;

    beforeEach(() => {
      db = r.db('testdb');
    });

    it('should execute listTables query', async () => {
      const expectedTables = ['users', 'posts', 'comments'];
      mockClient.send.mockResolvedValue(expectedTables);

      const listTablesQuery = db.listTables();
      const result = await listTablesQuery.run(mockClient);

      expect(result).toEqual(expectedTables);
      expect(mockClient.send).toHaveBeenCalledWith(listTablesQuery.toAST());
    });

    it('should execute listTables query with generic type', async () => {
      const expectedResult = { tables: ['users'] };
      mockClient.send.mockResolvedValue(expectedResult);

      const listTablesQuery = db.listTables();
      const result = await listTablesQuery.run<typeof expectedResult>(mockClient);

      expect(result).toEqual(expectedResult);
    });

    it('should execute createTable query', async () => {
      const expectedResult = { created: true };
      mockClient.send.mockResolvedValue(expectedResult);

      const createTableQuery = db.createTable('newtable');
      const result = await createTableQuery.run(mockClient);

      expect(result).toEqual(expectedResult);
      expect(mockClient.send).toHaveBeenCalledWith(createTableQuery.toAST());
    });

    it('should execute dropTable query', async () => {
      const expectedResult = { dropped: true };
      mockClient.send.mockResolvedValue(expectedResult);

      const dropTableQuery = db.dropTable('oldtable');
      const result = await dropTableQuery.run(mockClient);

      expect(result).toEqual(expectedResult);
      expect(mockClient.send).toHaveBeenCalledWith(dropTableQuery.toAST());
    });

    it('should debug table queries', () => {
      const consoleSpy = jest.spyOn(console, 'dir').mockImplementation();

      const listTablesQuery = db.listTables();
      const debugResult = listTablesQuery.debug();

      expect(consoleSpy).toHaveBeenCalledWith(listTablesQuery.toAST(), { depth: null });
      expect(debugResult).toBeInstanceOf(Object);
      expect(typeof debugResult.run).toBe('function');

      consoleSpy.mockRestore();
    });
  });

  describe('Error handling', () => {
    let db: ReturnType<typeof r.db>;

    beforeEach(() => {
      db = r.db('testdb');
    });

    it('should handle database query execution errors', async () => {
      const error = new Error('Database connection failed');
      mockClient.send.mockRejectedValue(error);

      await expect(db.run(mockClient)).rejects.toThrow('Database connection failed');
    });

    it('should handle table operation errors', async () => {
      const error = new Error('Table creation failed');
      mockClient.send.mockRejectedValue(error);

      const createTableQuery = db.createTable('newtable');
      await expect(createTableQuery.run(mockClient)).rejects.toThrow('Table creation failed');
    });
  });

  describe('Complex database operations', () => {
    it('should chain database and table operations', () => {
      const table = r.db('myapp').table('users');
      expect(table).toBeDefined();
      expect(table._context).toBe('table');

      const ast = table.toAST();
      expect(ast[0]).toBe(TermType.Table);

      // Check that the database is properly nested
      const dbArg = ast[1][0];
      // The argument should be resolved to its AST form
      if (Array.isArray(dbArg)) {
        expect(dbArg[0]).toBe(TermType.Database);
        expect(dbArg[1]).toEqual(['myapp']);
      } else {
        expect(dbArg).toHaveProperty('toAST');
        if (typeof dbArg === 'object' && dbArg && 'toAST' in dbArg) {
          const dbAst = (dbArg as any).toAST();
          expect(dbAst[0]).toBe(TermType.Database);
          expect(dbAst[1]).toEqual(['myapp']);
        }
      }
    });

    it('should handle multiple database operations', () => {
      const operations = [
        r.createDatabase('db1'),
        r.createDatabase('db2'),
        r.dropDatabase('olddb')
      ];

      operations.forEach((op, index) => {
        expect(op).toBeDefined();
        expect(op._context).toBe('database');

        const ast = op.toAST();
        if (index < 2) {
          expect(ast[0]).toBe(TermType.DatabaseCreate);
        } else {
          expect(ast[0]).toBe(TermType.DatabaseDrop);
        }
      });
    });
  });
});
