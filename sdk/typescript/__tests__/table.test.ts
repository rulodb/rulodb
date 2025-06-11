import { Client, Cursor, DatabaseDocument, r, TermType } from '../src/index';

// Mock the client and cursor
jest.mock('../src/client');
jest.mock('../src/cursor');

describe('Table Operations', () => {
  let mockClient: jest.Mocked<Client>;
  let mockCursor: jest.Mocked<Cursor<any>>;

  beforeEach(() => {
    mockClient = {
      send: jest.fn(),
      close: jest.fn()
    } as any;

    mockCursor = {
      toArray: jest.fn(),
      [Symbol.asyncIterator]: jest.fn(),
      return: jest.fn(),
      close: jest.fn(),
      executeImmediate: jest.fn()
    } as any;

    // Mock Cursor constructor
    (Cursor as jest.MockedClass<typeof Cursor>).mockImplementation(() => mockCursor);
  });

  describe('Table access', () => {
    it('should create table reference from database', () => {
      const table = r.db('testdb').table('users');
      expect(table).toBeDefined();
      expect(table._context).toBe('table');

      const ast = table.toAST();
      expect(ast[0]).toBe(TermType.Table);
      expect(ast[1]).toHaveLength(2);
      expect(ast[1][1]).toBe('users');
    });

    it('should create table reference with complex names', () => {
      const table = r.db('my-app').table('user_profiles');
      expect(table).toBeDefined();

      const ast = table.toAST();
      expect(ast[0]).toBe(TermType.Table);
      expect(ast[1][1]).toBe('user_profiles');
    });
  });

  describe('Get operations', () => {
    let table: any;

    beforeEach(() => {
      table = r.db('testdb').table('users');
    });

    it('should create get query with string key', () => {
      const getQuery = table.get('user123');
      expect(getQuery).toBeDefined();
      expect(typeof getQuery.run).toBe('function');
      expect(typeof getQuery.delete).toBe('function');
      expect(typeof getQuery.debug).toBe('function');

      const ast = getQuery.toAST();
      expect(ast[0]).toBe(TermType.Get);
      expect(ast[1]).toHaveLength(2);
      expect(ast[1][1]).toBe('user123');
    });

    it('should create get query with numeric key', () => {
      const getQuery = table.get(42);
      expect(getQuery).toBeDefined();

      const ast = getQuery.toAST();
      expect(ast[0]).toBe(TermType.Get);
      expect(ast[1][1]).toBe(42);
    });

    it('should execute get query', async () => {
      const expectedUser = { id: 'user123', name: 'John Doe', age: 30 };
      mockClient.send.mockResolvedValue(expectedUser);

      const getQuery = table.get('user123');
      const result = await getQuery.run(mockClient);

      expect(result).toEqual(expectedUser);
      expect(mockClient.send).toHaveBeenCalledWith(getQuery.toAST());
    });

    it('should execute get query with generic type', async () => {
      interface User extends DatabaseDocument {
        id: string;
        name: string;
        age: number;
      }

      const expectedUser: User = { id: 'user123', name: 'John Doe', age: 30 };
      mockClient.send.mockResolvedValue(expectedUser);

      const getQuery = table.get('user123');
      const result = await getQuery.run(mockClient);

      expect(result).toEqual(expectedUser);
    });

    it('should debug get query', () => {
      const consoleSpy = jest.spyOn(console, 'dir').mockImplementation();

      const getQuery = table.get('user123');
      const debugResult = getQuery.debug();

      expect(consoleSpy).toHaveBeenCalledWith(getQuery.toAST(), { depth: null });
      expect(typeof debugResult.run).toBe('function');

      consoleSpy.mockRestore();
    });
  });

  describe('Filter operations', () => {
    let table: any;

    beforeEach(() => {
      table = r.db('testdb').table('users');
    });

    it('should create filter query', () => {
      const predicate = r.row('age').gt(18);
      const filterQuery = table.filter(predicate);

      expect(filterQuery).toBeDefined();
      expect(typeof filterQuery.run).toBe('function');
      expect(typeof filterQuery.filter).toBe('function');
      expect(typeof filterQuery.debug).toBe('function');

      const ast = filterQuery.toAST();
      expect(ast[0]).toBe(TermType.Filter);
      expect(ast[1]).toHaveLength(2);
    });

    it('should chain multiple filters', () => {
      const ageFilter = r.row('age').gt(18);
      const nameFilter = r.row('name').eq('John');

      const filterQuery = table.filter(ageFilter).filter(nameFilter);
      expect(filterQuery).toBeDefined();

      const ast = filterQuery.toAST();
      expect(ast[0]).toBe(TermType.Filter);
    });

    it('should execute filter query returning cursor', () => {
      const predicate = r.row('age').gt(18);
      const filterQuery = table.filter(predicate);

      const result = filterQuery.run(mockClient);
      expect(result).toBe(mockCursor);
      expect(Cursor).toHaveBeenCalledWith(mockClient, expect.any(Object), undefined);
    });

    it('should execute filter query with options', () => {
      const predicate = r.row('age').gt(18);
      const filterQuery = table.filter(predicate);
      const options = { batchSize: 100 };

      const result = filterQuery.run(mockClient, options);
      expect(result).toBe(mockCursor);
      expect(Cursor).toHaveBeenCalledWith(mockClient, expect.any(Object), options);
    });

    it('should execute filter query with generic type', () => {
      interface User extends DatabaseDocument {
        id: string;
        age: number;
      }

      const predicate = r.row('age').gt(18);
      const filterQuery = table.filter(predicate);

      const result = filterQuery.run(mockClient);
      expect(result).toBe(mockCursor);
    });

    it('should debug filter query', () => {
      const consoleSpy = jest.spyOn(console, 'dir').mockImplementation();

      const predicate = r.row('age').gt(18);
      const filterQuery = table.filter(predicate);
      const debugResult = filterQuery.debug();

      expect(consoleSpy).toHaveBeenCalledWith(filterQuery.toAST(), { depth: null });
      expect(typeof debugResult.run).toBe('function');

      consoleSpy.mockRestore();
    });
  });

  describe('Insert operations', () => {
    let table: any;

    beforeEach(() => {
      table = r.db('testdb').table('users');
    });

    it('should create insert query with single document', () => {
      const document = { name: 'John Doe', age: 30 };
      const insertQuery = table.insert(document);

      expect(insertQuery).toBeDefined();
      expect(typeof insertQuery.run).toBe('function');
      expect(typeof insertQuery.debug).toBe('function');

      const ast = insertQuery.toAST();
      expect(ast[0]).toBe(TermType.Insert);
      expect(ast[1]).toHaveLength(2);
      expect(ast[1][1]).toEqual(document);
    });

    it('should create insert query with multiple documents', () => {
      const documents = [
        { name: 'John Doe', age: 30 },
        { name: 'Jane Smith', age: 25 }
      ];
      const insertQuery = table.insert(documents);

      expect(insertQuery).toBeDefined();

      const ast = insertQuery.toAST();
      expect(ast[0]).toBe(TermType.Insert);
      expect(ast[1][1]).toEqual(documents);
    });

    it('should execute insert query', async () => {
      const document = { name: 'John Doe', age: 30 };
      const expectedResult = { inserted: 1, generated_keys: ['user123'] };
      mockClient.send.mockResolvedValue(expectedResult);

      const insertQuery = table.insert(document);
      const result = await insertQuery.run(mockClient);

      expect(result).toEqual(expectedResult);
      expect(mockClient.send).toHaveBeenCalledWith(insertQuery.toAST());
    });

    it('should execute insert query with generic type', async () => {
      const document = { name: 'John Doe', age: 30 };
      const customResult = { success: true, id: 'user123' };
      mockClient.send.mockResolvedValue(customResult);

      const insertQuery = table.insert(document);
      const result = await insertQuery.run(mockClient);

      expect(result).toEqual(customResult);
    });

    it('should debug insert query', () => {
      const consoleSpy = jest.spyOn(console, 'dir').mockImplementation();

      const document = { name: 'John Doe', age: 30 };
      const insertQuery = table.insert(document);
      const debugResult = insertQuery.debug();

      expect(consoleSpy).toHaveBeenCalledWith(insertQuery.toAST(), { depth: null });
      expect(typeof debugResult.run).toBe('function');

      consoleSpy.mockRestore();
    });
  });

  describe('Delete operations', () => {
    let table: any;

    beforeEach(() => {
      table = r.db('testdb').table('users');
    });

    it('should create table delete query', () => {
      const deleteQuery = table.delete();

      expect(deleteQuery).toBeDefined();
      expect(typeof deleteQuery.run).toBe('function');
      expect(typeof deleteQuery.debug).toBe('function');

      const ast = deleteQuery.toAST();
      expect(ast[0]).toBe(TermType.Delete);
      expect(ast[1]).toHaveLength(1);
    });

    it('should create document delete query from get', () => {
      const getQuery = table.get('user123');
      const deleteQuery = getQuery.delete();

      expect(deleteQuery).toBeDefined();

      const ast = deleteQuery.toAST();
      expect(ast[0]).toBe(TermType.Delete);
    });

    it('should execute table delete query', async () => {
      const expectedResult = { deleted: 5 };
      mockClient.send.mockResolvedValue(expectedResult);

      const deleteQuery = table.delete();
      const result = await deleteQuery.run(mockClient);

      expect(result).toEqual(expectedResult);
      expect(mockClient.send).toHaveBeenCalledWith(deleteQuery.toAST());
    });

    it('should execute document delete query', async () => {
      const expectedResult = { deleted: 1 };
      mockClient.send.mockResolvedValue(expectedResult);

      const deleteQuery = table.get('user123').delete();
      const result = await deleteQuery.run(mockClient);

      expect(result).toEqual(expectedResult);
    });

    it('should debug delete query', () => {
      const consoleSpy = jest.spyOn(console, 'dir').mockImplementation();

      const deleteQuery = table.delete();
      const debugResult = deleteQuery.debug();

      expect(consoleSpy).toHaveBeenCalledWith(deleteQuery.toAST(), { depth: null });
      expect(typeof debugResult.run).toBe('function');

      consoleSpy.mockRestore();
    });
  });

  describe('Table execution', () => {
    let table: any;

    beforeEach(() => {
      table = r.db('testdb').table('users');
    });

    it('should execute table query returning cursor', () => {
      const result = table.run(mockClient);
      expect(result).toBe(mockCursor);
      expect(Cursor).toHaveBeenCalledWith(mockClient, table, undefined);
    });

    it('should execute table query with options', () => {
      const options = { batchSize: 50 };
      const result = table.run(mockClient, options);
      expect(result).toBe(mockCursor);
      expect(Cursor).toHaveBeenCalledWith(mockClient, table, options);
    });

    it('should execute table query with generic type', () => {
      interface User extends DatabaseDocument {
        name: string;
        age: number;
      }

      const result = table.run(mockClient);
      expect(result).toBe(mockCursor);
    });

    it('should debug table query', () => {
      const consoleSpy = jest.spyOn(console, 'dir').mockImplementation();

      const debugResult = table.debug();
      expect(debugResult).toBe(table);
      expect(consoleSpy).toHaveBeenCalledWith(table.toAST(), { depth: null });

      consoleSpy.mockRestore();
    });
  });

  describe('Error handling', () => {
    let table: any;

    beforeEach(() => {
      table = r.db('testdb').table('users');
    });

    it('should handle get query errors', async () => {
      const error = new Error('Document not found');
      mockClient.send.mockRejectedValue(error);

      const getQuery = table.get('nonexistent');
      await expect(getQuery.run(mockClient)).rejects.toThrow('Document not found');
    });

    it('should handle insert query errors', async () => {
      const error = new Error('Insert failed');
      mockClient.send.mockRejectedValue(error);

      const insertQuery = table.insert({ name: 'John' });
      await expect(insertQuery.run(mockClient)).rejects.toThrow('Insert failed');
    });

    it('should handle delete query errors', async () => {
      const error = new Error('Delete failed');
      mockClient.send.mockRejectedValue(error);

      const deleteQuery = table.delete();
      await expect(deleteQuery.run(mockClient)).rejects.toThrow('Delete failed');
    });
  });

  describe('Complex table operations', () => {
    let table: any;

    beforeEach(() => {
      table = r.db('testdb').table('users');
    });

    it('should chain get and delete operations', () => {
      const deleteQuery = table.get('user123').delete();
      expect(deleteQuery).toBeDefined();

      const ast = deleteQuery.toAST();
      expect(ast[0]).toBe(TermType.Delete);

      // Verify the nested structure
      const getArg = ast[1][0];
      // The argument should be resolved to its AST form
      if (Array.isArray(getArg)) {
        expect(getArg[0]).toBe(TermType.Get);
      } else {
        expect(getArg).toHaveProperty('toAST');
        if (typeof getArg === 'object' && getArg && 'toAST' in getArg) {
          const getAst = (getArg as any).toAST();
          expect(getAst[0]).toBe(TermType.Get);
        }
      }
    });

    it('should handle complex filter chains', () => {
      const ageFilter = r.row('age').gt(18);
      const statusFilter = r.row('status').eq('active');
      const nameFilter = r.row('name').ne('admin');

      const complexFilter = table.filter(ageFilter).filter(statusFilter).filter(nameFilter);

      expect(complexFilter).toBeDefined();
      const ast = complexFilter.toAST();
      expect(ast[0]).toBe(TermType.Filter);
    });

    it('should handle batch operations', () => {
      const users = Array.from({ length: 100 }, (_, i) => ({
        name: `User ${i}`,
        age: 20 + i,
        email: `user${i}@example.com`
      }));

      const insertQuery = table.insert(users);
      expect(insertQuery).toBeDefined();

      const ast = insertQuery.toAST();
      expect(ast[0]).toBe(TermType.Insert);
      expect(ast[1][1]).toHaveLength(100);
    });
  });
});
