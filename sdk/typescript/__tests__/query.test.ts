import { Client, Cursor, DatabaseDocument, r, TermType } from '../src/index';

// Mock the client and cursor
jest.mock('../src/client');
jest.mock('../src/cursor');

describe('Query Operations', () => {
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

  describe('Query context and chaining', () => {
    it('should maintain query context through operations', () => {
      const table = r.db('testdb').table('users');
      const getQuery = table.get('user123');

      expect(getQuery._context).toBe('query');

      const deleteQuery = getQuery.delete();
      expect(deleteQuery._context).toBe('query');
    });

    it('should chain filter operations', () => {
      const table = r.db('testdb').table('users');
      const ageFilter = r.row('age').gt(18);
      const statusFilter = r.row('status').eq('active');

      const filterQuery = table.filter(ageFilter);
      expect(filterQuery._context).toBe('query');

      const chainedFilter = filterQuery.filter(statusFilter);
      expect(chainedFilter._context).toBe('query');
    });
  });

  describe('Filter query operations', () => {
    let table: any;

    beforeEach(() => {
      table = r.db('testdb').table('users');
    });

    it('should create filter query with simple predicate', () => {
      const predicate = r.row('age').gt(18);
      const filterQuery = table.filter(predicate);

      expect(filterQuery).toBeDefined();
      expect(typeof filterQuery.filter).toBe('function');
      expect(typeof filterQuery.run).toBe('function');
      expect(typeof filterQuery.debug).toBe('function');

      const ast = filterQuery.toAST();
      expect(ast[0]).toBe(TermType.Filter);
      expect(ast[1]).toHaveLength(2);
    });

    it('should create filter query with complex predicate', () => {
      const agePredicate = r.row('age').gt(18);
      const namePredicate = r.row('name').ne('admin');
      const complexPredicate = r.and(agePredicate, namePredicate);

      const filterQuery = table.filter(complexPredicate);
      expect(filterQuery).toBeDefined();

      const ast = filterQuery.toAST();
      expect(ast[0]).toBe(TermType.Filter);
    });

    it('should chain multiple filter operations', () => {
      const ageFilter = r.row('age').gt(18);
      const nameFilter = r.row('name').eq('John');
      const statusFilter = r.row('status').eq('active');

      const chainedQuery = table.filter(ageFilter).filter(nameFilter).filter(statusFilter);

      expect(chainedQuery).toBeDefined();
      const ast = chainedQuery.toAST();
      expect(ast[0]).toBe(TermType.Filter);
    });

    it('should execute filter query with cursor', () => {
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
        name: string;
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

  describe('Get query operations', () => {
    let table: any;

    beforeEach(() => {
      table = r.db('testdb').table('users');
    });

    it('should create get query', () => {
      const getQuery = table.get('user123');

      expect(getQuery).toBeDefined();
      expect(getQuery._context).toBe('query');
      expect(typeof getQuery.run).toBe('function');
      expect(typeof getQuery.delete).toBe('function');
      expect(typeof getQuery.debug).toBe('function');

      const ast = getQuery.toAST();
      expect(ast[0]).toBe(TermType.Get);
      expect(ast[1]).toHaveLength(2);
      expect(ast[1][1]).toBe('user123');
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

    it('should create delete query from get', () => {
      const getQuery = table.get('user123');
      const deleteQuery = getQuery.delete();

      expect(deleteQuery).toBeDefined();
      expect(deleteQuery._context).toBe('query');

      const ast = deleteQuery.toAST();
      expect(ast[0]).toBe(TermType.Delete);
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

  describe('Insert query operations', () => {
    let table: any;

    beforeEach(() => {
      table = r.db('testdb').table('users');
    });

    it('should create insert query with single document', () => {
      const document = { name: 'John Doe', age: 30 };
      const insertQuery = table.insert(document);

      expect(insertQuery).toBeDefined();
      expect(insertQuery._context).toBe('query');
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

    it('should execute insert query with generic return type', async () => {
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

  describe('Delete query operations', () => {
    let table: any;

    beforeEach(() => {
      table = r.db('testdb').table('users');
    });

    it('should create delete query from table', () => {
      const deleteQuery = table.delete();

      expect(deleteQuery).toBeDefined();
      expect(deleteQuery._context).toBe('query');
      expect(typeof deleteQuery.run).toBe('function');
      expect(typeof deleteQuery.debug).toBe('function');

      const ast = deleteQuery.toAST();
      expect(ast[0]).toBe(TermType.Delete);
      expect(ast[1]).toHaveLength(1);
    });

    it('should create delete query from get', () => {
      const getQuery = table.get('user123');
      const deleteQuery = getQuery.delete();

      expect(deleteQuery).toBeDefined();

      const ast = deleteQuery.toAST();
      expect(ast[0]).toBe(TermType.Delete);
    });

    it('should execute delete query', async () => {
      const expectedResult = { deleted: 5 };
      mockClient.send.mockResolvedValue(expectedResult);

      const deleteQuery = table.delete();
      const result = await deleteQuery.run(mockClient);

      expect(result).toEqual(expectedResult);
      expect(mockClient.send).toHaveBeenCalledWith(deleteQuery.toAST());
    });

    it('should execute delete query with generic type', async () => {
      const customResult = { success: true, count: 5 };
      mockClient.send.mockResolvedValue(customResult);

      const deleteQuery = table.delete();
      const result = await deleteQuery.run(mockClient);

      expect(result).toEqual(customResult);
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

  describe('Query execution patterns', () => {
    let table: any;

    beforeEach(() => {
      table = r.db('testdb').table('users');
    });

    it('should determine cursor vs direct result based on operation type', () => {
      // Operations that should return cursors
      const filterQuery = table.filter(r.row('age').gt(18));
      const tableQuery = table;

      const filterResult = filterQuery.run(mockClient);
      const tableResult = tableQuery.run(mockClient);

      expect(filterResult).toBe(mockCursor);
      expect(tableResult).toBe(mockCursor);
    });

    it('should handle async operations correctly', async () => {
      // Operations that should return promises
      const getQuery = table.get('user123');
      const insertQuery = table.insert({ name: 'John' });
      const deleteQuery = table.delete();

      const user = { id: 'user123', name: 'John' };
      const insertResult = { inserted: 1 };
      const deleteResult = { deleted: 1 };

      mockClient.send
        .mockResolvedValueOnce(user)
        .mockResolvedValueOnce(insertResult)
        .mockResolvedValueOnce(deleteResult);

      const getUserResult = await getQuery.run(mockClient);
      const insertUserResult = await insertQuery.run(mockClient);
      const deleteUserResult = await deleteQuery.run(mockClient);

      expect(getUserResult).toEqual(user);
      expect(insertUserResult).toEqual(insertResult);
      expect(deleteUserResult).toEqual(deleteResult);
    });
  });

  describe('Error handling in queries', () => {
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

    it('should handle filter query cursor errors', () => {
      const error = new Error('Filter failed');
      (Cursor as jest.MockedClass<typeof Cursor>).mockImplementation(() => {
        throw error;
      });

      const filterQuery = table.filter(r.row('age').gt(18));
      expect(() => filterQuery.run(mockClient)).toThrow('Filter failed');
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

  describe('Complex query composition', () => {
    let table: any;

    beforeEach(() => {
      table = r.db('testdb').table('users');
    });

    it('should compose complex filter chains', () => {
      const baseFilter = r.row('status').eq('active');
      const ageFilter = r.row('age').ge(18);
      const nameFilter = r.row('name').ne('admin');
      const emailFilter = r.row('email').ne('');

      const complexQuery = table
        .filter(baseFilter)
        .filter(ageFilter)
        .filter(nameFilter)
        .filter(emailFilter);

      expect(complexQuery).toBeDefined();
      const ast = complexQuery.toAST();
      expect(ast[0]).toBe(TermType.Filter);
    });

    it('should handle nested query structures', () => {
      const innerCondition = r.and(r.row('age').gt(18), r.row('verified').eq(true));
      const outerCondition = r.or(innerCondition, r.row('premium').eq(true));

      const complexQuery = table.filter(outerCondition);
      expect(complexQuery).toBeDefined();

      const ast = complexQuery.toAST();
      expect(ast[0]).toBe(TermType.Filter);
    });

    it('should support query composition with different operations', () => {
      // Create a query that filters, then gets specific document
      const filterQuery = table.filter(r.row('status').eq('active'));
      expect(filterQuery).toBeDefined();

      // Chain operations maintain proper types
      const result = filterQuery.run(mockClient);
      expect(result).toBe(mockCursor);
    });

    it('should maintain type safety across query chains', () => {
      interface User extends DatabaseDocument {
        id: string;
        name: string;
        age: number;
        status: 'active' | 'inactive';
      }

      const typedQuery = table.filter(r.row('status').eq('active')).filter(r.row('age').gt(18));

      expect(typedQuery).toBeDefined();
      const result = typedQuery.run(mockClient);
      expect(result).toBe(mockCursor);
    });
  });

  describe('Query debugging and introspection', () => {
    let table: any;

    beforeEach(() => {
      table = r.db('testdb').table('users');
    });

    it('should debug all query types', () => {
      const consoleSpy = jest.spyOn(console, 'dir').mockImplementation();

      const queries = [
        table.get('user123'),
        table.filter(r.row('age').gt(18)),
        table.insert({ name: 'John' }),
        table.delete()
      ];

      queries.forEach((query) => {
        const debugResult = query.debug();
        expect(debugResult).toBeDefined();
        expect(typeof debugResult.run).toBe('function');
      });

      expect(consoleSpy).toHaveBeenCalledTimes(4);
      consoleSpy.mockRestore();
    });

    it('should provide AST introspection for complex queries', () => {
      const complexQuery = table.filter(r.row('age').gt(18)).filter(r.row('status').eq('active'));

      const ast = complexQuery.toAST();
      expect(ast).toBeDefined();
      expect(Array.isArray(ast)).toBe(true);
      expect(ast[0]).toBe(TermType.Filter);
      expect(Array.isArray(ast[1])).toBe(true);
    });
  });
});
