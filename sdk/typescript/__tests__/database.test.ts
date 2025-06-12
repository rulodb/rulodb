import { Connection } from '../src/connection';
import { r, RQuery } from '../src/query';

// Mock the Connection class
jest.mock('../src/connection');

describe('Database Operations', () => {
  let mockConnection: jest.Mocked<Connection>;

  beforeEach(() => {
    jest.clearAllMocks();
    mockConnection = {
      query: jest.fn(),
      connect: jest.fn(),
      disconnect: jest.fn(),
      isConnected: jest.fn().mockReturnValue(true),
      on: jest.fn(),
      off: jest.fn()
    } as any;
  });

  describe('db()', () => {
    it('should create a database query with default name', () => {
      const query = r.db();
      expect(query).toBeInstanceOf(RQuery);
      expect(query._dbName).toBe('default');
    });

    it('should create a database query with specified name', () => {
      const query = r.db('mydb');
      expect(query).toBeInstanceOf(RQuery);
      expect(query._dbName).toBe('mydb');
    });

    it('should create a database query with correct state type', () => {
      const query = r.db('testdb');
      // The state should be DatabaseQuery type
      expect(query).toBeInstanceOf(RQuery);
    });

    it('should allow chaining table operations', () => {
      const query = r.db('mydb').table('users');
      expect(query).toBeInstanceOf(RQuery);
    });
  });

  describe('dbCreate()', () => {
    it('should create database creation query', () => {
      const query = r.dbCreate('newdb');
      expect(query).toBeInstanceOf(RQuery);
      expect(query._query).toEqual({
        databaseCreate: { name: 'newdb' }
      });
    });

    it('should execute database creation successfully', async () => {
      const mockResult = {
        result: { created: 1 }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.dbCreate('newdb');
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
        databaseCreate: { name: 'newdb' }
      });
      expect(result).toEqual({ created: 1 });
    });

    it('should handle database creation errors', async () => {
      const error = new Error('Database already exists');
      mockConnection.query.mockRejectedValue(error);

      const query = r.dbCreate('existingdb');

      await expect(query.run(mockConnection)).rejects.toThrow('Database already exists');
    });

    it('should create query with correct return type', () => {
      const query = r.dbCreate('newdb');
      // Should return ValueQuery<{ created: number }>
      expect(query).toBeInstanceOf(RQuery);
    });
  });

  describe('dbDrop()', () => {
    it('should create database drop query', () => {
      const query = r.dbDrop('olddb');
      expect(query).toBeInstanceOf(RQuery);
      expect(query._query).toEqual({
        databaseDrop: { name: 'olddb' }
      });
    });

    it('should execute database drop successfully', async () => {
      const mockResult = {
        result: { dropped: 1 }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.dbDrop('olddb');
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
        databaseDrop: { name: 'olddb' }
      });
      expect(result).toEqual({ dropped: 1 });
    });

    it('should handle database drop errors', async () => {
      const error = new Error('Database not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r.dbDrop('nonexistent');

      await expect(query.run(mockConnection)).rejects.toThrow('Database not found');
    });

    it('should create query with correct return type', () => {
      const query = r.dbDrop('olddb');
      // Should return ValueQuery<{ dropped: number }>
      expect(query).toBeInstanceOf(RQuery);
    });
  });

  describe('dbList()', () => {
    it('should create database list query', () => {
      const query = r.dbList();
      expect(query).toBeInstanceOf(RQuery);
      expect(query._query).toEqual({
        databaseList: {}
      });
    });

    it('should execute database list successfully', async () => {
      const mockResult = {
        items: ['db1', 'db2', 'db3'],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.dbList();
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
        databaseList: {}
      });
      expect(result).toBeDefined();
    });

    it('should handle paginated database list', async () => {
      const mockResult = {
        items: ['db1', 'db2'],
        cursor: { startKey: 'db2', batchSize: 50 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.dbList();
      const result = await query.run(mockConnection, { batchSize: 50 });

      expect(mockConnection.query).toHaveBeenCalledWith({
        databaseList: {},
        cursor: { startKey: undefined, batchSize: 50 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      });
      expect(result).toBeDefined();
    });

    it('should handle database list errors', async () => {
      const error = new Error('Insufficient permissions');
      mockConnection.query.mockRejectedValue(error);

      const query = r.dbList();

      await expect(query.run(mockConnection)).rejects.toThrow('Insufficient permissions');
    });

    it('should create query with correct return type', () => {
      const query = r.dbList();
      // Should return ArrayQuery<string>
      expect(query).toBeInstanceOf(RQuery);
    });
  });

  describe('database query chaining', () => {
    it('should allow chaining from db() to table operations', () => {
      const query = r.db('mydb').table('users');
      expect(query._dbName).toBe('mydb');
      expect(query._query).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
    });

    it('should allow chaining from db() to tableCreate', () => {
      const query = r.db('mydb').tableCreate('newtable');
      expect(query._dbName).toBe('mydb');
      expect(query._query).toEqual({
        tableCreate: {
          table: {
            database: { name: 'mydb' },
            name: 'newtable'
          }
        }
      });
    });

    it('should allow chaining from db() to tableDrop', () => {
      const query = r.db('mydb').tableDrop('oldtable');
      expect(query._dbName).toBe('mydb');
      expect(query._query).toEqual({
        tableDrop: {
          table: {
            database: { name: 'mydb' },
            name: 'oldtable'
          }
        }
      });
    });

    it('should allow chaining from db() to tableList', () => {
      const query = r.db('mydb').tableList();
      expect(query._dbName).toBe('mydb');
      expect(query._query).toEqual({
        tableList: {
          database: { name: 'mydb' }
        }
      });
    });
  });

  describe('database query execution with options', () => {
    it('should pass query options to database creation', async () => {
      const mockResult = { result: { created: 1 } };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.dbCreate('newdb');
      await query.run(mockConnection, { timeout: 5000, explain: true });

      expect(mockConnection.query).toHaveBeenCalledWith({
        databaseCreate: { name: 'newdb' },
        options: {
          timeoutMs: 5000,
          explain: true
        }
      });
    });

    it('should pass query options to database drop', async () => {
      const mockResult = { result: { dropped: 1 } };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.dbDrop('olddb');
      await query.run(mockConnection, { timeout: 3000 });

      expect(mockConnection.query).toHaveBeenCalledWith({
        databaseDrop: { name: 'olddb' },
        options: {
          timeoutMs: 3000,
          explain: false
        }
      });
    });

    it('should pass cursor options to database list', async () => {
      const mockResult = {
        items: ['db1', 'db2'],
        cursor: { startKey: 'db2', batchSize: 10 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.dbList();
      await query.run(mockConnection, { batchSize: 10, startKey: 'db1' });

      expect(mockConnection.query).toHaveBeenCalledWith({
        databaseList: {},
        cursor: {
          startKey: 'db1',
          batchSize: 10
        },
        options: {
          explain: false,
          timeoutMs: 0
        }
      });
    });
  });

  describe('error handling', () => {
    it('should handle connection errors for dbCreate', async () => {
      mockConnection.query.mockRejectedValue(new Error('Connection lost'));

      const query = r.dbCreate('newdb');
      await expect(query.run(mockConnection)).rejects.toThrow('Connection lost');
    });

    it('should handle server errors for dbDrop', async () => {
      const serverError = {
        code: 500,
        message: 'Internal server error',
        type: 'INTERNAL_ERROR'
      };
      mockConnection.query.mockRejectedValue(serverError);

      const query = r.dbDrop('db');
      await expect(query.run(mockConnection)).rejects.toEqual(serverError);
    });

    it('should handle permission errors for dbList', async () => {
      const permissionError = new Error('Access denied');
      mockConnection.query.mockRejectedValue(permissionError);

      const query = r.dbList();
      await expect(query.run(mockConnection)).rejects.toThrow('Access denied');
    });
  });

  describe('type safety', () => {
    it('should maintain correct types through database operations', () => {
      // Test that TypeScript types are maintained correctly
      const dbQuery = r.db('test');
      const createQuery = r.dbCreate('test');
      const dropQuery = r.dbDrop('test');
      const listQuery = r.dbList();

      // These should all be RQuery instances with correct type parameters
      expect(dbQuery).toBeInstanceOf(RQuery);
      expect(createQuery).toBeInstanceOf(RQuery);
      expect(dropQuery).toBeInstanceOf(RQuery);
      expect(listQuery).toBeInstanceOf(RQuery);
    });
  });
});
