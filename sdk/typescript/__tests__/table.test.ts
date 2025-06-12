import { Connection } from '../src/connection';
import { r, RQuery } from '../src/query';

// Mock the Connection class
jest.mock('../src/connection');

describe('Table Operations', () => {
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

  describe('table()', () => {
    it('should create a table query with database context', () => {
      const query = r.db('mydb').table('users');
      expect(query).toBeInstanceOf(RQuery);
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

    it('should create a table query with default database', () => {
      const query = r.db().table('posts');
      expect(query._dbName).toBe('default');
      expect(query._query).toEqual({
        table: {
          table: {
            database: { name: 'default' },
            name: 'posts'
          }
        }
      });
    });

    it('should allow chaining from table query', () => {
      const query = r.db('mydb').table('users').get('user1');
      expect(query).toBeInstanceOf(RQuery);
      expect(query._dbName).toBe('mydb');
    });

    it('should execute table query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice' },
          { id: 'user2', name: 'Bob' }
        ],
        cursor: { startKey: 'user2', batchSize: 50 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users');
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
      expect(result).toBeDefined();
    });
  });

  describe('tableCreate()', () => {
    it('should create table creation query', () => {
      const query = r.db('mydb').tableCreate('newtable');
      expect(query).toBeInstanceOf(RQuery);
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

    it('should execute table creation successfully', async () => {
      const mockResult = {
        result: { created: 1 }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').tableCreate('newtable');
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
        tableCreate: {
          table: {
            database: { name: 'mydb' },
            name: 'newtable'
          }
        }
      });
      expect(result).toEqual({ created: 1 });
    });

    it('should handle table creation errors', async () => {
      const error = new Error('Table already exists');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').tableCreate('existingtable');

      await expect(query.run(mockConnection)).rejects.toThrow('Table already exists');
    });

    it('should work with default database', () => {
      const query = r.db().tableCreate('newtable');
      expect(query._query).toEqual({
        tableCreate: {
          table: {
            database: { name: 'default' },
            name: 'newtable'
          }
        }
      });
    });
  });

  describe('tableDrop()', () => {
    it('should create table drop query', () => {
      const query = r.db('mydb').tableDrop('oldtable');
      expect(query).toBeInstanceOf(RQuery);
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

    it('should execute table drop successfully', async () => {
      const mockResult = {
        result: { dropped: 1 }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').tableDrop('oldtable');
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
        tableDrop: {
          table: {
            database: { name: 'mydb' },
            name: 'oldtable'
          }
        }
      });
      expect(result).toEqual({ dropped: 1 });
    });

    it('should handle table drop errors', async () => {
      const error = new Error('Table not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').tableDrop('nonexistent');

      await expect(query.run(mockConnection)).rejects.toThrow('Table not found');
    });

    it('should work with default database', () => {
      const query = r.db().tableDrop('oldtable');
      expect(query._query).toEqual({
        tableDrop: {
          table: {
            database: { name: 'default' },
            name: 'oldtable'
          }
        }
      });
    });
  });

  describe('tableList()', () => {
    it('should create table list query', () => {
      const query = r.db('mydb').tableList();
      expect(query).toBeInstanceOf(RQuery);
      expect(query._dbName).toBe('mydb');
      expect(query._query).toEqual({
        tableList: {
          database: { name: 'mydb' }
        }
      });
    });

    it('should execute table list successfully', async () => {
      const mockResult = {
        items: ['users', 'posts', 'comments'],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').tableList();
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
        tableList: {
          database: { name: 'mydb' }
        }
      });
      expect(result).toBeDefined();
    });

    it('should handle paginated table list', async () => {
      const mockResult = {
        items: ['table1', 'table2'],
        cursor: { startKey: 'table2', batchSize: 50 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').tableList();
      const result = await query.run(mockConnection, { batchSize: 50 });

      expect(mockConnection.query).toHaveBeenCalledWith({
        tableList: {
          database: { name: 'mydb' }
        },
        cursor: { startKey: undefined, batchSize: 50 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      });
      expect(result).toBeDefined();
    });

    it('should handle table list errors', async () => {
      const error = new Error('Database not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('nonexistent').tableList();

      await expect(query.run(mockConnection)).rejects.toThrow('Database not found');
    });

    it('should work with default database', () => {
      const query = r.db().tableList();
      expect(query._query).toEqual({
        tableList: {
          database: { name: 'default' }
        }
      });
    });
  });

  describe('table query chaining', () => {
    it('should allow chaining get() from table', () => {
      const query = r.db('mydb').table('users').get('user1');
      expect(query._dbName).toBe('mydb');
      expect(query._query).toEqual({
        get: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          key: { string: 'user1' }
        }
      });
    });

    it('should allow chaining getAll() from table', () => {
      const query = r.db('mydb').table('users').getAll('user1', 'user2');
      expect(query._dbName).toBe('mydb');
      expect(query._query).toEqual({
        getAll: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          keys: [{ string: 'user1' }, { string: 'user2' }]
        }
      });
    });

    it('should allow chaining filter() from table', () => {
      const query = r.db('mydb').table('users').filter({ active: true });
      expect(query._dbName).toBe('mydb');
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.source).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
    });

    it('should allow chaining orderBy() from table', () => {
      const query = r.db('mydb').table('users').orderBy('name');
      expect(query._dbName).toBe('mydb');
      expect(query._query).toEqual({
        orderBy: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          fields: [
            { fieldName: 'name', direction: 0 } // ASC
          ]
        }
      });
    });

    it('should allow chaining limit() from table', () => {
      const query = r.db('mydb').table('users').limit(10);
      expect(query._dbName).toBe('mydb');
      expect(query._query).toEqual({
        limit: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          count: 10
        }
      });
    });

    it('should allow chaining skip() from table', () => {
      const query = r.db('mydb').table('users').skip(5);
      expect(query._dbName).toBe('mydb');
      expect(query._query).toEqual({
        skip: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          count: 5
        }
      });
    });

    it('should allow chaining count() from table', () => {
      const query = r.db('mydb').table('users').count();
      expect(query._dbName).toBe('mydb');
      expect(query._query).toEqual({
        count: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          }
        }
      });
    });

    it('should allow chaining insert() from table', () => {
      const doc = { name: 'Alice', age: 30 };
      const query = r.db('mydb').table('users').insert(doc);
      expect(query._dbName).toBe('mydb');
      expect(query._query.insert).toBeDefined();
      expect(query._query.insert!.source).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
    });

    it('should allow chaining update() from table', () => {
      const patch = { active: true };
      const query = r.db('mydb').table('users').update(patch);
      expect(query._dbName).toBe('mydb');
      expect(query._query.update).toBeDefined();
      expect(query._query.update!.source).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
    });

    it('should allow chaining delete() from table', () => {
      const query = r.db('mydb').table('users').delete();
      expect(query._dbName).toBe('mydb');
      expect(query._query).toEqual({
        delete: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          }
        }
      });
    });
  });

  describe('table query execution with options', () => {
    it('should pass query options to table creation', async () => {
      const mockResult = { result: { created: 1 } };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').tableCreate('newtable');
      await query.run(mockConnection, { timeout: 5000, explain: true });

      expect(mockConnection.query).toHaveBeenCalledWith({
        tableCreate: {
          table: {
            database: { name: 'mydb' },
            name: 'newtable'
          }
        },
        options: {
          timeoutMs: 5000,
          explain: true
        }
      });
    });

    it('should pass cursor options to table queries', async () => {
      const mockResult = {
        items: [{ id: '1', name: 'Alice' }],
        cursor: { startKey: '1', batchSize: 25 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users');
      await query.run(mockConnection, { batchSize: 25, startKey: '0' });

      expect(mockConnection.query).toHaveBeenCalledWith({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        },
        cursor: {
          startKey: '0',
          batchSize: 25
        },
        options: {
          explain: false,
          timeoutMs: 0
        }
      });
    });
  });

  describe('error handling', () => {
    it('should handle table access errors', async () => {
      const error = new Error('Table access denied');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('restricted');
      await expect(query.run(mockConnection)).rejects.toThrow('Table access denied');
    });

    it('should handle table creation conflicts', async () => {
      const error = new Error('Table name already in use');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').tableCreate('existing');
      await expect(query.run(mockConnection)).rejects.toThrow('Table name already in use');
    });

    it('should handle table drop on non-existent table', async () => {
      const error = new Error('Table does not exist');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').tableDrop('nonexistent');
      await expect(query.run(mockConnection)).rejects.toThrow('Table does not exist');
    });

    it('should handle database not found for table operations', async () => {
      const error = new Error('Database not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('nonexistent').table('users');
      await expect(query.run(mockConnection)).rejects.toThrow('Database not found');
    });
  });

  describe('complex table operations', () => {
    it('should handle complex chained operations', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter({ active: true })
        .orderBy('created_at')
        .limit(100)
        .skip(50);

      expect(query._dbName).toBe('mydb');
      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.source).toEqual({
        skip: {
          source: {
            orderBy: {
              source: {
                filter: {
                  source: {
                    table: {
                      table: {
                        database: { name: 'mydb' },
                        name: 'users'
                      }
                    }
                  },
                  predicate: expect.any(Object)
                }
              },
              fields: [{ fieldName: 'created_at', direction: 0 }]
            }
          },
          count: 50
        }
      });
      expect(query._query.limit!.count).toBe(100);
    });

    it('should maintain database context through complex chains', () => {
      const query = r.db('analytics').table('events').filter({ type: 'click' }).count();

      expect(query._dbName).toBe('analytics');
      expect(query._query.count!.source).toEqual({
        filter: {
          source: {
            table: {
              table: {
                database: { name: 'analytics' },
                name: 'events'
              }
            }
          },
          predicate: expect.any(Object)
        }
      });
    });
  });

  describe('type safety', () => {
    it('should maintain correct types through table operations', () => {
      const tableQuery = r.db('test').table('users');
      const createQuery = r.db('test').tableCreate('users');
      const dropQuery = r.db('test').tableDrop('users');
      const listQuery = r.db('test').tableList();

      expect(tableQuery).toBeInstanceOf(RQuery);
      expect(createQuery).toBeInstanceOf(RQuery);
      expect(dropQuery).toBeInstanceOf(RQuery);
      expect(listQuery).toBeInstanceOf(RQuery);
    });

    it('should preserve type information in chained operations', () => {
      interface User {
        id: string;
        name: string;
        age: number;
      }

      const query = r.db('test').table<User>('users').get('user1');
      expect(query).toBeInstanceOf(RQuery);
      expect(query._dbName).toBe('test');
    });
  });
});
