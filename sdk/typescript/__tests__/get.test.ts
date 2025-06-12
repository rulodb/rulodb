import { Connection } from '../src/connection';
import { r, RQuery } from '../src/query';

// Mock the Connection class
jest.mock('../src/connection');

describe('Get Operations', () => {
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

  describe('get()', () => {
    it('should create a get query with string key', () => {
      const query = r.db('mydb').table('users').get('user1');
      expect(query).toBeInstanceOf(RQuery);
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

    it('should execute get query successfully and return document', async () => {
      const mockResult = {
        result: { id: 'user1', name: 'Alice', age: 30 }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').get('user1');
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
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
      expect(result).toEqual({ id: 'user1', name: 'Alice', age: 30 });
    });

    it('should handle get query returning null for non-existent document', async () => {
      const mockResult = {
        result: null
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').get('nonexistent');
      const result = await query.run(mockConnection);

      expect(result).toBeNull();
    });

    it('should handle errors from get query', async () => {
      const error = new Error('Document not accessible');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('users').get('user1');

      await expect(query.run(mockConnection)).rejects.toThrow('Document not accessible');
    });

    it('should create correct query state type', () => {
      const query = r.db('mydb').table('users').get('user1');
      // Should return SelectionQuery type
      expect(query).toBeInstanceOf(RQuery);
    });

    it('should work with default database', () => {
      const query = r.db().table('users').get('user1');
      expect(query._query.get!.source).toEqual({
        table: {
          table: {
            database: { name: 'default' },
            name: 'users'
          }
        }
      });
    });
  });

  describe('getAll()', () => {
    it('should create getAll query with single key', () => {
      const query = r.db('mydb').table('users').getAll('user1');
      expect(query).toBeInstanceOf(RQuery);
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
          keys: [{ string: 'user1' }]
        }
      });
    });

    it('should create getAll query with multiple keys', () => {
      const query = r.db('mydb').table('users').getAll('user1', 'user2', 'user3');
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
          keys: [{ string: 'user1' }, { string: 'user2' }, { string: 'user3' }]
        }
      });
    });

    it('should execute getAll query successfully and return cursor', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice' },
          { id: 'user2', name: 'Bob' }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').getAll('user1', 'user2');
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
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
      expect(result).toBeDefined();
    });

    it('should handle getAll with cursor pagination', async () => {
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

      const query = r.db('mydb').table('users').getAll('user1', 'user2', 'user3');
      const result = await query.run(mockConnection, { batchSize: 50 });

      expect(mockConnection.query).toHaveBeenCalledWith({
        getAll: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          keys: [{ string: 'user1' }, { string: 'user2' }, { string: 'user3' }]
        },
        cursor: { startKey: undefined, batchSize: 50 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      });
      expect(result).toBeDefined();
    });

    it('should handle empty keys array', () => {
      const query = r.db('mydb').table('users').getAll();
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
          keys: []
        }
      });
    });

    it('should handle errors from getAll query', async () => {
      const error = new Error('Table access denied');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('users').getAll('user1', 'user2');

      await expect(query.run(mockConnection)).rejects.toThrow('Table access denied');
    });

    it('should work with default database', () => {
      const query = r.db().table('users').getAll('user1', 'user2');
      expect(query._query.getAll!.source).toEqual({
        table: {
          table: {
            database: { name: 'default' },
            name: 'users'
          }
        }
      });
    });
  });

  describe('get and getAll chaining', () => {
    it('should allow chaining from get query', () => {
      const query = r.db('mydb').table('users').get('user1').update({ active: true });
      expect(query._dbName).toBe('mydb');
      expect(query._query.update).toBeDefined();
      expect(query._query.update!.source).toEqual({
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

    it('should allow chaining from getAll query', () => {
      const query = r.db('mydb').table('users').getAll('user1', 'user2').filter({ active: true });
      expect(query._dbName).toBe('mydb');
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.source).toEqual({
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

    it('should allow chaining orderBy from getAll', () => {
      const query = r.db('mydb').table('users').getAll('user1', 'user2').orderBy('name');
      expect(query._query).toEqual({
        orderBy: {
          source: {
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
          },
          fields: [
            { fieldName: 'name', direction: 0 } // ASC
          ]
        }
      });
    });

    it('should allow chaining limit from getAll', () => {
      const query = r.db('mydb').table('users').getAll('user1', 'user2', 'user3').limit(2);
      expect(query._query).toEqual({
        limit: {
          source: {
            getAll: {
              source: {
                table: {
                  table: {
                    database: { name: 'mydb' },
                    name: 'users'
                  }
                }
              },
              keys: [{ string: 'user1' }, { string: 'user2' }, { string: 'user3' }]
            }
          },
          count: 2
        }
      });
    });

    it('should allow chaining delete from get', () => {
      const query = r.db('mydb').table('users').get('user1').delete();
      expect(query._query).toEqual({
        delete: {
          source: {
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
          }
        }
      });
    });
  });

  describe('query execution with options', () => {
    it('should pass query options to get', async () => {
      const mockResult = { result: { id: 'user1', name: 'Alice' } };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').get('user1');
      await query.run(mockConnection, { timeout: 5000, explain: true });

      expect(mockConnection.query).toHaveBeenCalledWith({
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
        },
        options: {
          timeoutMs: 5000,
          explain: true
        }
      });
    });

    it('should pass cursor options to getAll', async () => {
      const mockResult = {
        items: [{ id: 'user1' }],
        cursor: { startKey: 'user1', batchSize: 25 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').getAll('user1', 'user2');
      await query.run(mockConnection, { batchSize: 25, startKey: 'start' });

      expect(mockConnection.query).toHaveBeenCalledWith({
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
        },
        cursor: {
          startKey: 'start',
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
    it('should handle table not found errors', async () => {
      const error = new Error('Table "nonexistent" does not exist');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('nonexistent').get('key1');
      await expect(query.run(mockConnection)).rejects.toThrow('Table "nonexistent" does not exist');
    });

    it('should handle permission errors', async () => {
      const error = new Error('Access denied to table "restricted"');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('restricted').getAll('key1', 'key2');
      await expect(query.run(mockConnection)).rejects.toThrow(
        'Access denied to table "restricted"'
      );
    });

    it('should handle network errors', async () => {
      const error = new Error('Connection lost');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('users').get('user1');
      await expect(query.run(mockConnection)).rejects.toThrow('Connection lost');
    });
  });

  describe('type safety', () => {
    it('should maintain correct types through get operations', () => {
      const getQuery = r.db('test').table('users').get('user1');
      const getAllQuery = r.db('test').table('users').getAll('user1', 'user2');

      expect(getQuery).toBeInstanceOf(RQuery);
      expect(getAllQuery).toBeInstanceOf(RQuery);
    });

    it('should preserve type information in chained operations', () => {
      interface User {
        id: string;
        name: string;
        age: number;
      }

      const query = r.db('test').table<User>('users').get('user1').update({ age: 31 });
      expect(query).toBeInstanceOf(RQuery);
      expect(query._dbName).toBe('test');
    });
  });
});
