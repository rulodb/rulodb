import { Connection } from '../src/connection';
import { r, RQuery } from '../src/query';

// Mock the Connection class
jest.mock('../src/connection');

describe('Filter Operations', () => {
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

  describe('filter() with object predicates', () => {
    it('should create filter query with simple object predicate', () => {
      const query = r.db('mydb').table('users').filter({ active: true });
      expect(query).toBeInstanceOf(RQuery);
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
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should create filter query with multiple field object predicate', () => {
      const query = r.db('mydb').table('users').filter({
        active: true,
        role: 'admin',
        age: 25
      });
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should create filter query with nested object predicate', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter({
          profile: { verified: true },
          settings: { theme: 'dark' }
        });
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute filter query with object predicate successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice', active: true },
          { id: 'user2', name: 'Bob', active: true }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').filter({ active: true });
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
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
      });
      expect(result).toBeDefined();
    });
  });

  describe('filter() with function predicates', () => {
    it('should create filter query with row field access', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.age.gt(18));
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should create filter query with multiple field conditions', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.age.gt(18).and(row.active.eq(true)));
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should create filter query with OR conditions', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.role.eq('admin').or(row.role.eq('moderator')));
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should create filter query with nested field access', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('profile.verified').eq(true));
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute filter query with function predicate successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice', age: 25 },
          { id: 'user2', name: 'Bob', age: 30 }
        ],
        cursor: { startKey: 'user2', batchSize: 50 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.age.gt(18));
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
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
      });
      expect(result).toBeDefined();
    });
  });

  describe('filter() with row helper', () => {
    it('should work with row() helper function', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((r) => r.active.eq(true));
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should support typed row access', () => {
      interface User {
        id: string;
        name: string;
        age: number;
        active: boolean;
      }

      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.age.gt(21).and(row.active.eq(true)));
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should support field method for dynamic field access', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('dynamic_field').ne(null));
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('filter() chaining', () => {
    it('should allow chaining from table to filter', () => {
      const query = r.db('mydb').table('users').filter({ active: true });
      expect(query._dbName).toBe('mydb');
      expect(query._query.filter!.source).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
    });

    it('should allow chaining from getAll to filter', () => {
      const query = r.db('mydb').table('users').getAll('user1', 'user2').filter({ active: true });
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

    it('should allow chaining multiple filters', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter({ active: true })
        .filter((row) => row.age.gt(18));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.source).toEqual({
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
      });
    });

    it('should allow chaining orderBy after filter', () => {
      const query = r.db('mydb').table('users').filter({ active: true }).orderBy('name');

      expect(query._query.orderBy).toBeDefined();
      expect(query._query.orderBy!.source).toEqual({
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
      });
    });

    it('should allow chaining limit after filter', () => {
      const query = r.db('mydb').table('users').filter({ active: true }).limit(10);

      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.count).toBe(10);
      expect(query._query.limit!.source).toEqual({
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
      });
    });

    it('should allow chaining count after filter', () => {
      const query = r.db('mydb').table('users').filter({ active: true }).count();

      expect(query._query.count).toBeDefined();
      expect(query._query.count!.source).toEqual({
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
      });
    });
  });

  describe('filter() with pagination', () => {
    it('should handle filter with cursor options', async () => {
      const mockResult = {
        items: [{ id: 'user1', name: 'Alice', active: true }],
        cursor: { startKey: 'user1', batchSize: 25 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').filter({ active: true });
      const result = await query.run(mockConnection, { batchSize: 25, startKey: 'start' });

      expect(mockConnection.query).toHaveBeenCalledWith({
        filter: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          predicate: {
            binary: {
              left: {
                field: {
                  path: ['active'],
                  separator: '.'
                }
              },
              op: 0,
              right: {
                literal: {
                  bool: true
                }
              }
            }
          }
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
      expect(result).toBeDefined();
    });

    it('should handle large result sets with pagination', async () => {
      const mockResult = {
        items: new Array(50).fill(0).map((_, i) => ({
          id: `user${i}`,
          name: `User ${i}`,
          active: true
        })),
        cursor: { startKey: 'user49', batchSize: 50 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').filter({ active: true });
      const result = await query.run(mockConnection, { batchSize: 50 });

      expect(result).toBeDefined();
    });
  });

  describe('filter() error handling', () => {
    it('should handle filter errors', async () => {
      const error = new Error('Filter predicate invalid');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('users').filter({ invalid_field: 'value' });

      await expect(query.run(mockConnection)).rejects.toThrow('Filter predicate invalid');
    });

    it('should handle table not found errors in filter', async () => {
      const error = new Error('Table not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('nonexistent').filter({ active: true });

      await expect(query.run(mockConnection)).rejects.toThrow('Table not found');
    });

    it('should handle permission errors in filter', async () => {
      const error = new Error('Access denied');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('restricted').filter({ sensitive: true });

      await expect(query.run(mockConnection)).rejects.toThrow('Access denied');
    });
  });

  describe('filter() with complex predicates', () => {
    it('should handle complex AND/OR combinations', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.age.gt(18).and(row.role.eq('admin').or(row.role.eq('moderator'))));
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle nested field comparisons', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('profile.settings.notifications').eq(true));
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle pattern matching in filters', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.email.match('@example\\.com$'));
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle null checks in filters', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.deleted_at.eq(null));
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle mixed type comparisons', () => {
      const query = r
        .db('mydb')
        .table('events')
        .filter((row) => row.timestamp.gt(1640995200).and(row.type.eq('click')));
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('filter() execution with options', () => {
    it('should pass query options to filter', async () => {
      const mockResult = {
        items: [{ id: 'user1', active: true }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').filter({ active: true });
      await query.run(mockConnection, { timeout: 5000, explain: true });

      expect(mockConnection.query).toHaveBeenCalledWith({
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
        },
        options: {
          timeoutMs: 5000,
          explain: true
        }
      });
    });
  });

  describe('filter() type safety', () => {
    it('should maintain correct types through filter operations', () => {
      const filterQuery = r.db('test').table('users').filter({ active: true });
      expect(filterQuery).toBeInstanceOf(RQuery);
    });

    it('should preserve type information in chained filter operations', () => {
      interface User {
        id: string;
        name: string;
        age: number;
        active: boolean;
      }

      const query = r
        .db('test')
        .table<User>('users')
        .filter({ active: true })
        .filter((row) => row.age.gt(21));

      expect(query).toBeInstanceOf(RQuery);
      expect(query._dbName).toBe('test');
    });

    it('should work with default database', () => {
      const query = r.db().table('users').filter({ active: true });
      expect(query._query.filter!.source).toEqual({
        table: {
          table: {
            database: { name: 'default' },
            name: 'users'
          }
        }
      });
    });
  });
});
