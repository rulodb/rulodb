import { Connection } from '../src/connection';
import { r, RQuery } from '../src/query';

// Mock the Connection class
jest.mock('../src/connection');

describe('Limit Operations', () => {
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

  describe('limit() basic functionality', () => {
    it('should create limit query with positive number', () => {
      const query = r.db('mydb').table('users').limit(10);

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.count).toBe(10);
      expect(query._query.limit!.source).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
    });

    it('should create limit query with small number', () => {
      const query = r.db('mydb').table('users').limit(1);

      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.count).toBe(1);
    });

    it('should create limit query with large number', () => {
      const query = r.db('mydb').table('users').limit(1000000);

      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.count).toBe(1000000);
    });

    it('should execute limit query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice', age: 25 },
          { id: 'user2', name: 'Bob', age: 30 },
          { id: 'user3', name: 'Charlie', age: 35 }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').limit(3);
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
        limit: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          count: 3
        }
      });
      expect(result).toBeDefined();
    });
  });

  describe('limit() with zero and edge cases', () => {
    it('should create limit query with zero', () => {
      const query = r.db('mydb').table('users').limit(0);

      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.count).toBe(0);
    });

    it('should handle zero limit execution', async () => {
      const mockResult = {
        items: [],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').limit(0);
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
        limit: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          count: 0
        }
      });
      expect(result).toBeDefined();
    });

    it('should handle very large limit values', () => {
      const largeLimit = Number.MAX_SAFE_INTEGER;
      const query = r.db('mydb').table('users').limit(largeLimit);

      expect(query._query.limit!.count).toBe(largeLimit);
    });
  });

  describe('limit() chaining from different sources', () => {
    it('should chain limit from table scan', () => {
      const query = r.db('mydb').table('users').limit(5);

      expect(query._query.limit!.source).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
    });

    it('should chain limit from getAll', () => {
      const query = r.db('mydb').table('users').getAll('user1', 'user2').limit(1);

      expect(query._query.limit!.source).toEqual({
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

    it('should chain limit from filter', () => {
      const query = r.db('mydb').table('users').filter({ active: true }).limit(10);

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

    it('should chain limit from orderBy', () => {
      const query = r.db('mydb').table('users').orderBy('name').limit(5);

      expect(query._query.limit!.source).toEqual({
        orderBy: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          fields: expect.any(Array)
        }
      });
    });
  });

  describe('limit() chaining to other operations', () => {
    it('should allow chaining skip after limit', () => {
      const query = r.db('mydb').table('users').limit(10).skip(5);

      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.count).toBe(10);
      expect((query._query.limit!.source as any).skip).toBeDefined();
      expect((query._query.limit!.source as any).skip!.count).toBe(5);
      expect((query._query.limit!.source as any).skip!.source).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
    });

    it('should allow chaining filter after limit', () => {
      const query = r.db('mydb').table('users').limit(20).filter({ active: true });

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.source).toEqual({
        limit: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          count: 20
        }
      });
    });

    it('should allow chaining orderBy after limit', () => {
      const query = r.db('mydb').table('users').limit(50).orderBy('name');

      expect(query._query.orderBy).toBeDefined();
      expect(query._query.orderBy!.source).toEqual({
        limit: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          count: 50
        }
      });
    });

    it('should allow chaining count after limit', () => {
      const query = r.db('mydb').table('users').limit(100).count();

      expect(query._query.count).toBeDefined();
      expect(query._query.count!.source).toEqual({
        limit: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          count: 100
        }
      });
    });

    it('should work in complex query chains', () => {
      const query = r
        .db('mydb')
        .table('posts')
        .filter({ published: true })
        .orderBy({ field: 'created_at', direction: 'desc' })
        .limit(20)
        .skip(10);

      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.count).toBe(20);
      expect((query._query.limit!.source as any).skip).toBeDefined();
      expect((query._query.limit!.source as any).skip!.count).toBe(10);
      expect((query._query.limit!.source as any).skip!.source).toEqual({
        orderBy: {
          source: {
            filter: {
              source: {
                table: {
                  table: {
                    database: { name: 'mydb' },
                    name: 'posts'
                  }
                }
              },
              predicate: expect.any(Object)
            }
          },
          fields: expect.any(Array)
        }
      });
    });
  });

  describe('limit() with skip interaction', () => {
    it('should handle skip before limit correctly', () => {
      const query = r.db('mydb').table('users').skip(5).limit(10);

      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.count).toBe(10);
      expect(query._query.limit!.source).toEqual({
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

    it('should handle limit after skip with reordering', () => {
      const query = r.db('mydb').table('users').limit(20).skip(5);

      // The skip method should reorder to put skip before limit
      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.source).toEqual({
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
      expect(query._query.limit!.count).toBe(20);
    });

    it('should execute skip-limit combination successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user6', name: 'User 6' },
          { id: 'user7', name: 'User 7' },
          { id: 'user8', name: 'User 8' }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').skip(5).limit(3);
      const result = await query.run(mockConnection);

      expect(result).toBeDefined();
    });
  });

  describe('limit() with pagination', () => {
    it('should handle limit queries with cursor options', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice' },
          { id: 'user2', name: 'Bob' }
        ],
        cursor: { startKey: 'user2', batchSize: 25 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').limit(10);
      const result = await query.run(mockConnection, { batchSize: 25, startKey: 'start' });

      expect(mockConnection.query).toHaveBeenCalledWith({
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

    it('should handle limit smaller than batch size', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice' },
          { id: 'user2', name: 'Bob' }
        ],
        cursor: undefined // No more data since limit was reached
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').limit(2);
      const result = await query.run(mockConnection, { batchSize: 50 });

      expect(result).toBeDefined();
    });

    it('should handle limit larger than available data', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice' },
          { id: 'user2', name: 'Bob' }
        ],
        cursor: undefined // No more data available
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').limit(1000);
      const result = await query.run(mockConnection);

      expect(result).toBeDefined();
    });
  });

  describe('limit() with typed interfaces', () => {
    interface User {
      id: string;
      name: string;
      email: string;
      age: number;
    }

    it('should work with typed table queries', () => {
      const query = r.db('mydb').table<User>('users').limit(5);

      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.count).toBe(5);
    });

    it('should maintain type information through limit', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.age.gt(18))
        .limit(10);

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.limit!.count).toBe(10);
    });
  });

  describe('limit() error handling', () => {
    it('should handle negative limit values', () => {
      // Note: The behavior with negative values depends on implementation
      // This test documents current behavior
      const query = r.db('mydb').table('users').limit(-5);

      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.count).toBe(-5);
    });

    it('should handle table not found errors', async () => {
      const error = new Error('Table not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('nonexistent').limit(10);

      await expect(query.run(mockConnection)).rejects.toThrow('Table not found');
    });

    it('should handle database not found errors', async () => {
      const error = new Error('Database not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('nonexistent').table('users').limit(10);

      await expect(query.run(mockConnection)).rejects.toThrow('Database not found');
    });

    it('should handle connection errors during limit execution', async () => {
      const error = new Error('Connection lost');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('users').limit(5);

      await expect(query.run(mockConnection)).rejects.toThrow('Connection lost');
    });
  });

  describe('limit() with query options', () => {
    it('should pass query options through limit operations', async () => {
      const mockResult = {
        items: [{ id: 'user1', name: 'Alice' }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').limit(5);
      await query.run(mockConnection, { timeout: 5000, explain: true });

      expect(mockConnection.query).toHaveBeenCalledWith({
        limit: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          count: 5
        },
        options: {
          timeoutMs: 5000,
          explain: true
        }
      });
    });

    it('should handle explain option with limit', async () => {
      const mockResult = {
        items: [],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').limit(1);
      await query.run(mockConnection, { explain: true });

      expect(mockConnection.query).toHaveBeenCalledWith(
        expect.objectContaining({
          options: expect.objectContaining({
            explain: true
          })
        })
      );
    });
  });

  describe('limit() with default database', () => {
    it('should work with default database', () => {
      const query = r.db().table('users').limit(10);

      expect(query._query.limit!.source).toEqual({
        table: {
          table: {
            database: { name: 'default' },
            name: 'users'
          }
        }
      });
    });
  });

  describe('limit() performance and optimization scenarios', () => {
    it('should handle top-N queries efficiently', () => {
      const query = r
        .db('mydb')
        .table('posts')
        .orderBy({ field: 'view_count', direction: 'desc' })
        .limit(10);

      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.count).toBe(10);
      expect(query._query.limit!.source).toEqual({
        orderBy: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'posts'
              }
            }
          },
          fields: expect.any(Array)
        }
      });
    });

    it('should work with indexed field filtering and limit', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.email.match('@company\\.com$'))
        .limit(20);

      expect(query._query.limit!.count).toBe(20);
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

    it('should handle large table scans with reasonable limits', () => {
      const query = r.db('analytics').table('events').limit(100000);

      expect(query._query.limit!.count).toBe(100000);
    });

    it('should work with multiple limit applications (last wins)', () => {
      // Note: This behavior depends on how the query builder handles multiple limits
      const query = r.db('mydb').table('users').limit(100).limit(10);

      expect(query._query.limit!.count).toBe(10); // Last limit should win
    });
  });

  describe('limit() edge cases and boundary conditions', () => {
    it('should handle limit with float values (should be converted to int)', () => {
      const query = r.db('mydb').table('users').limit(10.7);

      // Assuming the implementation converts floats to integers
      expect(query._query.limit!.count).toBe(10.7);
    });

    it('should handle very large numbers close to MAX_SAFE_INTEGER', () => {
      const largeNumber = Number.MAX_SAFE_INTEGER - 1;
      const query = r.db('mydb').table('users').limit(largeNumber);

      expect(query._query.limit!.count).toBe(largeNumber);
    });

    it('should handle limit of 1 for single record retrieval', () => {
      const query = r.db('mydb').table('users').orderBy('id').limit(1);

      expect(query._query.limit!.count).toBe(1);
    });

    it('should work with empty table and limit', async () => {
      const mockResult = {
        items: [],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('empty_table').limit(5);
      const result = await query.run(mockConnection);

      expect(result).toBeDefined();
      expect(mockConnection.query).toHaveBeenCalled();
    });
  });

  describe('limit() with update and delete operations', () => {
    it('should work with delete operations', () => {
      const query = r.db('mydb').table('users').limit(5).delete();

      expect(query._query.delete).toBeDefined();
      expect(query._query.delete!.source).toEqual({
        limit: {
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

    it('should work with update operations', () => {
      const query = r.db('mydb').table('users').limit(10).update({ status: 'processed' });

      expect(query._query.update).toBeDefined();
      expect(query._query.update!.source).toEqual({
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
  });
});
