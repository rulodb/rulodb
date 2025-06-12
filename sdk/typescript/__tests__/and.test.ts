import { Connection } from '../src/connection';
import { r, RQuery } from '../src/query';
import { BinaryOp_Operator } from '../src/rulo';

// Mock the Connection class
jest.mock('../src/connection');
const MockedConnection = Connection as jest.MockedClass<typeof Connection>;

describe('AND Operations', () => {
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

  describe('and() with simple comparisons', () => {
    it('should create AND comparison with string values', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').eq('John').and(row.field('status').eq('active')));

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should create AND comparison with numeric values', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').gt(18).and(row.field('score').ge(70)));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should create AND comparison with different field types', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('active').eq(true).and(row.field('age').gt(18)));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute AND query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'John', age: 25, active: true },
          { id: 'user2', name: 'Jane', age: 30, active: true }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').eq('John').and(row.field('active').eq(true)));
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
          predicate: expect.objectContaining({
            subquery: expect.objectContaining({
              expression: expect.objectContaining({
                binary: expect.objectContaining({
                  op: BinaryOp_Operator.AND,
                  left: expect.objectContaining({
                    subquery: expect.objectContaining({
                      expression: expect.objectContaining({
                        binary: expect.objectContaining({
                          op: BinaryOp_Operator.EQ
                        })
                      })
                    })
                  }),
                  right: expect.objectContaining({
                    subquery: expect.objectContaining({
                      expression: expect.objectContaining({
                        binary: expect.objectContaining({
                          op: BinaryOp_Operator.EQ
                        })
                      })
                    })
                  })
                })
              })
            })
          })
        }
      });
      expect(result).toBeDefined();
    });
  });

  describe('and() with multiple conditions', () => {
    it('should handle multiple AND conditions', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row
            .field('age')
            .ge(18)
            .and(row.field('active').eq(true))
            .and(row.field('verified').eq(true))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle complex AND with different operators', () => {
      const query = r
        .db('mydb')
        .table('products')
        .filter((row) =>
          row
            .field('price')
            .gt(10)
            .and(row.field('stock').gt(0))
            .and(row.field('category').eq('electronics'))
            .and(row.field('rating').ge(4.0))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle AND with range conditions', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row.field('age').ge(18).and(row.field('age').le(65)).and(row.field('active').eq(true))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute complex AND query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice', age: 25, active: true, verified: true },
          { id: 'user2', name: 'Bob', age: 30, active: true, verified: true }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row
            .field('age')
            .ge(18)
            .and(row.field('active').eq(true))
            .and(row.field('verified').eq(true))
        );
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

  describe('and() with typed interfaces', () => {
    interface User {
      id: string;
      name: string;
      email: string;
      age: number;
      active: boolean;
      role?: string;
      verified?: boolean;
    }

    it('should work with typed row access', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('age').ge(18).and(row.field('active').eq(true)));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with optional typed fields', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('active').eq(true).and(row.field('email').ne('')));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle type-safe comparisons', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('name').eq('John').and(row.field('age').ge(18)));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('and() with OR combinations', () => {
    it('should handle AND combined with OR', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row
            .field('age')
            .ge(18)
            .and(row.field('role').eq('admin').or(row.field('role').eq('moderator')))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle complex AND/OR combinations', () => {
      const query = r
        .db('mydb')
        .table('products')
        .filter((row) =>
          row
            .field('available')
            .eq(true)
            .and(row.field('price').lt(100).or(row.field('discount').gt(0.2)))
            .and(row.field('rating').ge(4.0))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle nested logical operations', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row
            .field('active')
            .eq(true)
            .and(row.field('age').ge(18).and(row.field('age').le(65)))
            .and(row.field('role').eq('user').or(row.field('verified').eq(true)))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('and() with different data types', () => {
    it('should handle AND with string comparisons', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').gt('A').and(row.field('name').lt('Z')));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle AND with numeric range', () => {
      const query = r
        .db('mydb')
        .table('products')
        .filter((row) => row.field('price').ge(10).and(row.field('price').le(100)));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle AND with boolean and date combinations', () => {
      const timestamp = Date.now();
      const query = r
        .db('mydb')
        .table('events')
        .filter((row) => row.field('active').eq(true).and(row.field('created_at').gt(timestamp)));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle AND with null checks', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('deleted_at').eq(null).and(row.field('active').eq(true)));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('and() error handling', () => {
    it('should handle field not found errors', async () => {
      const error = new Error('Field not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').eq('John').and(row.field('nonexistent').eq('value')));

      await expect(query.run(mockConnection)).rejects.toThrow('Field not found');
    });

    it('should handle type mismatch errors', async () => {
      const error = new Error('Type mismatch');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').eq(25).and(row.field('name').eq(123)));

      await expect(query.run(mockConnection)).rejects.toThrow('Type mismatch');
    });

    it('should handle table not found errors', async () => {
      const error = new Error('Table not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('nonexistent')
        .filter((row) => row.field('name').eq('John').and(row.field('active').eq(true)));

      await expect(query.run(mockConnection)).rejects.toThrow('Table not found');
    });
  });

  describe('and() with nested field access', () => {
    it('should work with nested object fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row.field('profile.verified').eq(true).and(row.field('settings.notifications').eq(true))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with deeply nested fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row
            .field('settings.privacy.public')
            .eq(false)
            .and(row.field('profile.contact.email').ne(''))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with array index access', () => {
      const query = r
        .db('mydb')
        .table('posts')
        .filter((row) =>
          row.field('tags.0').eq('technology').and(row.field('categories.1').eq('programming'))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('and() with pagination', () => {
    it('should handle AND queries with cursor options', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'John', age: 25, active: true },
          { id: 'user2', name: 'Jane', age: 30, active: true }
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
        .filter((row) => row.field('age').ge(18).and(row.field('active').eq(true)));
      const result = await query.run(mockConnection, {
        startKey: 'user1',
        batchSize: 50
      });

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
        cursor: {
          startKey: 'user1',
          batchSize: 50
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
          age: 20 + i,
          active: true
        })),
        cursor: { startKey: 'user49', batchSize: 50 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').ge(18).and(row.field('active').eq(true)));
      const result = await query.run(mockConnection, { batchSize: 50 });

      expect(result).toBeDefined();
    });
  });

  describe('and() with query options', () => {
    it('should pass query options through AND operations', async () => {
      const mockResult = {
        items: [{ id: 'user1', name: 'John', age: 25, active: true }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').ge(18).and(row.field('active').eq(true)));
      await query.run(mockConnection, {
        timeout: 5000,
        explain: true
      });

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

  describe('and() with default database', () => {
    it('should work with default database', () => {
      const query = r
        .db()
        .table('users')
        .filter((row) => row.field('active').eq(true).and(row.field('age').ge(18)));

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

  describe('and() performance considerations', () => {
    it('should work efficiently with indexed fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row.field('email').eq('john@example.com').and(row.field('active').eq(true))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with orderBy after AND filtering', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('active').eq(true).and(row.field('age').ge(18)))
        .orderBy('name');

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

    it('should work with limit after AND filtering', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').ge(18).and(row.field('active').eq(true)))
        .limit(10);

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
  });

  describe('and() practical use cases', () => {
    it('should filter active adult users', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').ge(18).and(row.field('active').eq(true)));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should filter available products in price range', () => {
      const query = r
        .db('mydb')
        .table('products')
        .filter((row) =>
          row
            .field('available')
            .eq(true)
            .and(row.field('price').ge(10))
            .and(row.field('price').le(100))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should filter verified users with high scores', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row
            .field('verified')
            .eq(true)
            .and(row.field('score').ge(80))
            .and(row.field('active').eq(true))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should filter recent orders with specific status', () => {
      const oneWeekAgo = Date.now() - 7 * 24 * 60 * 60 * 1000;
      const query = r
        .db('mydb')
        .table('orders')
        .filter((row) =>
          row.field('status').eq('completed').and(row.field('created_at').gt(oneWeekAgo))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });
});
