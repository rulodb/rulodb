import { Connection } from '../src/connection';
import { r, RQuery } from '../src/query';
import { BinaryOp_Operator } from '../src/rulo';

// Mock the Connection class
jest.mock('../src/connection');
const MockedConnection = Connection as jest.MockedClass<typeof Connection>;

describe('OR Operations', () => {
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

  describe('or() with simple comparisons', () => {
    it('should create OR comparison with string values', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').eq('John').or(row.field('name').eq('Jane')));

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should create OR comparison with numeric values', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').lt(18).or(row.field('age').gt(65)));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should create OR comparison with different field types', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('role').eq('admin').or(row.field('permissions').eq('all')));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute OR query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'John', email: 'john@example.com' },
          { id: 'user2', name: 'Jane', email: 'jane@example.com' }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').eq('John').or(row.field('name').eq('Jane')));
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
                  op: BinaryOp_Operator.OR,
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

  describe('or() with multiple conditions', () => {
    it('should handle multiple OR conditions', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row
            .field('role')
            .eq('admin')
            .or(row.field('role').eq('moderator'))
            .or(row.field('role').eq('editor'))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle complex OR with different operators', () => {
      const query = r
        .db('mydb')
        .table('orders')
        .filter((row) =>
          row
            .field('status')
            .eq('pending')
            .or(row.field('status').eq('processing'))
            .or(row.field('status').eq('shipped'))
            .or(row.field('priority').eq('urgent'))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle OR with boolean and numeric conditions', () => {
      const query = r
        .db('mydb')
        .table('products')
        .filter((row) =>
          row
            .field('featured')
            .eq(true)
            .or(row.field('discount').gt(0.5))
            .or(row.field('category').eq('electronics'))
            .or(row.field('rating').ge(4.8))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute complex OR query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', role: 'admin', active: true },
          { id: 'user2', role: 'moderator', verified: true }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row
            .field('role')
            .eq('admin')
            .or(row.field('role').eq('moderator'))
            .or(row.field('verified').eq(true))
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

  describe('or() with typed interfaces', () => {
    interface User {
      id: string;
      name: string;
      email: string;
      age: number;
      active: boolean;
      role?: string;
    }

    it('should work with typed row access', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('role').eq('admin').or(row.field('role').eq('moderator')));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with optional typed fields', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('active').eq(false).or(row.field('email').eq('')));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle type-safe comparisons', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) =>
          row
            .field('role')
            .eq('admin')
            .or(row.field('active').eq(true))
            .or(row.field('name').ne(''))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('or() with AND combinations', () => {
    it('should handle OR combined with AND', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row
            .field('role')
            .eq('admin')
            .and(row.field('active').eq(true))
            .or(row.field('role').eq('superuser'))
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
            .field('category')
            .eq('electronics')
            .or(row.field('category').eq('computers'))
            .and(row.field('price').lt(1000).or(row.field('discount').gt(0.2)))
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
            .field('role')
            .eq('admin')
            .or(row.field('role').eq('moderator'))
            .and(row.field('active').eq(true).or(row.field('verified').eq(true)))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('or() with different data types', () => {
    it('should handle OR with numeric comparisons', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').lt(18).or(row.field('age').gt(65)));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle OR with float comparisons', () => {
      const query = r
        .db('mydb')
        .table('products')
        .filter((row) => row.field('price').lt(10).or(row.field('price').gt(100)));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle OR with boolean and string combinations', () => {
      const query = r
        .db('mydb')
        .table('events')
        .filter((row) =>
          row
            .field('day_of_week')
            .eq('saturday')
            .or(row.field('day_of_week').eq('sunday'))
            .or(row.field('is_holiday').eq(true))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle OR with multiple string comparisons', () => {
      const query = r
        .db('mydb')
        .table('orders')
        .filter((row) =>
          row
            .field('status')
            .eq('pending')
            .or(row.field('status').eq('processing'))
            .or(row.field('status').eq('shipped'))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('or() error handling', () => {
    it('should handle field not found errors', async () => {
      const error = new Error('Field not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').eq('John').or(row.field('nonexistent').eq('value')));

      await expect(query.run(mockConnection)).rejects.toThrow('Field not found');
    });

    it('should handle type mismatch errors', async () => {
      const error = new Error('Type mismatch');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').eq(25).or(row.field('name').eq(123)));

      await expect(query.run(mockConnection)).rejects.toThrow('Type mismatch');
    });

    it('should handle table not found errors', async () => {
      const error = new Error('Table not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('nonexistent')
        .filter((row) => row.field('name').eq('John').or(row.field('active').eq(true)));

      await expect(query.run(mockConnection)).rejects.toThrow('Table not found');
    });
  });

  describe('or() with nested field access', () => {
    it('should work with nested object fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row.field('profile.verified').eq(true).or(row.field('settings.notifications').eq(false))
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
            .eq(true)
            .or(row.field('profile.contact.phone').ne(''))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with array index access', () => {
      const query = r
        .db('mydb')
        .table('posts')
        .filter((row) =>
          row.field('tags.0').eq('technology').or(row.field('categories.1').eq('programming'))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('or() with pagination', () => {
    it('should handle OR queries with cursor options', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'John', role: 'admin' },
          { id: 'user2', name: 'Jane', role: 'moderator' }
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
        .filter((row) => row.field('role').eq('admin').or(row.field('role').eq('moderator')));
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
          active: i % 2 === 0
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
        .filter((row) => row.field('active').eq(true).or(row.field('name').match('Admin.*')));
      const result = await query.run(mockConnection, { batchSize: 50 });

      expect(result).toBeDefined();
    });
  });

  describe('or() with query options', () => {
    it('should pass query options through OR operations', async () => {
      const mockResult = {
        items: [{ id: 'user1', name: 'John', role: 'admin' }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('role').eq('admin').or(row.field('role').eq('moderator')));
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

  describe('or() with default database', () => {
    it('should work with default database', () => {
      const query = r
        .db()
        .table('users')
        .filter((row) => row.field('active').eq(true).or(row.field('role').eq('admin')));

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

  describe('or() performance considerations', () => {
    it('should work efficiently with indexed fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row.field('email').eq('john@example.com').or(row.field('username').eq('john'))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with orderBy after OR filtering', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('active').eq(true).or(row.field('role').eq('admin')))
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

    it('should work with limit after OR filtering', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('role').eq('admin').or(row.field('role').eq('moderator')))
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
});
