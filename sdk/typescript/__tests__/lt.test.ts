import { Connection } from '../src/connection';
import { r, RQuery } from '../src/query';
import { BinaryOp_Operator } from '../src/rulo';

// Mock the Connection class
jest.mock('../src/connection');
const MockedConnection = Connection as jest.MockedClass<typeof Connection>;

describe('Less Than (lt) Operations', () => {
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

  describe('lt() with numeric values', () => {
    it('should create less than comparison with integer value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').lt(65));

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should create less than comparison with float value', () => {
      const query = r
        .db('mydb')
        .table('products')
        .filter((row) => row.field('price').lt(99.99));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle zero comparison', () => {
      const query = r
        .db('mydb')
        .table('accounts')
        .filter((row) => row.field('balance').lt(0));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle negative number comparison', () => {
      const query = r
        .db('mydb')
        .table('temperatures')
        .filter((row) => row.field('celsius').lt(-5));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle large number comparison', () => {
      const largeNumber = 1000000;
      const query = r
        .db('mydb')
        .table('statistics')
        .filter((row) => row.field('count').lt(largeNumber));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute numeric lt query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice', age: 25 },
          { id: 'user2', name: 'Bob', age: 30 }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').lt(65));
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
                  op: BinaryOp_Operator.LT,
                  left: expect.objectContaining({
                    subquery: expect.objectContaining({
                      expression: expect.objectContaining({
                        field: expect.objectContaining({
                          path: ['age'],
                          separator: '.'
                        })
                      })
                    })
                  }),
                  right: expect.objectContaining({
                    literal: expect.objectContaining({
                      int: '65'
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

  describe('lt() with string values', () => {
    it('should create less than comparison with string value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').lt('M'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle empty string comparison', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('description').lt(''));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle unicode string comparison', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').lt('José'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute string lt query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice', email: 'alice@example.com' },
          { id: 'user2', name: 'Bob', email: 'bob@example.com' }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').lt('M'));
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
                  op: BinaryOp_Operator.LT,
                  left: expect.objectContaining({
                    subquery: expect.objectContaining({
                      expression: expect.objectContaining({
                        field: expect.objectContaining({
                          path: ['name'],
                          separator: '.'
                        })
                      })
                    })
                  }),
                  right: expect.objectContaining({
                    literal: expect.objectContaining({
                      string: 'M'
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

  describe('lt() with date and timestamp values', () => {
    it('should create less than comparison with timestamp', () => {
      const timestamp = 1640995200000; // 2022-01-01 00:00:00 UTC
      const query = r
        .db('mydb')
        .table('events')
        .filter((row) => row.field('created_at').lt(timestamp));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle date string comparison', () => {
      const query = r
        .db('mydb')
        .table('bookings')
        .filter((row) => row.field('check_out').lt('2024-01-01'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute timestamp lt query successfully', async () => {
      const timestamp = 1640995200000;
      const mockResult = {
        items: [
          { id: 'event1', name: 'Old Meeting', created_at: 1640908800000 },
          { id: 'event2', name: 'Past Conference', created_at: 1640822400000 }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('events')
        .filter((row) => row.field('created_at').lt(timestamp));
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
        filter: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'events'
              }
            }
          },
          predicate: expect.any(Object)
        }
      });
      expect(result).toBeDefined();
    });
  });

  describe('lt() with logical operations', () => {
    it('should chain lt with AND operation', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').lt(65).and(row.field('score').lt(100)));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should chain lt with OR operation', () => {
      const query = r
        .db('mydb')
        .table('products')
        .filter((row) => row.field('price').lt(50).or(row.field('weight').lt(1)));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle complex logical combinations', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row
            .field('age')
            .lt(65)
            .and(row.field('score').lt(100).or(row.field('premium').eq(false)))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should chain multiple lt operations', () => {
      const query = r
        .db('mydb')
        .table('products')
        .filter((row) =>
          row.field('price').lt(100).and(row.field('weight').lt(5)).and(row.field('size').lt(10))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute chained lt query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice', age: 25, score: 85 },
          { id: 'user2', name: 'Bob', age: 30, score: 90 }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').lt(65).and(row.field('score').lt(100)));
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
                          op: BinaryOp_Operator.LT
                        })
                      })
                    })
                  }),
                  right: expect.objectContaining({
                    subquery: expect.objectContaining({
                      expression: expect.objectContaining({
                        binary: expect.objectContaining({
                          op: BinaryOp_Operator.LT
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

  describe('lt() with nested field access', () => {
    it('should work with nested object fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('profile.age').lt(30));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with deeply nested fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('settings.limits.maxSize').lt(1000));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with array index access', () => {
      const query = r
        .db('mydb')
        .table('games')
        .filter((row) => row.field('scores.0').lt(500));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('lt() with typed interfaces', () => {
    interface User {
      id: string;
      name: string;
      age: number;
      score?: number;
      active: boolean;
    }

    it('should work with typed row access', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('age').lt(65));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with optional typed fields', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('score').lt(50));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle type-safe comparisons', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('name').lt('Z'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('lt() error handling', () => {
    it('should handle field not found errors', async () => {
      const error = new Error('Field not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('nonexistent').lt(10));

      await expect(query.run(mockConnection)).rejects.toThrow('Field not found');
    });

    it('should handle type mismatch errors', async () => {
      const error = new Error('Type mismatch');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').lt(123));

      await expect(query.run(mockConnection)).rejects.toThrow('Type mismatch');
    });

    it('should handle table not found errors', async () => {
      const error = new Error('Table not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('nonexistent')
        .filter((row) => row.field('age').lt(65));

      await expect(query.run(mockConnection)).rejects.toThrow('Table not found');
    });
  });

  describe('lt() with pagination', () => {
    it('should handle lt queries with cursor options', async () => {
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
        .filter((row) => row.field('age').lt(65));
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
          age: 20 + i
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
        .filter((row) => row.field('age').lt(65));
      const result = await query.run(mockConnection, { batchSize: 50 });

      expect(result).toBeDefined();
    });
  });

  describe('lt() with query options', () => {
    it('should pass query options through lt operations', async () => {
      const mockResult = {
        items: [{ id: 'user1', age: 25 }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').lt(65));
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

  describe('lt() with default database', () => {
    it('should work with default database', () => {
      const query = r
        .db()
        .table('users')
        .filter((row) => row.field('age').lt(65));

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

  describe('lt() performance considerations', () => {
    it('should work efficiently with indexed fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('created_at').lt(Date.now()));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with orderBy after lt filtering', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').lt(65))
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

    it('should work with limit after lt filtering', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').lt(65))
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

  describe('lt() practical use cases', () => {
    it('should filter users below retirement age', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').lt(65));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should filter low-priced products', () => {
      const query = r
        .db('mydb')
        .table('products')
        .filter((row) => row.field('price').lt(50.0));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should filter recent events', () => {
      const oneWeekAgo = Date.now() - 7 * 24 * 60 * 60 * 1000;
      const query = r
        .db('mydb')
        .table('events')
        .filter((row) => row.field('timestamp').lt(oneWeekAgo));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should filter small files', () => {
      const query = r
        .db('mydb')
        .table('files')
        .filter((row) => row.field('size').lt(1024000)); // < 1MB

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });
});
