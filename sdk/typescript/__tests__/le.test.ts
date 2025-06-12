import { Connection } from '../src/connection';
import { r, RQuery } from '../src/query';
import { BinaryOp_Operator } from '../src/rulo';

// Mock the Connection class
jest.mock('../src/connection');
const MockedConnection = Connection as jest.MockedClass<typeof Connection>;

describe('Less Than or Equal (le) Operations', () => {
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

  describe('le() with numeric values', () => {
    it('should create less than or equal comparison with integer value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').le(65));

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should create less than or equal comparison with float value', () => {
      const query = r
        .db('mydb')
        .table('products')
        .filter((row) => row.field('price').le(99.99));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle zero comparison', () => {
      const query = r
        .db('mydb')
        .table('accounts')
        .filter((row) => row.field('balance').le(0));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle negative number comparison', () => {
      const query = r
        .db('mydb')
        .table('temperatures')
        .filter((row) => row.field('celsius').le(-5));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle large number comparison', () => {
      const largeNumber = 1000000;
      const query = r
        .db('mydb')
        .table('statistics')
        .filter((row) => row.field('count').le(largeNumber));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute numeric le query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice', age: 25 },
          { id: 'user2', name: 'Bob', age: 65 },
          { id: 'user3', name: 'Charlie', age: 30 }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').le(65));
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
                  op: BinaryOp_Operator.LE,
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

  describe('le() with string values', () => {
    it('should create less than or equal comparison with string value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').le('M'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle empty string comparison', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('description').le(''));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle unicode string comparison', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').le('José'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute string le query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice', email: 'alice@example.com' },
          { id: 'user2', name: 'Bob', email: 'bob@example.com' },
          { id: 'user3', name: 'M', email: 'm@example.com' }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').le('M'));
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
                  op: BinaryOp_Operator.LE,
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

  describe('le() with date and timestamp values', () => {
    it('should create less than or equal comparison with timestamp', () => {
      const timestamp = 1640995200000; // 2022-01-01 00:00:00 UTC
      const query = r
        .db('mydb')
        .table('events')
        .filter((row) => row.field('created_at').le(timestamp));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle date string comparison', () => {
      const query = r
        .db('mydb')
        .table('bookings')
        .filter((row) => row.field('check_out').le('2024-01-01'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute timestamp le query successfully', async () => {
      const timestamp = 1640995200000;
      const mockResult = {
        items: [
          { id: 'event1', name: 'Past Meeting', created_at: 1640908800000 },
          { id: 'event2', name: 'Current Conference', created_at: 1640995200000 }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('events')
        .filter((row) => row.field('created_at').le(timestamp));
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

  describe('le() with logical operations', () => {
    it('should chain le with AND operation', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').le(65).and(row.field('score').le(100)));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should chain le with OR operation', () => {
      const query = r
        .db('mydb')
        .table('products')
        .filter((row) => row.field('price').le(50).or(row.field('weight').le(1)));

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
            .le(65)
            .and(row.field('score').le(100).or(row.field('premium').eq(false)))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should chain multiple le operations', () => {
      const query = r
        .db('mydb')
        .table('products')
        .filter((row) =>
          row.field('price').le(100).and(row.field('weight').le(5)).and(row.field('size').le(10))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute chained le query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice', age: 25, score: 85 },
          { id: 'user2', name: 'Bob', age: 65, score: 100 }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').le(65).and(row.field('score').le(100)));
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
                          op: BinaryOp_Operator.LE
                        })
                      })
                    })
                  }),
                  right: expect.objectContaining({
                    subquery: expect.objectContaining({
                      expression: expect.objectContaining({
                        binary: expect.objectContaining({
                          op: BinaryOp_Operator.LE
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

  describe('le() with nested field access', () => {
    it('should work with nested object fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('profile.age').le(30));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with deeply nested fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('settings.limits.maxSize').le(1000));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with array index access', () => {
      const query = r
        .db('mydb')
        .table('games')
        .filter((row) => row.field('scores.0').le(500));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('le() with typed interfaces', () => {
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
        .filter((row) => row.field('age').le(65));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with optional typed fields', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('score').le(50));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle type-safe comparisons', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('name').le('Z'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('le() error handling', () => {
    it('should handle field not found errors', async () => {
      const error = new Error('Field not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('nonexistent').le(10));

      await expect(query.run(mockConnection)).rejects.toThrow('Field not found');
    });

    it('should handle type mismatch errors', async () => {
      const error = new Error('Type mismatch');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').le(123));

      await expect(query.run(mockConnection)).rejects.toThrow('Type mismatch');
    });

    it('should handle table not found errors', async () => {
      const error = new Error('Table not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('nonexistent')
        .filter((row) => row.field('age').le(65));

      await expect(query.run(mockConnection)).rejects.toThrow('Table not found');
    });
  });

  describe('le() with pagination', () => {
    it('should handle le queries with cursor options', async () => {
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
        .filter((row) => row.field('age').le(65));
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
        .filter((row) => row.field('age').le(65));
      const result = await query.run(mockConnection, { batchSize: 50 });

      expect(result).toBeDefined();
    });
  });

  describe('le() with query options', () => {
    it('should pass query options through le operations', async () => {
      const mockResult = {
        items: [{ id: 'user1', age: 25 }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').le(65));
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

  describe('le() with default database', () => {
    it('should work with default database', () => {
      const query = r
        .db()
        .table('users')
        .filter((row) => row.field('age').le(65));

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

  describe('le() performance considerations', () => {
    it('should work efficiently with indexed fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('created_at').le(Date.now()));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with orderBy after le filtering', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').le(65))
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

    it('should work with limit after le filtering', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').le(65))
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

  describe('le() practical use cases', () => {
    it('should filter users up to retirement age', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').le(65));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should filter affordable products', () => {
      const query = r
        .db('mydb')
        .table('products')
        .filter((row) => row.field('price').le(100.0));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should filter events up to a deadline', () => {
      const deadline = new Date('2024-12-31').getTime();
      const query = r
        .db('mydb')
        .table('events')
        .filter((row) => row.field('timestamp').le(deadline));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should filter files within size limit', () => {
      const query = r
        .db('mydb')
        .table('files')
        .filter((row) => row.field('size').le(10485760)); // <= 10MB

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });
});
