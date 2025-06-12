import { Connection } from '../src/connection';
import { r } from '../src/query';
import { RQuery } from '../src/query';

// Mock the Connection class
jest.mock('../src/connection');

describe('Count Operations', () => {
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

  describe('count() basic functionality', () => {
    it('should create count query from table scan', () => {
      const query = r.db('mydb').table('users').count();

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.count).toBeDefined();
      expect(query._query.count!.source).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
    });

    it('should execute count query successfully and return number', async () => {
      const mockResult = {
        result: 42
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').count();
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
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
      expect(result).toBe(42);
    });

    it('should handle zero count', async () => {
      const mockResult = {
        result: 0
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('empty_table').count();
      const result = await query.run(mockConnection);

      expect(result).toBe(0);
    });

    it('should handle large count values', async () => {
      const largeCount = 1000000;
      const mockResult = {
        result: largeCount
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('analytics').table('events').count();
      const result = await query.run(mockConnection);

      expect(result).toBe(largeCount);
    });
  });

  describe('count() chaining from different sources', () => {
    it('should chain count from getAll', () => {
      const query = r.db('mydb').table('users').getAll('user1', 'user2').count();

      expect(query._query.count!.source).toEqual({
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

    it('should chain count from filter', () => {
      const query = r.db('mydb').table('users').filter({ active: true }).count();

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

    it('should chain count from orderBy', () => {
      const query = r.db('mydb').table('users').orderBy('name').count();

      expect(query._query.count!.source).toEqual({
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

    it('should chain count from limit', () => {
      const query = r.db('mydb').table('users').limit(100).count();

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

    it('should chain count from skip', () => {
      const query = r.db('mydb').table('users').skip(10).count();

      expect(query._query.count!.source).toEqual({
        skip: {
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

  describe('count() with filtered data', () => {
    it('should execute count on filtered active users', async () => {
      const mockResult = {
        result: 15
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').filter({ active: true }).count();
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
        count: {
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
          }
        }
      });
      expect(result).toBe(15);
    });

    it('should count filtered results with function predicate', async () => {
      const mockResult = {
        result: 8
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.age.gt(18))
        .count();
      const result = await query.run(mockConnection);

      expect(result).toBe(8);
    });

    it('should count filtered results with complex conditions', async () => {
      const mockResult = {
        result: 3
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.active.eq(true).and(row.role.eq('admin')))
        .count();
      const result = await query.run(mockConnection);

      expect(result).toBe(3);
    });

    it('should count with pattern matching filter', async () => {
      const mockResult = {
        result: 25
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.email.match('@company\\.com$'))
        .count();
      const result = await query.run(mockConnection);

      expect(result).toBe(25);
    });
  });

  describe('count() with complex query chains', () => {
    it('should count results from complex filter-orderBy-limit chain', async () => {
      const mockResult = {
        result: 10
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('posts')
        .filter({ published: true })
        .orderBy({ field: 'created_at', direction: 'desc' })
        .limit(10)
        .count();

      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
        count: {
          source: {
            limit: {
              source: {
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
              },
              count: 10
            }
          }
        }
      });
      expect(result).toBe(10);
    });

    it('should count results with skip and limit', async () => {
      const mockResult = {
        result: 5
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').skip(20).limit(10).count();

      const result = await query.run(mockConnection);
      expect(result).toBe(5);
    });

    it('should count getAll results', async () => {
      const mockResult = {
        result: 3
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').getAll('user1', 'user2', 'user3').count();

      const result = await query.run(mockConnection);
      expect(result).toBe(3);
    });
  });

  describe('count() with typed interfaces', () => {
    interface User {
      id: string;
      name: string;
      email: string;
      age: number;
      active: boolean;
    }

    it('should work with typed table queries', async () => {
      const mockResult = {
        result: 50
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table<User>('users').count();
      const result = await query.run(mockConnection);

      expect(result).toBe(50);
    });

    it('should count typed filtered results', async () => {
      const mockResult = {
        result: 12
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.age.gt(21))
        .count();

      const result = await query.run(mockConnection);
      expect(result).toBe(12);
    });
  });

  describe('count() error handling', () => {
    it('should handle table not found errors', async () => {
      const error = new Error('Table not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('nonexistent').count();

      await expect(query.run(mockConnection)).rejects.toThrow('Table not found');
    });

    it('should handle database not found errors', async () => {
      const error = new Error('Database not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('nonexistent').table('users').count();

      await expect(query.run(mockConnection)).rejects.toThrow('Database not found');
    });

    it('should handle permission errors', async () => {
      const error = new Error('Access denied');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('restricted').table('sensitive_data').count();

      await expect(query.run(mockConnection)).rejects.toThrow('Access denied');
    });

    it('should handle connection errors during count execution', async () => {
      const error = new Error('Connection lost');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('users').count();

      await expect(query.run(mockConnection)).rejects.toThrow('Connection lost');
    });

    it('should handle field not found in filter before count', async () => {
      const error = new Error('Field not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('nonexistent').eq('value'))
        .count();

      await expect(query.run(mockConnection)).rejects.toThrow('Field not found');
    });
  });

  describe('count() with query options', () => {
    it('should pass query options through count operations', async () => {
      const mockResult = {
        result: 100
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').count();
      await query.run(mockConnection, { timeout: 5000, explain: true });

      expect(mockConnection.query).toHaveBeenCalledWith({
        count: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          }
        },
        options: {
          timeoutMs: 5000,
          explain: true
        }
      });
    });

    it('should handle explain option with count', async () => {
      const mockResult = {
        result: 42
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').filter({ active: true }).count();
      await query.run(mockConnection, { explain: true });

      expect(mockConnection.query).toHaveBeenCalledWith(
        expect.objectContaining({
          options: expect.objectContaining({
            explain: true
          })
        })
      );
    });

    it('should handle timeout option with count', async () => {
      const mockResult = {
        result: 1000
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('analytics').table('large_events').count();
      await query.run(mockConnection, { timeout: 30000 });

      expect(mockConnection.query).toHaveBeenCalledWith(
        expect.objectContaining({
          options: expect.objectContaining({
            timeoutMs: 30000
          })
        })
      );
    });
  });

  describe('count() with default database', () => {
    it('should work with default database', () => {
      const query = r.db().table('users').count();

      expect(query._query.count!.source).toEqual({
        table: {
          table: {
            database: { name: 'default' },
            name: 'users'
          }
        }
      });
    });

    it('should execute count with default database', async () => {
      const mockResult = {
        result: 75
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db().table('users').count();
      const result = await query.run(mockConnection);

      expect(result).toBe(75);
    });
  });

  describe('count() performance considerations', () => {
    it('should handle count on indexed fields efficiently', async () => {
      const mockResult = {
        result: 500
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.id.eq('specific_id'))
        .count();

      const result = await query.run(mockConnection);
      expect(result).toBe(500);
    });

    it('should handle count on large tables', async () => {
      const mockResult = {
        result: 10000000
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('analytics').table('page_views').count();
      const result = await query.run(mockConnection);

      expect(result).toBe(10000000);
    });

    it('should work efficiently with filtered counts', async () => {
      const mockResult = {
        result: 2500
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('ecommerce')
        .table('orders')
        .filter((row) => row.status.eq('completed'))
        .count();

      const result = await query.run(mockConnection);
      expect(result).toBe(2500);
    });
  });

  describe('count() edge cases', () => {
    it('should handle count after multiple filters', async () => {
      const mockResult = {
        result: 1
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter({ active: true })
        .filter((row) => row.age.gt(25))
        .filter((row) => row.role.eq('admin'))
        .count();

      const result = await query.run(mockConnection);
      expect(result).toBe(1);
    });

    it('should handle count with no matching records', async () => {
      const mockResult = {
        result: 0
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.email.eq('nonexistent@example.com'))
        .count();

      const result = await query.run(mockConnection);
      expect(result).toBe(0);
    });

    it('should handle count with complex nested conditions', async () => {
      const mockResult = {
        result: 7
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row.status.eq('active').and(row.role.eq('admin').or(row.permissions.eq('write')))
        )
        .count();

      const result = await query.run(mockConnection);
      expect(result).toBe(7);
    });

    it('should handle count with orderBy (count ignores ordering)', async () => {
      const mockResult = {
        result: 100
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').orderBy('name').count();

      const result = await query.run(mockConnection);
      expect(result).toBe(100);
    });
  });

  describe('count() result type consistency', () => {
    it('should always return a number', async () => {
      const mockResult = {
        result: 42
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').count();
      const result = await query.run(mockConnection);

      expect(typeof result).toBe('number');
      expect(result).toBe(42);
    });

    it('should handle string numbers from protocol', async () => {
      // Simulating a case where the protocol returns string numbers
      const mockResult = {
        result: 42 // Should be converted to number
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').count();
      const result = await query.run(mockConnection);

      expect(typeof result).toBe('number');
      expect(result).toBe(42);
    });
  });

  describe('count() with cursor pagination', () => {
    it('should not use cursor options for count queries', async () => {
      const mockResult = {
        result: 100
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').count();
      const result = await query.run(mockConnection, { batchSize: 25, startKey: 'test' });

      // Count queries should ignore cursor options since they return a single number
      expect(mockConnection.query).toHaveBeenCalledWith({
        count: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          }
        },
        cursor: {
          batchSize: 25,
          startKey: 'test'
        },
        options: {
          explain: false,
          timeoutMs: 0
        }
      });
      expect(result).toBe(100);
    });
  });
});
