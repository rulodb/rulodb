import { Connection } from '../src/connection';
import { r, RQuery } from '../src/query';

// Mock the Connection class
jest.mock('../src/connection');

describe('Skip Operations', () => {
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

  describe('skip() basic functionality', () => {
    it('should create skip query with positive number', () => {
      const query = r.db('mydb').table('users').skip(10);

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.skip).toBeDefined();
      expect(query._query.skip!.count).toBe(10);
      expect(query._query.skip!.source).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
    });

    it('should create skip query with small number', () => {
      const query = r.db('mydb').table('users').skip(1);

      expect(query._query.skip).toBeDefined();
      expect(query._query.skip!.count).toBe(1);
    });

    it('should create skip query with large number', () => {
      const query = r.db('mydb').table('users').skip(1000000);

      expect(query._query.skip).toBeDefined();
      expect(query._query.skip!.count).toBe(1000000);
    });

    it('should execute skip query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user11', name: 'User 11', age: 25 },
          { id: 'user12', name: 'User 12', age: 30 },
          { id: 'user13', name: 'User 13', age: 35 }
        ],
        cursor: { startKey: 'user13', batchSize: 50 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').skip(10);
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
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
      expect(result).toBeDefined();
    });
  });

  describe('skip() with zero and edge cases', () => {
    it('should create skip query with zero', () => {
      const query = r.db('mydb').table('users').skip(0);

      expect(query._query.skip).toBeDefined();
      expect(query._query.skip!.count).toBe(0);
    });

    it('should handle zero skip execution', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'User 1', age: 25 },
          { id: 'user2', name: 'User 2', age: 30 }
        ],
        cursor: { startKey: 'user2', batchSize: 50 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').skip(0);
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
        skip: {
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

    it('should handle very large skip values', () => {
      const largeSkip = Number.MAX_SAFE_INTEGER;
      const query = r.db('mydb').table('users').skip(largeSkip);

      expect(query._query.skip!.count).toBe(largeSkip);
    });

    it('should handle negative skip values', () => {
      // Note: The behavior with negative values depends on implementation
      // This test documents current behavior
      const query = r.db('mydb').table('users').skip(-5);

      expect(query._query.skip).toBeDefined();
      expect(query._query.skip!.count).toBe(-5);
    });
  });

  describe('skip() chaining from different sources', () => {
    it('should chain skip from table scan', () => {
      const query = r.db('mydb').table('users').skip(5);

      expect(query._query.skip!.source).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
    });

    it('should chain skip from getAll', () => {
      const query = r.db('mydb').table('users').getAll('user1', 'user2').skip(1);

      expect(query._query.skip!.source).toEqual({
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

    it('should chain skip from filter', () => {
      const query = r.db('mydb').table('users').filter({ active: true }).skip(10);

      expect(query._query.skip!.source).toEqual({
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

    it('should chain skip from orderBy', () => {
      const query = r.db('mydb').table('users').orderBy('name').skip(5);

      expect(query._query.skip!.source).toEqual({
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

  describe('skip() chaining to other operations', () => {
    it('should allow chaining limit after skip', () => {
      const query = r.db('mydb').table('users').skip(10).limit(5);

      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.count).toBe(5);
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
          count: 10
        }
      });
    });

    it('should allow chaining filter after skip', () => {
      const query = r.db('mydb').table('users').skip(20).filter({ active: true });

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.source).toEqual({
        skip: {
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

    it('should allow chaining orderBy after skip', () => {
      const query = r.db('mydb').table('users').skip(50).orderBy('name');

      expect(query._query.orderBy).toBeDefined();
      expect(query._query.orderBy!.source).toEqual({
        skip: {
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

    it('should allow chaining count after skip', () => {
      const query = r.db('mydb').table('users').skip(100).count();

      expect(query._query.count).toBeDefined();
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
        .skip(20)
        .limit(10);

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
          count: 20
        }
      });
    });
  });

  describe('skip() with limit interaction and reordering', () => {
    it('should handle limit before skip correctly', () => {
      const query = r.db('mydb').table('users').limit(20).skip(5);

      // The skip method should reorder to put skip before limit
      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.count).toBe(20);
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

    it('should handle skip after limit with proper reordering', () => {
      const query = r.db('mydb').table('users').limit(10).skip(3);

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
          count: 3
        }
      });
    });

    it('should handle multiple skip operations correctly', () => {
      // Multiple skips should accumulate or the last one should win
      const query = r.db('mydb').table('users').skip(5).skip(10);

      expect(query._query.skip).toBeDefined();
      expect(query._query.skip!.count).toBe(10); // Last skip should win
    });

    it('should execute skip-limit combination successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user8', name: 'User 8' },
          { id: 'user9', name: 'User 9' },
          { id: 'user10', name: 'User 10' }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').skip(7).limit(3);
      const result = await query.run(mockConnection);

      expect(result).toBeDefined();
    });
  });

  describe('skip() for pagination scenarios', () => {
    it('should support offset-based pagination', () => {
      const pageSize = 20;
      const pageNumber = 3; // 0-based
      const offset = pageNumber * pageSize;

      const query = r.db('mydb').table('posts').orderBy('created_at').skip(offset).limit(pageSize);

      expect(query._query.limit!.count).toBe(pageSize);
      expect(query._query.limit!.source).toEqual({
        skip: {
          source: {
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
          },
          count: offset
        }
      });
    });

    it('should handle first page (skip 0)', () => {
      const query = r.db('mydb').table('users').skip(0).limit(10);

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
          count: 0
        }
      });
    });

    it('should handle deep pagination', () => {
      const query = r.db('mydb').table('logs').skip(10000).limit(100);

      expect(query._query.limit!.source).toEqual({
        skip: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'logs'
              }
            }
          },
          count: 10000
        }
      });
    });

    it('should execute pagination query successfully', async () => {
      const mockResult = {
        items: new Array(10).fill(0).map((_, i) => ({
          id: `post${i + 21}`,
          title: `Post ${i + 21}`,
          created_at: new Date().toISOString()
        })),
        cursor: { startKey: 'post30', batchSize: 50 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('posts').skip(20).limit(10);
      const result = await query.run(mockConnection);

      expect(result).toBeDefined();
    });
  });

  describe('skip() with cursor-based pagination', () => {
    it('should handle skip queries with cursor options', async () => {
      const mockResult = {
        items: [
          { id: 'user11', name: 'User 11' },
          { id: 'user12', name: 'User 12' }
        ],
        cursor: { startKey: 'user12', batchSize: 25 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').skip(10);
      const result = await query.run(mockConnection, { batchSize: 25, startKey: 'start' });

      expect(mockConnection.query).toHaveBeenCalledWith({
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

    it('should handle skip with large batch sizes', async () => {
      const mockResult = {
        items: new Array(50).fill(0).map((_, i) => ({
          id: `user${i + 101}`,
          name: `User ${i + 101}`
        })),
        cursor: { startKey: 'user150', batchSize: 100 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').skip(100);
      const result = await query.run(mockConnection, { batchSize: 100 });

      expect(result).toBeDefined();
    });
  });

  describe('skip() with typed interfaces', () => {
    interface User {
      id: string;
      name: string;
      email: string;
      age: number;
    }

    it('should work with typed table queries', () => {
      const query = r.db('mydb').table<User>('users').skip(5);

      expect(query._query.skip).toBeDefined();
      expect(query._query.skip!.count).toBe(5);
    });

    it('should maintain type information through skip', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('age').gt(18))
        .skip(10)
        .limit(5);

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.limit!.source).toEqual({
        skip: {
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
          count: 10
        }
      });
    });
  });

  describe('skip() error handling', () => {
    it('should handle table not found errors', async () => {
      const error = new Error('Table not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('nonexistent').skip(10);

      await expect(query.run(mockConnection)).rejects.toThrow('Table not found');
    });

    it('should handle database not found errors', async () => {
      const error = new Error('Database not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('nonexistent').table('users').skip(10);

      await expect(query.run(mockConnection)).rejects.toThrow('Database not found');
    });

    it('should handle connection errors during skip execution', async () => {
      const error = new Error('Connection lost');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('users').skip(5);

      await expect(query.run(mockConnection)).rejects.toThrow('Connection lost');
    });

    it('should handle skip larger than available data gracefully', async () => {
      const mockResult = {
        items: [],
        cursor: undefined // No data available after skip
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').skip(1000000);
      const result = await query.run(mockConnection);

      expect(result).toBeDefined();
      expect(mockConnection.query).toHaveBeenCalled();
    });
  });

  describe('skip() with query options', () => {
    it('should pass query options through skip operations', async () => {
      const mockResult = {
        items: [{ id: 'user6', name: 'User 6' }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').skip(5);
      await query.run(mockConnection, { timeout: 5000, explain: true });

      expect(mockConnection.query).toHaveBeenCalledWith({
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
        },
        options: {
          timeoutMs: 5000,
          explain: true
        }
      });
    });

    it('should handle explain option with skip', async () => {
      const mockResult = {
        items: [],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').skip(1);
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

  describe('skip() with default database', () => {
    it('should work with default database', () => {
      const query = r.db().table('users').skip(10);

      expect(query._query.skip!.source).toEqual({
        table: {
          table: {
            database: { name: 'default' },
            name: 'users'
          }
        }
      });
    });
  });

  describe('skip() performance and optimization scenarios', () => {
    it('should handle range queries efficiently', () => {
      const query = r.db('mydb').table('events').orderBy('timestamp').skip(1000).limit(100);

      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.count).toBe(100);
      expect(query._query.limit!.source).toEqual({
        skip: {
          source: {
            orderBy: {
              source: {
                table: {
                  table: {
                    database: { name: 'mydb' },
                    name: 'events'
                  }
                }
              },
              fields: expect.any(Array)
            }
          },
          count: 1000
        }
      });
    });

    it('should work with filtered skip operations', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('active').eq(true))
        .orderBy('created_at')
        .skip(50)
        .limit(25);

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
              fields: expect.any(Array)
            }
          },
          count: 50
        }
      });
    });

    it('should handle large table scans with skip', () => {
      const query = r.db('analytics').table('events').skip(1000000);

      expect(query._query.skip!.count).toBe(1000000);
    });
  });

  describe('skip() edge cases and boundary conditions', () => {
    it('should handle skip with float values (should be used as-is)', () => {
      const query = r.db('mydb').table('users').skip(10.7);

      expect(query._query.skip!.count).toBe(10.7);
    });

    it('should handle very large numbers close to MAX_SAFE_INTEGER', () => {
      const largeNumber = Number.MAX_SAFE_INTEGER - 1;
      const query = r.db('mydb').table('users').skip(largeNumber);

      expect(query._query.skip!.count).toBe(largeNumber);
    });

    it('should work with empty table and skip', async () => {
      const mockResult = {
        items: [],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('empty_table').skip(5);
      const result = await query.run(mockConnection);

      expect(result).toBeDefined();
      expect(mockConnection.query).toHaveBeenCalled();
    });

    it('should handle skip equal to table size', async () => {
      const mockResult = {
        items: [],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('small_table').skip(100);
      const result = await query.run(mockConnection);

      expect(result).toBeDefined();
    });
  });

  describe('skip() with update and delete operations', () => {
    it('should work with delete operations', () => {
      const query = r.db('mydb').table('users').skip(5).delete();

      expect(query._query.delete).toBeDefined();
      expect(query._query.delete!.source).toEqual({
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

    it('should work with update operations', () => {
      const query = r
        .db('mydb')
        .table('users')
        .skip(10)
        .update({ last_processed: new Date().toISOString() });

      expect(query._query.update).toBeDefined();
      expect(query._query.update!.source).toEqual({
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

    it('should work with batch processing patterns', () => {
      const query = r
        .db('mydb')
        .table('pending_jobs')
        .orderBy('created_at')
        .skip(100)
        .limit(50)
        .update({ status: 'processing' });

      expect(query._query.update).toBeDefined();
      expect(query._query.update!.source).toEqual({
        limit: {
          source: {
            skip: {
              source: {
                orderBy: {
                  source: {
                    table: {
                      table: {
                        database: { name: 'mydb' },
                        name: 'pending_jobs'
                      }
                    }
                  },
                  fields: expect.any(Array)
                }
              },
              count: 100
            }
          },
          count: 50
        }
      });
    });
  });

  describe('skip() with aggregation operations', () => {
    it('should work with count after skip', async () => {
      const mockResult = {
        result: 45,
        metadata: {
          queryId: 'test',
          timestamp: '123',
          serverVersion: '1.0'
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').skip(10).count();
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
        count: {
          source: {
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
          }
        }
      });
      expect(result).toBe(45);
    });
  });
});
