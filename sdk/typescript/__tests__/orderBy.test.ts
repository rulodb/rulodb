import { Connection } from '../src/connection';
import { r, RQuery } from '../src/query';
import { SortDirection } from '../src/rulo';

// Mock the Connection class
jest.mock('../src/connection');

describe('OrderBy Operations', () => {
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

  describe('orderBy() with single field', () => {
    it('should create orderBy query with single string field (default asc)', () => {
      const query = r.db('mydb').table('users').orderBy('name');

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.orderBy).toBeDefined();
      expect(query._query.orderBy!.source).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
      expect(query._query.orderBy!.fields).toHaveLength(1);
      expect(query._query.orderBy!.fields[0]).toEqual({
        fieldName: 'name',
        direction: SortDirection.ASC
      });
    });

    it('should create orderBy query with age field', () => {
      const query = r.db('mydb').table('users').orderBy('age');

      expect(query._query.orderBy).toBeDefined();
      expect(query._query.orderBy!.fields[0]).toEqual({
        fieldName: 'age',
        direction: SortDirection.ASC
      });
    });

    it('should create orderBy query with created_at field', () => {
      const query = r.db('mydb').table('posts').orderBy('created_at');

      expect(query._query.orderBy).toBeDefined();
      expect(query._query.orderBy!.fields[0]).toEqual({
        fieldName: 'created_at',
        direction: SortDirection.ASC
      });
    });

    it('should execute orderBy query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice', age: 25 },
          { id: 'user2', name: 'Bob', age: 30 },
          { id: 'user3', name: 'Charlie', age: 35 }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').orderBy('name');
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
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
            {
              fieldName: 'name',
              direction: SortDirection.ASC
            }
          ]
        }
      });
      expect(result).toBeDefined();
    });
  });

  describe('orderBy() with sort direction objects', () => {
    it('should create orderBy query with ascending direction', () => {
      const query = r.db('mydb').table('users').orderBy({ field: 'name', direction: 'asc' });

      expect(query._query.orderBy).toBeDefined();
      expect(query._query.orderBy!.fields[0]).toEqual({
        fieldName: 'name',
        direction: SortDirection.ASC
      });
    });

    it('should create orderBy query with descending direction', () => {
      const query = r.db('mydb').table('users').orderBy({ field: 'age', direction: 'desc' });

      expect(query._query.orderBy).toBeDefined();
      expect(query._query.orderBy!.fields[0]).toEqual({
        fieldName: 'age',
        direction: SortDirection.DESC
      });
    });

    it('should default to ascending when direction is not specified', () => {
      const query = r.db('mydb').table('users').orderBy({ field: 'name' });

      expect(query._query.orderBy!.fields[0]).toEqual({
        fieldName: 'name',
        direction: SortDirection.ASC
      });
    });

    it('should handle invalid direction as ascending', () => {
      const query = r
        .db('mydb')
        .table('users')
        .orderBy({ field: 'name', direction: 'invalid' as any });

      expect(query._query.orderBy!.fields[0]).toEqual({
        fieldName: 'name',
        direction: SortDirection.ASC
      });
    });

    it('should execute orderBy query with desc direction successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user3', name: 'Charlie', age: 35 },
          { id: 'user2', name: 'Bob', age: 30 },
          { id: 'user1', name: 'Alice', age: 25 }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').orderBy({ field: 'age', direction: 'desc' });
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
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
            {
              fieldName: 'age',
              direction: SortDirection.DESC
            }
          ]
        }
      });
      expect(result).toBeDefined();
    });
  });

  describe('orderBy() with multiple fields', () => {
    it('should create orderBy query with multiple string fields', () => {
      const query = r.db('mydb').table('users').orderBy('department', 'name', 'age');

      expect(query._query.orderBy).toBeDefined();
      expect(query._query.orderBy!.fields).toHaveLength(3);
      expect(query._query.orderBy!.fields[0]).toEqual({
        fieldName: 'department',
        direction: SortDirection.ASC
      });
      expect(query._query.orderBy!.fields[1]).toEqual({
        fieldName: 'name',
        direction: SortDirection.ASC
      });
      expect(query._query.orderBy!.fields[2]).toEqual({
        fieldName: 'age',
        direction: SortDirection.ASC
      });
    });

    it('should create orderBy query with mixed field types', () => {
      const query = r
        .db('mydb')
        .table('users')
        .orderBy(
          'department',
          { field: 'name', direction: 'asc' },
          { field: 'age', direction: 'desc' }
        );

      expect(query._query.orderBy).toBeDefined();
      expect(query._query.orderBy!.fields).toHaveLength(3);
      expect(query._query.orderBy!.fields[0]).toEqual({
        fieldName: 'department',
        direction: SortDirection.ASC
      });
      expect(query._query.orderBy!.fields[1]).toEqual({
        fieldName: 'name',
        direction: SortDirection.ASC
      });
      expect(query._query.orderBy!.fields[2]).toEqual({
        fieldName: 'age',
        direction: SortDirection.DESC
      });
    });

    it('should create orderBy query with all descending directions', () => {
      const query = r
        .db('mydb')
        .table('posts')
        .orderBy(
          { field: 'created_at', direction: 'desc' },
          { field: 'view_count', direction: 'desc' },
          { field: 'title', direction: 'desc' }
        );

      expect(query._query.orderBy!.fields).toHaveLength(3);
      expect(
        query._query.orderBy!.fields.every((field) => field.direction === SortDirection.DESC)
      ).toBe(true);
    });

    it('should execute multi-field orderBy query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', department: 'Engineering', name: 'Alice', age: 25 },
          { id: 'user2', department: 'Engineering', name: 'Bob', age: 30 },
          { id: 'user3', department: 'Marketing', name: 'Charlie', age: 28 }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .orderBy('department', { field: 'age', direction: 'desc' });
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
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
            { fieldName: 'department', direction: SortDirection.ASC },
            { fieldName: 'age', direction: SortDirection.DESC }
          ]
        }
      });
      expect(result).toBeDefined();
    });
  });

  describe('orderBy() chaining from different sources', () => {
    it('should chain orderBy from table scan', () => {
      const query = r.db('mydb').table('users').orderBy('name');

      expect(query._query.orderBy!.source).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
    });

    it('should chain orderBy from getAll', () => {
      const query = r.db('mydb').table('users').getAll('user1', 'user2').orderBy('name');

      expect(query._query.orderBy!.source).toEqual({
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

    it('should chain orderBy from filter', () => {
      const query = r.db('mydb').table('users').filter({ active: true }).orderBy('name');

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

    it('should chain orderBy from skip', () => {
      const query = r.db('mydb').table('users').skip(10).orderBy('name');

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
          count: 10
        }
      });
    });
  });

  describe('orderBy() chaining to other operations', () => {
    it('should allow chaining limit after orderBy', () => {
      const query = r.db('mydb').table('users').orderBy('name').limit(10);

      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.count).toBe(10);
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
          fields: [
            {
              fieldName: 'name',
              direction: SortDirection.ASC
            }
          ]
        }
      });
    });

    it('should allow chaining skip after orderBy', () => {
      const query = r.db('mydb').table('users').orderBy('name').skip(5);

      expect(query._query.skip).toBeDefined();
      expect(query._query.skip!.count).toBe(5);
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
          fields: [
            {
              fieldName: 'name',
              direction: SortDirection.ASC
            }
          ]
        }
      });
    });

    it('should allow chaining filter after orderBy', () => {
      const query = r.db('mydb').table('users').orderBy('name').filter({ active: true });

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.source).toEqual({
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
            {
              fieldName: 'name',
              direction: SortDirection.ASC
            }
          ]
        }
      });
    });

    it('should allow chaining count after orderBy', () => {
      const query = r.db('mydb').table('users').orderBy('name').count();

      expect(query._query.count).toBeDefined();
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
          fields: [
            {
              fieldName: 'name',
              direction: SortDirection.ASC
            }
          ]
        }
      });
    });

    it('should work in complex query chains', () => {
      const query = r
        .db('mydb')
        .table('posts')
        .filter({ published: true })
        .orderBy({ field: 'created_at', direction: 'desc' })
        .limit(20);

      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.source).toEqual({
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
          fields: [
            {
              fieldName: 'created_at',
              direction: SortDirection.DESC
            }
          ]
        }
      });
    });
  });

  describe('orderBy() with nested field names', () => {
    it('should handle simple nested field names', () => {
      const query = r.db('mydb').table('users').orderBy('profile.name');

      expect(query._query.orderBy!.fields[0]).toEqual({
        fieldName: 'profile.name',
        direction: SortDirection.ASC
      });
    });

    it('should handle deeply nested field names', () => {
      const query = r.db('mydb').table('users').orderBy('settings.notifications.email');

      expect(query._query.orderBy!.fields[0]).toEqual({
        fieldName: 'settings.notifications.email',
        direction: SortDirection.ASC
      });
    });

    it('should handle array index field names', () => {
      const query = r.db('mydb').table('posts').orderBy('tags.0');

      expect(query._query.orderBy!.fields[0]).toEqual({
        fieldName: 'tags.0',
        direction: SortDirection.ASC
      });
    });

    it('should handle mixed nested and regular fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .orderBy('department', 'profile.name', { field: 'settings.theme', direction: 'desc' });

      expect(query._query.orderBy!.fields).toHaveLength(3);
      expect(query._query.orderBy!.fields[1]).toEqual({
        fieldName: 'profile.name',
        direction: SortDirection.ASC
      });
      expect(query._query.orderBy!.fields[2]).toEqual({
        fieldName: 'settings.theme',
        direction: SortDirection.DESC
      });
    });
  });

  describe('orderBy() with typed interfaces', () => {
    interface User {
      id: string;
      name: string;
      email: string;
      age: number;
      department: string;
      created_at: string;
    }

    it('should work with typed table queries', () => {
      const query = r.db('mydb').table<User>('users').orderBy('name');

      expect(query._query.orderBy).toBeDefined();
      expect(query._query.orderBy!.fields[0]).toEqual({
        fieldName: 'name',
        direction: SortDirection.ASC
      });
    });

    it('should work with multiple typed fields', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .orderBy('department', { field: 'age', direction: 'desc' }, 'name');

      expect(query._query.orderBy!.fields).toHaveLength(3);
      expect(query._query.orderBy!.fields[0].fieldName).toBe('department');
      expect(query._query.orderBy!.fields[1].fieldName).toBe('age');
      expect(query._query.orderBy!.fields[2].fieldName).toBe('name');
    });
  });

  describe('orderBy() with pagination', () => {
    it('should handle orderBy queries with cursor options', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice', age: 25 },
          { id: 'user2', name: 'Bob', age: 30 }
        ],
        cursor: { startKey: 'user2', batchSize: 25 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').orderBy('name');
      const result = await query.run(mockConnection, { batchSize: 25, startKey: 'start' });

      expect(mockConnection.query).toHaveBeenCalledWith({
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
            {
              fieldName: 'name',
              direction: SortDirection.ASC
            }
          ]
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

    it('should handle large result sets with ordering', async () => {
      const mockResult = {
        items: new Array(100).fill(0).map((_, i) => ({
          id: `user${i}`,
          name: `User ${i}`,
          age: 20 + (i % 40)
        })),
        cursor: { startKey: 'user99', batchSize: 100 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').orderBy({ field: 'age', direction: 'desc' });
      const result = await query.run(mockConnection, { batchSize: 100 });

      expect(result).toBeDefined();
    });
  });

  describe('orderBy() error handling', () => {
    it('should handle field not found errors', async () => {
      const error = new Error('Field not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('users').orderBy('nonexistent_field');

      await expect(query.run(mockConnection)).rejects.toThrow('Field not found');
    });

    it('should handle table not found errors', async () => {
      const error = new Error('Table not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('nonexistent').orderBy('name');

      await expect(query.run(mockConnection)).rejects.toThrow('Table not found');
    });

    it('should handle sort on non-comparable types', async () => {
      const error = new Error('Cannot sort on field type');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('data').orderBy('binary_field');

      await expect(query.run(mockConnection)).rejects.toThrow('Cannot sort on field type');
    });
  });

  describe('orderBy() with query options', () => {
    it('should pass query options through orderBy operations', async () => {
      const mockResult = {
        items: [{ id: 'user1', name: 'Alice' }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').orderBy('name');
      await query.run(mockConnection, { timeout: 5000, explain: true });

      expect(mockConnection.query).toHaveBeenCalledWith({
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
            {
              fieldName: 'name',
              direction: SortDirection.ASC
            }
          ]
        },
        options: {
          timeoutMs: 5000,
          explain: true
        }
      });
    });
  });

  describe('orderBy() with default database', () => {
    it('should work with default database', () => {
      const query = r.db().table('users').orderBy('name');

      expect(query._query.orderBy!.source).toEqual({
        table: {
          table: {
            database: { name: 'default' },
            name: 'users'
          }
        }
      });
    });
  });

  describe('orderBy() performance considerations', () => {
    it('should work efficiently with indexed fields', () => {
      const query = r.db('mydb').table('users').orderBy('id');

      expect(query._query.orderBy).toBeDefined();
      expect(query._query.orderBy!.fields[0].fieldName).toBe('id');
    });

    it('should handle complex multi-field sorts', () => {
      const query = r
        .db('mydb')
        .table('events')
        .orderBy({ field: 'timestamp', direction: 'desc' }, 'priority', 'user_id');

      expect(query._query.orderBy!.fields).toHaveLength(3);
      expect(query._query.orderBy!.fields[0]).toEqual({
        fieldName: 'timestamp',
        direction: SortDirection.DESC
      });
      expect(query._query.orderBy!.fields[1]).toEqual({
        fieldName: 'priority',
        direction: SortDirection.ASC
      });
      expect(query._query.orderBy!.fields[2]).toEqual({
        fieldName: 'user_id',
        direction: SortDirection.ASC
      });
    });

    it('should work well with limit for top-N queries', () => {
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
          fields: [
            {
              fieldName: 'view_count',
              direction: SortDirection.DESC
            }
          ]
        }
      });
    });
  });

  describe('orderBy() edge cases', () => {
    it('should handle empty field name gracefully', () => {
      const query = r.db('mydb').table('users').orderBy('');

      expect(query._query.orderBy!.fields[0]).toEqual({
        fieldName: '',
        direction: SortDirection.ASC
      });
    });

    it('should handle field names with special characters', () => {
      const query = r.db('mydb').table('data').orderBy('field-with-dashes');

      expect(query._query.orderBy!.fields[0]).toEqual({
        fieldName: 'field-with-dashes',
        direction: SortDirection.ASC
      });
    });

    it('should handle numeric field names', () => {
      const query = r.db('mydb').table('matrix').orderBy('0');

      expect(query._query.orderBy!.fields[0]).toEqual({
        fieldName: '0',
        direction: SortDirection.ASC
      });
    });

    it('should handle very long field names', () => {
      const longFieldName = 'a'.repeat(1000);
      const query = r.db('mydb').table('data').orderBy(longFieldName);

      expect(query._query.orderBy!.fields[0]).toEqual({
        fieldName: longFieldName,
        direction: SortDirection.ASC
      });
    });
  });
});
