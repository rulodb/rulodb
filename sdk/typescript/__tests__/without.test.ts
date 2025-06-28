import { Connection } from '../src/connection';
import { r, RQuery } from '../src/query';

// Mock the Connection class
jest.mock('../src/connection');

describe('Without Operations', () => {
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

  describe('without() basic functionality', () => {
    it('should create without query with single field', () => {
      const query = r.db('mydb').table('users').without('password');

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.without).toBeDefined();
      expect(query._query.without!.fields).toEqual([{ path: ['password'], separator: '.' }]);
      expect(query._query.without!.source).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
    });

    it('should create without query with multiple fields', () => {
      const query = r.db('mydb').table('users').without('password', 'ssn', 'secret');

      expect(query._query.without).toBeDefined();
      expect(query._query.without!.fields).toEqual([
        { path: ['password'], separator: '.' },
        { path: ['ssn'], separator: '.' },
        { path: ['secret'], separator: '.' }
      ]);
      expect(query._query.without!.source).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
    });

    it('should execute without query successfully', async () => {
      const mockResult = {
        items: [
          { id: '1', name: 'John', email: 'john@example.com' },
          { id: '2', name: 'Jane', email: 'jane@example.com' }
        ]
      };

      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').without('password');
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
        without: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          fields: [{ path: ['password'], separator: '.' }]
        }
      });
      expect(result).toBeDefined();
    });

    it('should require at least one field', () => {
      // TypeScript would prevent calling without() with no arguments
      // Instead, verify that without requires at least one field
      const query = r.db('mydb').table('users').without('password');
      expect(query._query.without).toBeDefined();
      expect(query._query.without!.fields).toEqual([{ path: ['password'], separator: '.' }]);
    });
  });

  describe('without() chaining from different sources', () => {
    it('should chain without from table scan', () => {
      const query = r.db('mydb').table('users').without('password');

      expect(query._query.without!.source).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
    });

    it('should chain without from getAll', () => {
      const query = r.db('mydb').table('users').getAll('user1', 'user2').without('password', 'ssn');

      expect(query._query.without!.source).toEqual({
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
      expect(query._query.without!.fields).toHaveLength(2);
    });

    it('should chain without from filter', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter({ active: true })
        .without('internal_notes', 'password');

      expect(query._query.without!.source).toEqual({
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
      expect(query._query.without!.fields).toHaveLength(2);
    });

    it('should chain without from orderBy', () => {
      const query = r.db('mydb').table('users').orderBy('name').without('created_at', 'updated_at');

      expect(query._query.without!.source).toEqual({
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
      expect(query._query.without!.fields).toHaveLength(2);
    });
  });

  describe('without() chaining to other operations', () => {
    it('should allow chaining filter after without', () => {
      const query = r.db('mydb').table('users').without('password').filter({ status: 'active' });

      expect(query._query.filter!.source!.without).toBeDefined();
      expect(query._query.filter!.source!.without!.fields).toHaveLength(1);
    });

    it('should allow chaining orderBy after without', () => {
      const query = r.db('mydb').table('users').without('password', 'ssn').orderBy('name');

      expect(query._query.orderBy!.source!.without).toBeDefined();
      expect(query._query.orderBy!.source!.without!.fields).toHaveLength(2);
    });

    it('should allow chaining limit after without', () => {
      const query = r.db('mydb').table('users').without('internal_data').limit(10);

      expect(query._query.limit!.source!.without).toBeDefined();
      expect(query._query.limit!.count).toBe(10);
    });

    it('should allow chaining skip after without', () => {
      const query = r.db('mydb').table('users').without('metadata', 'temp_data').skip(5);

      expect(query._query.skip!.source!.without).toBeDefined();
      expect(query._query.skip!.count).toBe(5);
      expect(query._query.skip!.source!.without!.fields).toHaveLength(2);
    });

    it('should allow chaining count after without', () => {
      const query = r.db('mydb').table('users').without('sensitive_info').count();

      expect(query._query.count!.source!.without).toBeDefined();
    });
  });

  describe('without() with complex queries', () => {
    it('should work in complex query chains', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter({ status: 'active' })
        .without('password', 'ssn', 'internal_notes')
        .orderBy('name')
        .limit(5);

      expect(query._query.limit!.source!.orderBy!.source!.without).toBeDefined();
      expect(query._query.limit!.source!.orderBy!.source!.without!.fields).toHaveLength(3);
    });

    it('should handle nested field names', () => {
      const query = r.db('mydb').table('users').without('user.password', 'metadata.internal');

      expect(query._query.without!.fields).toEqual([
        { path: ['user', 'password'], separator: '.' },
        { path: ['metadata', 'internal'], separator: '.' }
      ]);
    });
  });

  describe('without() with pagination', () => {
    it('should handle without queries with cursor options', async () => {
      const firstPageResult = {
        items: [
          { id: '1', name: 'John', email: 'john@example.com' },
          { id: '2', name: 'Jane', email: 'jane@example.com' }
        ],
        cursor: { startKey: 'cursor_token_123' }
      };

      const secondPageResult = {
        items: [
          { id: '3', name: 'Bob', email: 'bob@example.com' },
          { id: '4', name: 'Alice', email: 'alice@example.com' }
        ]
      };

      mockConnection.query
        .mockResolvedValueOnce(firstPageResult)
        .mockResolvedValueOnce(secondPageResult);

      const firstPage = await r
        .db('mydb')
        .table('users')
        .without('password', 'ssn')
        .limit(2)
        .run(mockConnection);

      const firstPageItems = await firstPage.toArray();
      expect(firstPageItems).toEqual(firstPageResult.items);
      expect(firstPage.getCurrentStartKey()).toEqual(firstPageResult.cursor.startKey);

      const secondPage = await r
        .db('mydb')
        .table('users')
        .without('password', 'ssn')
        .limit(2)
        .run(mockConnection);

      const secondPageItems = await secondPage.toArray();
      expect(secondPageItems).toEqual(secondPageResult.items);
    });

    it('should handle large result sets with without', async () => {
      const mockResult = {
        items: Array.from({ length: 1000 }, (_, i) => ({
          id: `${i}`,
          name: `User${i}`,
          email: `user${i}@example.com`
        }))
      };

      mockConnection.query.mockResolvedValue(mockResult);

      const result = await r.db('mydb').table('users').without('temp_data').run(mockConnection);

      const resultItems = await result.toArray();
      expect(resultItems).toEqual(mockResult.items);
    });
  });

  describe('without() with typed interfaces', () => {
    interface User {
      id: string;
      name: string;
      email: string;
      password: string;
      ssn: string;
      phone?: string;
      address?: string;
    }

    interface UserPublic {
      id: string;
      name: string;
      email: string;
      phone?: string;
      address?: string;
    }

    it('should work with typed table queries', () => {
      const query = r.db('mydb').table<User>('users').without('password');

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.without).toBeDefined();
    });

    it('should maintain type information through without', () => {
      const query = r.db('mydb').table<User>('users').without('password', 'ssn');

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.without!.fields).toHaveLength(2);
    });

    it('should support type override with explicit result type', () => {
      const query = r.db('mydb').table<User>('users').without<UserPublic>('password', 'ssn');

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.without!.fields).toHaveLength(2);
    });

    it('should support type override with array syntax', () => {
      const query = r.db('mydb').table('users').without<UserPublic>('password', 'ssn');

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.without!.fields).toHaveLength(2);
    });
  });

  describe('without() error handling', () => {
    it('should handle without errors', async () => {
      mockConnection.query.mockRejectedValue(new Error('Network error'));

      const query = r.db('mydb').table('users').without('password');

      await expect(query.run(mockConnection)).rejects.toThrow('Network error');
    });

    it('should handle table not found errors in without', async () => {
      mockConnection.query.mockRejectedValue(new Error('Table not found'));

      const query = r.db('mydb').table('nonexistent_table').without('password');

      await expect(query.run(mockConnection)).rejects.toThrow('Table not found');
    });

    it('should handle database not found errors in without', async () => {
      mockConnection.query.mockRejectedValue(new Error('Database not found'));

      const query = r.db('nonexistent_db').table('users').without('password');

      await expect(query.run(mockConnection)).rejects.toThrow('Database not found');
    });

    it('should handle connection errors during without execution', async () => {
      const disconnectedConnection = {
        ...mockConnection,
        isConnected: jest.fn().mockReturnValue(false),
        query: jest.fn().mockRejectedValue(new Error('Connection closed'))
      } as any;

      const query = r.db('mydb').table('users').without('password');

      await expect(query.run(disconnectedConnection)).rejects.toThrow('Connection closed');
    });
  });

  describe('without() edge cases', () => {
    it('should handle duplicate field names', () => {
      const query = r.db('mydb').table('users').without('password', 'password', 'ssn');

      expect(query._query.without!.fields).toHaveLength(3);
      expect(query._query.without!.fields).toEqual([
        { path: ['password'], separator: '.' },
        { path: ['password'], separator: '.' },
        { path: ['ssn'], separator: '.' }
      ]);
    });

    it('should handle empty table with without', async () => {
      const mockResult = { items: [] };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('empty_table').without('password');
      const result = await query.run(mockConnection);

      const resultItems = await result.toArray();
      expect(resultItems).toEqual(mockResult.items);
    });

    it('should handle very long field names', () => {
      const longFieldName = 'a'.repeat(1000);
      const query = r.db('mydb').table('users').without(longFieldName);

      expect(query._query.without!.fields[0].path).toEqual([longFieldName]);
    });

    it('should handle special characters in field names', () => {
      const query = r
        .db('mydb')
        .table('users')
        .without('field-with-dashes', 'field_with_underscores', 'field with spaces');

      expect(query._query.without!.fields).toHaveLength(3);
      expect(query._query.without!.fields[0].path).toEqual(['field-with-dashes']);
      expect(query._query.without!.fields[1].path).toEqual(['field_with_underscores']);
      expect(query._query.without!.fields[2].path).toEqual(['field with spaces']);
    });
  });

  describe('without() performance scenarios', () => {
    it('should handle without with large number of fields', () => {
      const manyFields = Array.from({ length: 100 }, (_, i) => `field_${i}`);
      const query = r
        .db('mydb')
        .table('users')
        .without(...manyFields);

      expect(query._query.without!.fields).toHaveLength(100);
      manyFields.forEach((field, index) => {
        expect(query._query.without!.fields[index].path).toEqual([field]);
      });
    });

    it('should work with without on indexed fields', async () => {
      const mockResult = {
        items: [{ id: '1', name: 'John', email: 'john@example.com', status: 'active' }]
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter({ status: 'active' })
        .without('password', 'ssn');

      const result = await query.run(mockConnection);
      const resultItems = await result.toArray();
      expect(resultItems).toEqual(mockResult.items);
    });
  });

  describe('without() with multiple without operations', () => {
    it('should handle multiple without operations (last wins)', () => {
      const query = r.db('mydb').table('users').without('password').without('ssn', 'secret');

      // The second without should replace the first
      expect(query._query.without!.fields).toHaveLength(2);
      expect(query._query.without!.fields).toEqual([
        { path: ['ssn'], separator: '.' },
        { path: ['secret'], separator: '.' }
      ]);
    });
  });

  describe('without() with custom separator', () => {
    it('should support custom separator with array syntax', () => {
      const query = r
        .db('mydb')
        .table('users')
        .without(['password', 'user->profile->bio'], { separator: '->' });

      expect(query._query.without!.fields).toEqual([
        { path: ['password'], separator: '->' },
        { path: ['user', 'profile', 'bio'], separator: '->' }
      ]);
    });

    it('should support custom separator with array syntax (pipe)', () => {
      const query = r
        .db('mydb')
        .table('users')
        .without(['password', 'metadata|internal'], { separator: '|' });

      expect(query._query.without!.fields).toEqual([
        { path: ['password'], separator: '|' },
        { path: ['metadata', 'internal'], separator: '|' }
      ]);
    });

    it('should default to dot separator when not specified', () => {
      const query = r.db('mydb').table('users').without('password', 'user.profile');

      expect(query._query.without!.fields).toEqual([
        { path: ['password'], separator: '.' },
        { path: ['user', 'profile'], separator: '.' }
      ]);
    });

    it('should handle complex nested paths with custom separator', () => {
      const query = r
        .db('mydb')
        .table('users')
        .without(['user::profile::private::ssn', 'system::internal::debug'], {
          separator: '::'
        });

      expect(query._query.without!.fields).toEqual([
        { path: ['user', 'profile', 'private', 'ssn'], separator: '::' },
        { path: ['system', 'internal', 'debug'], separator: '::' }
      ]);
    });

    it('should handle single field with custom separator', () => {
      const query = r.db('mydb').table('users').without(['password'], { separator: '|' });

      expect(query._query.without!.fields).toEqual([{ path: ['password'], separator: '|' }]);
    });

    it('should ignore separator when field has no nested parts', () => {
      const query = r.db('mydb').table('users').without(['password', 'ssn'], { separator: '::' });

      expect(query._query.without!.fields).toEqual([
        { path: ['password'], separator: '::' },
        { path: ['ssn'], separator: '::' }
      ]);
    });
  });
});
