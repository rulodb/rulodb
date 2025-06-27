import { Connection } from '../src/connection';
import { r, RQuery } from '../src/query';

// Mock the Connection class
jest.mock('../src/connection');

describe('Pluck Operations', () => {
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

  describe('pluck() basic functionality', () => {
    it('should create pluck query with single field', () => {
      const query = r.db('mydb').table('users').pluck('name');

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.pluck).toBeDefined();
      expect(query._query.pluck!.fields).toEqual([{ path: ['name'], separator: '.' }]);
      expect(query._query.pluck!.source).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
    });

    it('should create pluck query with multiple fields', () => {
      const query = r.db('mydb').table('users').pluck('name', 'email', 'age');

      expect(query._query.pluck).toBeDefined();
      expect(query._query.pluck!.fields).toEqual([
        { path: ['name'], separator: '.' },
        { path: ['email'], separator: '.' },
        { path: ['age'], separator: '.' }
      ]);
      expect(query._query.pluck!.source).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
    });

    it('should execute pluck query successfully', async () => {
      const mockResult = {
        items: [
          { name: 'Alice', email: 'alice@example.com' },
          { name: 'Bob', email: 'bob@example.com' }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').pluck('name', 'email');
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
        pluck: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          fields: [
            { path: ['name'], separator: '.' },
            { path: ['email'], separator: '.' }
          ]
        }
      });
      expect(result).toBeDefined();
    });

    it('should require at least one field', () => {
      // Empty pluck should not be allowed - this would cause TypeScript error
      // const query = r.db('mydb').table('users').pluck();

      // Instead, pluck requires at least one field
      const query = r.db('mydb').table('users').pluck('name');
      expect(query._query.pluck).toBeDefined();
      expect(query._query.pluck!.fields).toEqual([{ path: ['name'], separator: '.' }]);
    });
  });

  describe('pluck() chaining from different sources', () => {
    it('should chain pluck from table scan', () => {
      const query = r.db('mydb').table('users').pluck('name');

      expect(query._query.pluck!.source).toEqual({
        table: {
          table: {
            database: { name: 'mydb' },
            name: 'users'
          }
        }
      });
    });

    it('should chain pluck from getAll', () => {
      const query = r.db('mydb').table('users').getAll('user1', 'user2').pluck('name', 'email');

      expect(query._query.pluck!.source).toEqual({
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

    it('should chain pluck from filter', () => {
      const query = r.db('mydb').table('users').filter({ active: true }).pluck('name', 'email');

      expect(query._query.pluck!.source).toEqual({
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

    it('should chain pluck from orderBy', () => {
      const query = r.db('mydb').table('users').orderBy('name').pluck('name', 'email');

      expect(query._query.pluck!.source).toEqual({
        orderBy: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          fields: [{ fieldName: 'name', direction: 0 }]
        }
      });
    });
  });

  describe('pluck() chaining to other operations', () => {
    it('should allow chaining filter after pluck', () => {
      const query = r.db('mydb').table('users').pluck('name', 'active').filter({ active: true });

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.source).toEqual({
        pluck: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          fields: [
            { path: ['name'], separator: '.' },
            { path: ['active'], separator: '.' }
          ]
        }
      });
    });

    it('should allow chaining orderBy after pluck', () => {
      const query = r.db('mydb').table('users').pluck('name', 'age').orderBy('name');

      expect(query._query.orderBy).toBeDefined();
      expect(query._query.orderBy!.source).toEqual({
        pluck: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          fields: [
            { path: ['name'], separator: '.' },
            { path: ['age'], separator: '.' }
          ]
        }
      });
    });

    it('should allow chaining limit after pluck', () => {
      const query = r.db('mydb').table('users').pluck('name', 'email').limit(10);

      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.count).toBe(10);
      expect(query._query.limit!.source).toEqual({
        pluck: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          fields: [
            { path: ['name'], separator: '.' },
            { path: ['email'], separator: '.' }
          ]
        }
      });
    });

    it('should allow chaining skip after pluck', () => {
      const query = r.db('mydb').table('users').pluck('name', 'email').skip(5);

      expect(query._query.skip).toBeDefined();
      expect(query._query.skip!.count).toBe(5);
      expect(query._query.skip!.source).toEqual({
        pluck: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          fields: [
            { path: ['name'], separator: '.' },
            { path: ['email'], separator: '.' }
          ]
        }
      });
    });

    it('should allow chaining count after pluck', () => {
      const query = r.db('mydb').table('users').pluck('name').count();

      expect(query._query.count).toBeDefined();
      expect(query._query.count!.source).toEqual({
        pluck: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          fields: [{ path: ['name'], separator: '.' }]
        }
      });
    });
  });

  describe('pluck() with complex queries', () => {
    it('should work in complex query chains', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter({ active: true })
        .orderBy('name')
        .pluck('name', 'email', 'created_at')
        .limit(50);

      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.source).toEqual({
        pluck: {
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
              fields: [{ fieldName: 'name', direction: 0 }]
            }
          },
          fields: [
            { path: ['name'], separator: '.' },
            { path: ['email'], separator: '.' },
            { path: ['created_at'], separator: '.' }
          ]
        }
      });
    });

    it('should handle nested field names', () => {
      const query = r.db('mydb').table('users').pluck('profile.name', 'settings.theme');

      expect(query._query.pluck!.fields).toEqual([
        { path: ['profile', 'name'], separator: '.' },
        { path: ['settings', 'theme'], separator: '.' }
      ]);
    });
  });

  describe('pluck() with pagination', () => {
    it('should handle pluck queries with cursor options', async () => {
      const mockResult = {
        items: [
          { name: 'Alice', email: 'alice@example.com' },
          { name: 'Bob', email: 'bob@example.com' }
        ],
        cursor: { startKey: 'user_bob', batchSize: 25 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').pluck('name', 'email');
      const result = await query.run(mockConnection, { batchSize: 25, startKey: 'start' });

      expect(mockConnection.query).toHaveBeenCalledWith({
        pluck: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          fields: [
            { path: ['name'], separator: '.' },
            { path: ['email'], separator: '.' }
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

    it('should handle large result sets with pluck', async () => {
      const mockResult = {
        items: new Array(50).fill(0).map((_, i) => ({
          name: `User ${i}`,
          email: `user${i}@example.com`
        })),
        cursor: { startKey: 'user49', batchSize: 50 },
        options: {
          explain: false,
          timeoutMs: 0
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').pluck('name', 'email');
      const result = await query.run(mockConnection, { batchSize: 50 });

      expect(result).toBeDefined();
    });
  });

  describe('pluck() with typed interfaces', () => {
    interface User {
      id: string;
      name: string;
      email: string;
      age: number;
      active: boolean;
      profile: {
        bio: string;
        avatar: string;
      };
    }

    interface UserProfile {
      name: string;
      email: string;
    }

    it('should work with typed table queries', () => {
      const query = r.db('mydb').table<User>('users').pluck('name', 'email');

      expect(query).toBeInstanceOf(RQuery);
      expect(query._dbName).toBe('mydb');
    });

    it('should maintain type information through pluck', () => {
      const query = r
        .db('test')
        .table<User>('users')
        .filter({ active: true })
        .pluck('name', 'email', 'age');

      expect(query).toBeInstanceOf(RQuery);
      expect(query._dbName).toBe('test');
    });

    it('should support type override with explicit result type', () => {
      const query = r.db('mydb').table<User>('users').pluck<UserProfile>('name', 'email');

      expect(query).toBeInstanceOf(RQuery);
      expect(query._dbName).toBe('mydb');
      expect(query._query.pluck!.fields).toEqual([
        { path: ['name'], separator: '.' },
        { path: ['email'], separator: '.' }
      ]);
    });

    it('should support type override with Omit utility type', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .pluck<Omit<User, 'id' | 'active'>>('name', 'email', 'age', 'profile');

      expect(query).toBeInstanceOf(RQuery);
      expect(query._dbName).toBe('mydb');
      expect(query._query.pluck!.fields).toEqual([
        { path: ['name'], separator: '.' },
        { path: ['email'], separator: '.' },
        { path: ['age'], separator: '.' },
        { path: ['profile'], separator: '.' }
      ]);
    });

    it('should support type override with Pick utility type', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .pluck<Pick<User, 'name' | 'email'>>('name', 'email');

      expect(query).toBeInstanceOf(RQuery);
      expect(query._dbName).toBe('mydb');
      expect(query._query.pluck!.fields).toEqual([
        { path: ['name'], separator: '.' },
        { path: ['email'], separator: '.' }
      ]);
    });

    it('should support type override with array syntax', () => {
      const query = r.db('mydb').table<User>('users').pluck<UserProfile>(['name', 'email']);

      expect(query).toBeInstanceOf(RQuery);
      expect(query._dbName).toBe('mydb');
      expect(query._query.pluck!.fields).toEqual([
        { path: ['name'], separator: '.' },
        { path: ['email'], separator: '.' }
      ]);
    });
  });

  describe('pluck() error handling', () => {
    it('should handle pluck errors', async () => {
      const error = new Error('Field not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('users').pluck('nonexistent_field');

      await expect(query.run(mockConnection)).rejects.toThrow('Field not found');
    });

    it('should handle table not found errors in pluck', async () => {
      const error = new Error('Table not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('nonexistent').pluck('name');

      await expect(query.run(mockConnection)).rejects.toThrow('Table not found');
    });

    it('should handle database not found errors in pluck', async () => {
      const error = new Error('Database not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('nonexistent').table('users').pluck('name');

      await expect(query.run(mockConnection)).rejects.toThrow('Database not found');
    });

    it('should handle connection errors during pluck execution', async () => {
      const error = new Error('Connection failed');
      mockConnection.query.mockRejectedValue(error);

      const query = r.db('mydb').table('users').pluck('name', 'email');

      await expect(query.run(mockConnection)).rejects.toThrow('Connection failed');
    });
  });

  describe('pluck() with query options', () => {
    it('should pass query options through pluck operations', async () => {
      const mockResult = {
        items: [{ name: 'Alice' }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').pluck('name');
      await query.run(mockConnection, { timeout: 5000, explain: true });

      expect(mockConnection.query).toHaveBeenCalledWith({
        pluck: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          fields: [{ path: ['name'], separator: '.' }]
        },
        options: {
          timeoutMs: 5000,
          explain: true
        }
      });
    });

    it('should handle explain option with pluck', async () => {
      const mockResult = {
        items: [],
        cursor: undefined,
        explain: {
          query: 'pluck operation',
          executionTime: 15,
          scannedRows: 100,
          returnedRows: 100
        }
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').pluck('name', 'email');
      const result = await query.run(mockConnection, { explain: true });

      expect(mockConnection.query).toHaveBeenCalledWith({
        pluck: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          fields: [
            { path: ['name'], separator: '.' },
            { path: ['email'], separator: '.' }
          ]
        },
        options: {
          explain: true,
          timeoutMs: 0
        }
      });
      expect(result).toBeDefined();
    });
  });

  describe('pluck() with default database', () => {
    it('should work with default database', () => {
      const query = r.db().table('users').pluck('name');

      expect(query._query.pluck!.source).toEqual({
        table: {
          table: {
            database: { name: 'default' },
            name: 'users'
          }
        }
      });
    });
  });

  describe('pluck() edge cases', () => {
    it('should handle duplicate field names', () => {
      const query = r.db('mydb').table('users').pluck('name', 'name', 'email');

      expect(query._query.pluck!.fields).toEqual([
        { path: ['name'], separator: '.' },
        { path: ['name'], separator: '.' },
        { path: ['email'], separator: '.' }
      ]);
    });

    it('should handle empty table with pluck', async () => {
      const mockResult = {
        items: [],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').pluck('name');
      const result = await query.run(mockConnection);

      expect(result).toBeDefined();
    });

    it('should handle very long field names', () => {
      const longFieldName = 'a'.repeat(1000);
      const query = r.db('mydb').table('users').pluck(longFieldName);

      expect(query._query.pluck!.fields).toEqual([{ path: [longFieldName], separator: '.' }]);
    });

    it('should handle special characters in field names', () => {
      const query = r
        .db('mydb')
        .table('users')
        .pluck('field-with-dashes', 'field_with_underscores', 'field.with.dots');

      expect(query._query.pluck!.fields).toEqual([
        { path: ['field-with-dashes'], separator: '.' },
        { path: ['field_with_underscores'], separator: '.' },
        { path: ['field', 'with', 'dots'], separator: '.' }
      ]);
    });
  });

  describe('pluck() performance scenarios', () => {
    it('should handle pluck with large number of fields', () => {
      const fields = Array.from({ length: 100 }, (_, i) => `field${i}`);
      const query = r.db('mydb').table('users').pluck(fields);

      expect(query._query.pluck!.fields).toEqual(
        fields.map((field) => ({ path: [field], separator: '.' }))
      );
    });

    it('should work with pluck on indexed fields', async () => {
      const mockResult = {
        items: [
          { id: 'user1', email: 'alice@example.com' },
          { id: 'user2', email: 'bob@example.com' }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r.db('mydb').table('users').pluck('id', 'email');
      const result = await query.run(mockConnection);

      expect(result).toBeDefined();
    });
  });

  describe('pluck() with multiple pluck operations', () => {
    it('should handle multiple pluck operations (last wins)', () => {
      const query = r.db('mydb').table('users').pluck('name', 'email', 'age').pluck('name');

      expect(query._query.pluck).toBeDefined();
      expect(query._query.pluck!.fields).toEqual([{ path: ['name'], separator: '.' }]);
      expect(query._query.pluck!.source).toEqual({
        pluck: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'users'
              }
            }
          },
          fields: [
            { path: ['name'], separator: '.' },
            { path: ['email'], separator: '.' },
            { path: ['age'], separator: '.' }
          ]
        }
      });
    });
  });

  describe('pluck() with custom separator', () => {
    it('should support custom separator with array syntax', () => {
      const query = r
        .db('mydb')
        .table('users')
        .pluck(['name', 'profile->bio'], { separator: '->' });

      expect(query._query.pluck!.fields).toEqual([
        { path: ['name'], separator: '->' },
        { path: ['profile', 'bio'], separator: '->' }
      ]);
    });

    it('should support custom separator with array syntax (pipe)', () => {
      const query = r.db('mydb').table('users').pluck(['name', 'profile|bio'], { separator: '|' });

      expect(query._query.pluck!.fields).toEqual([
        { path: ['name'], separator: '|' },
        { path: ['profile', 'bio'], separator: '|' }
      ]);
    });

    it('should default to dot separator when not specified', () => {
      const query = r.db('mydb').table('users').pluck('name', 'profile.bio');

      expect(query._query.pluck!.fields).toEqual([
        { path: ['name'], separator: '.' },
        { path: ['profile', 'bio'], separator: '.' }
      ]);
    });

    it('should handle complex nested paths with custom separator', () => {
      const query = r
        .db('mydb')
        .table('users')
        .pluck(['user::profile::settings::theme', 'user::metadata::created'], {
          separator: '::'
        });

      expect(query._query.pluck!.fields).toEqual([
        { path: ['user', 'profile', 'settings', 'theme'], separator: '::' },
        { path: ['user', 'metadata', 'created'], separator: '::' }
      ]);
    });

    it('should handle single field with custom separator', () => {
      const query = r.db('mydb').table('users').pluck(['name'], { separator: '|' });

      expect(query._query.pluck!.fields).toEqual([{ path: ['name'], separator: '|' }]);
    });

    it('should ignore separator when field has no nested parts', () => {
      const query = r.db('mydb').table('users').pluck(['name', 'email'], { separator: '::' });

      expect(query._query.pluck!.fields).toEqual([
        { path: ['name'], separator: '::' },
        { path: ['email'], separator: '::' }
      ]);
    });
  });
});
