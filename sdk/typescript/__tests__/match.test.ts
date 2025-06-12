import { Connection } from '../src/connection';
import { r, RQuery } from '../src/query';

// Mock the Connection class
jest.mock('../src/connection');
const MockedConnection = Connection as jest.MockedClass<typeof Connection>;

describe('Pattern Matching (match) Operations', () => {
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

  describe('match() with basic patterns', () => {
    it('should create pattern match with simple string pattern', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('email').match('@example\\.com$'));

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle exact string matching', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').match('^John$'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle case-insensitive patterns', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').match('(?i)john'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle wildcard patterns', () => {
      const query = r
        .db('mydb')
        .table('files')
        .filter((row) => row.field('filename').match('.*\\.txt$'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute pattern match query successfully', async () => {
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
        .filter((row) => row.field('email').match('@example\\.com$'));
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
                match: expect.objectContaining({
                  pattern: '@example\\.com$',
                  flags: '',
                  value: expect.objectContaining({
                    subquery: expect.objectContaining({
                      expression: expect.objectContaining({
                        field: expect.objectContaining({
                          path: ['email'],
                          separator: '.'
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

  describe('match() with email patterns', () => {
    it('should match valid email addresses', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('email').match('^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should match specific email domains', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('email').match('@(gmail|yahoo|hotmail)\\.com$'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should match corporate email patterns', () => {
      const query = r
        .db('mydb')
        .table('employees')
        .filter((row) => row.field('work_email').match('@company\\.(com|org)$'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute email pattern query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', email: 'john@gmail.com' },
          { id: 'user2', email: 'jane@yahoo.com' }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('email').match('@(gmail|yahoo)\\.com$'));
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

  describe('match() with phone number patterns', () => {
    it('should match US phone number format', () => {
      const query = r
        .db('mydb')
        .table('contacts')
        .filter((row) => row.field('phone').match('^\\+1[0-9]{10}$'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should match international phone formats', () => {
      const query = r
        .db('mydb')
        .table('contacts')
        .filter((row) => row.field('phone').match('^\\+[1-9][0-9]{1,14}$'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should match formatted phone numbers', () => {
      const query = r
        .db('mydb')
        .table('contacts')
        .filter((row) => row.field('phone').match('^\\([0-9]{3}\\) [0-9]{3}-[0-9]{4}$'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('match() with URL patterns', () => {
    it('should match HTTP URLs', () => {
      const query = r
        .db('mydb')
        .table('links')
        .filter((row) => row.field('url').match('^https?://'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should match specific domain URLs', () => {
      const query = r
        .db('mydb')
        .table('bookmarks')
        .filter((row) => row.field('url').match('github\\.com'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should match API endpoint patterns', () => {
      const query = r
        .db('mydb')
        .table('api_logs')
        .filter((row) => row.field('endpoint').match('/api/v[0-9]+/'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('match() with file patterns', () => {
    it('should match file extensions', () => {
      const query = r
        .db('mydb')
        .table('files')
        .filter((row) => row.field('filename').match('\\.(jpg|jpeg|png|gif)$'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should match document files', () => {
      const query = r
        .db('mydb')
        .table('documents')
        .filter((row) => row.field('filename').match('\\.(pdf|doc|docx|txt)$'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should match hidden files', () => {
      const query = r
        .db('mydb')
        .table('files')
        .filter((row) => row.field('filename').match('^\\.'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('match() with logical operations', () => {
    it('should chain match with AND operation', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row.field('email').match('@company\\.com$').and(row.field('name').match('^[A-Z]'))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should chain match with OR operation', () => {
      const query = r
        .db('mydb')
        .table('files')
        .filter((row) =>
          row.field('filename').match('\\.jpg$').or(row.field('filename').match('\\.png$'))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle complex logical combinations', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row
            .field('email')
            .match('@(gmail|yahoo)\\.com$')
            .and(row.field('name').match('^[A-Z][a-z]+'))
            .or(row.field('role').eq('admin'))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute chained match query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'John', email: 'john@company.com' },
          { id: 'user2', name: 'Jane', email: 'jane@company.com' }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row.field('email').match('@company\\.com$').and(row.field('name').match('^[A-Z]'))
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
          predicate: {
            subquery: {
              expression: {
                binary: {
                  left: {
                    subquery: {
                      expression: {
                        match: {
                          value: {
                            subquery: {
                              expression: {
                                field: {
                                  path: ['email'],
                                  separator: '.'
                                }
                              }
                            }
                          },
                          pattern: '@company\\.com$',
                          flags: ''
                        }
                      }
                    }
                  },
                  op: 6,
                  right: {
                    subquery: {
                      expression: {
                        match: {
                          value: {
                            subquery: {
                              expression: {
                                field: {
                                  path: ['name'],
                                  separator: '.'
                                }
                              }
                            }
                          },
                          pattern: '^[A-Z]',
                          flags: ''
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      });
      expect(result).toBeDefined();
    });
  });

  describe('match() with nested field access', () => {
    it('should work with nested object fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('profile.bio').match('engineer|developer'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with deeply nested fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('settings.notifications.email').match('daily|weekly'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with array index access', () => {
      const query = r
        .db('mydb')
        .table('posts')
        .filter((row) => row.field('tags.0').match('tech|programming'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('match() with typed interfaces', () => {
    interface User {
      id: string;
      name: string;
      email: string;
      phone?: string;
      website?: string;
    }

    it('should work with typed row access', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('email').match('@example\\.com$'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with optional typed fields', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('phone').match('^\\+1'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle type-safe pattern matching', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('name').match('^[A-Z][a-z]+$'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('match() error handling', () => {
    it('should handle field not found errors', async () => {
      const error = new Error('Field not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('nonexistent').match('.*'));

      await expect(query.run(mockConnection)).rejects.toThrow('Field not found');
    });

    it('should handle invalid regex pattern errors', async () => {
      const error = new Error('Invalid regex pattern');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('email').match('[invalid'));

      await expect(query.run(mockConnection)).rejects.toThrow('Invalid regex pattern');
    });

    it('should handle table not found errors', async () => {
      const error = new Error('Table not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('nonexistent')
        .filter((row) => row.field('name').match('.*'));

      await expect(query.run(mockConnection)).rejects.toThrow('Table not found');
    });
  });

  describe('match() with pagination', () => {
    it('should handle match queries with cursor options', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'John', email: 'john@example.com' },
          { id: 'user2', name: 'Jane', email: 'jane@example.com' }
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
        .filter((row) => row.field('email').match('@example\\.com$'));
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
          email: `user${i}@example.com`
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
        .filter((row) => row.field('email').match('@example\\.com$'));
      const result = await query.run(mockConnection, { batchSize: 50 });

      expect(result).toBeDefined();
    });
  });

  describe('match() with query options', () => {
    it('should pass query options through match operations', async () => {
      const mockResult = {
        items: [{ id: 'user1', email: 'john@example.com' }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('email').match('@example\\.com$'));
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

  describe('match() with default database', () => {
    it('should work with default database', () => {
      const query = r
        .db()
        .table('users')
        .filter((row) => row.field('email').match('@example\\.com$'));

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

  describe('match() performance considerations', () => {
    it('should work efficiently with indexed fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('email').match('@company\\.com$'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with orderBy after match filtering', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('email').match('@example\\.com$'))
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

    it('should work with limit after match filtering', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('email').match('@example\\.com$'))
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

  describe('match() practical use cases', () => {
    it('should validate email addresses', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('email').match('^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should find social media profiles', () => {
      const query = r
        .db('mydb')
        .table('profiles')
        .filter((row) => row.field('bio').match('(twitter|linkedin|github)\\.com'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should filter log entries by pattern', () => {
      const query = r
        .db('mydb')
        .table('logs')
        .filter((row) => row.field('message').match('ERROR|FATAL'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should find code files by extension', () => {
      const query = r
        .db('mydb')
        .table('files')
        .filter((row) => row.field('path').match('\\.(js|ts|jsx|tsx)$'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should validate phone number formats', () => {
      const query = r
        .db('mydb')
        .table('contacts')
        .filter((row) => row.field('phone').match('^(\\+1|1)?[0-9]{10}$'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });
});
