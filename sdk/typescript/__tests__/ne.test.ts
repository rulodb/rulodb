import { Connection } from '../src/connection';
import { r, RQuery } from '../src/query';
import { BinaryOp_Operator } from '../src/rulo';

// Mock the Connection class
jest.mock('../src/connection');
const MockedConnection = Connection as jest.MockedClass<typeof Connection>;

describe('Not Equal (ne) Operations', () => {
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

  describe('ne() with string values', () => {
    it('should create not equal comparison with string value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').ne('John'));

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle empty string comparison', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').ne(''));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle string with special characters', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('email').ne('user@example.com'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle unicode strings', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').ne('José'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute string ne query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Jane', email: 'jane@example.com' },
          { id: 'user2', name: 'Bob', email: 'bob@example.com' }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').ne('John'));
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
                  op: BinaryOp_Operator.NE,
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
                      string: 'John'
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

  describe('ne() with numeric values', () => {
    it('should create not equal comparison with integer value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').ne(25));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should create not equal comparison with float value', () => {
      const query = r
        .db('mydb')
        .table('products')
        .filter((row) => row.field('price').ne(19.99));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle zero comparison', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('score').ne(0));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle negative number comparison', () => {
      const query = r
        .db('mydb')
        .table('accounts')
        .filter((row) => row.field('balance').ne(-100));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle large number comparison', () => {
      const query = r
        .db('mydb')
        .table('stats')
        .filter((row) => row.field('count').ne(1000000));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute numeric ne query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Jane', age: 30 },
          { id: 'user2', name: 'Bob', age: 22 }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').ne(25));
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
                  op: BinaryOp_Operator.NE,
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
                      int: '25'
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

  describe('ne() with boolean values', () => {
    it('should create not equal comparison with true value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('active').ne(true));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should create not equal comparison with false value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('verified').ne(false));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute boolean ne query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice', active: true },
          { id: 'user2', name: 'Bob', active: true }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('active').ne(false));
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
                  op: BinaryOp_Operator.NE,
                  left: expect.objectContaining({
                    subquery: expect.objectContaining({
                      expression: expect.objectContaining({
                        field: expect.objectContaining({
                          path: ['active'],
                          separator: '.'
                        })
                      })
                    })
                  }),
                  right: expect.objectContaining({
                    literal: expect.objectContaining({
                      bool: false
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

  describe('ne() with null values', () => {
    it('should create not equal comparison with null value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('middle_name').ne(null));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should create not equal comparison with undefined value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('middle_name').ne(undefined));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute null ne query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'John', middle_name: 'William' },
          { id: 'user2', name: 'Jane', middle_name: 'Marie' }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('middle_name').ne(null));
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
                  op: BinaryOp_Operator.NE,
                  left: expect.objectContaining({
                    subquery: expect.objectContaining({
                      expression: expect.objectContaining({
                        field: expect.objectContaining({
                          path: ['middle_name'],
                          separator: '.'
                        })
                      })
                    })
                  }),
                  right: expect.objectContaining({
                    literal: expect.objectContaining({
                      null: 0
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

  describe('ne() with array values', () => {
    it('should create not equal comparison with array value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('tags').ne(['admin', 'user']));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle empty array comparison', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('tags').ne([]));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle mixed type arrays', () => {
      const query = r
        .db('mydb')
        .table('data')
        .filter((row) => row.field('data').ne([1, 'test', true]));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle nested arrays', () => {
      const query = r
        .db('mydb')
        .table('matrix')
        .filter((row) =>
          row.field('values').ne([
            [1, 2],
            [3, 4]
          ])
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute array ne query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'John', tags: ['user'] },
          { id: 'user2', name: 'Jane', tags: ['moderator', 'user'] }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('tags').ne(['admin']));
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
                  op: BinaryOp_Operator.NE,
                  left: expect.objectContaining({
                    subquery: expect.objectContaining({
                      expression: expect.objectContaining({
                        field: expect.objectContaining({
                          path: ['tags'],
                          separator: '.'
                        })
                      })
                    })
                  }),
                  right: expect.objectContaining({
                    literal: expect.objectContaining({
                      array: expect.any(Object)
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

  describe('ne() with object values', () => {
    it('should create not equal comparison with object value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('address').ne({ city: 'New York', country: 'USA' }));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle empty object comparison', () => {
      const query = r
        .db('mydb')
        .table('data')
        .filter((row) => row.field('metadata').ne({}));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle nested object comparison', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row.field('profile').ne({
            personal: {
              name: 'John',
              age: 25
            },
            settings: {
              theme: 'dark'
            }
          })
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle object with array properties', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row.field('config').ne({
            permissions: ['read', 'write'],
            settings: { notifications: true }
          })
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute object ne query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', address: { city: 'Boston', country: 'USA' } },
          { id: 'user2', address: { city: 'Chicago', country: 'USA' } }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('address').ne({ city: 'New York' }));
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
                  op: BinaryOp_Operator.NE,
                  left: expect.objectContaining({
                    subquery: expect.objectContaining({
                      expression: expect.objectContaining({
                        field: expect.objectContaining({
                          path: ['address'],
                          separator: '.'
                        })
                      })
                    })
                  }),
                  right: expect.objectContaining({
                    literal: expect.objectContaining({
                      object: expect.objectContaining({
                        fields: expect.any(Object)
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

  describe('ne() with binary data', () => {
    it('should create not equal comparison with Uint8Array', () => {
      const binaryData = new Uint8Array([1, 2, 3, 4]);
      const query = r
        .db('mydb')
        .table('files')
        .filter((row) => row.field('data').ne(binaryData));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle empty binary data', () => {
      const emptyData = new Uint8Array();
      const query = r
        .db('mydb')
        .table('files')
        .filter((row) => row.field('data').ne(emptyData));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute binary ne query successfully', async () => {
      const binaryData = new Uint8Array([1, 2, 3, 4]);
      const mockResult = {
        items: [
          { id: 'file1', data: new Uint8Array([5, 6, 7, 8]) },
          { id: 'file2', data: new Uint8Array([9, 10, 11, 12]) }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('files')
        .filter((row) => row.field('data').ne(binaryData));
      const result = await query.run(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledWith({
        filter: {
          source: {
            table: {
              table: {
                database: { name: 'mydb' },
                name: 'files'
              }
            }
          },
          predicate: expect.objectContaining({
            subquery: expect.objectContaining({
              expression: expect.objectContaining({
                binary: expect.objectContaining({
                  op: BinaryOp_Operator.NE,
                  left: expect.objectContaining({
                    subquery: expect.objectContaining({
                      expression: expect.objectContaining({
                        field: expect.objectContaining({
                          path: ['data'],
                          separator: '.'
                        })
                      })
                    })
                  }),
                  right: expect.objectContaining({
                    literal: expect.objectContaining({
                      binary: expect.any(Uint8Array)
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

  describe('ne() chaining with logical operations', () => {
    it('should chain ne with AND operation', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').ne('John').and(row.field('age').ne(25)));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should chain ne with OR operation', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').ne('John').or(row.field('age').ne(25)));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle complex logical combinations', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row
            .field('name')
            .ne('John')
            .and(row.field('age').ne(25))
            .or(row.field('active').ne(false))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should chain multiple ne operations', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row
            .field('name')
            .ne('John')
            .and(row.field('name').ne('Jane'))
            .and(row.field('name').ne('Bob'))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute chained ne query successfully', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice', age: 30 },
          { id: 'user2', name: 'Bob', age: 22 }
        ],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').ne('John').and(row.field('age').ne(25)));
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
                          op: BinaryOp_Operator.NE
                        })
                      })
                    })
                  }),
                  right: expect.objectContaining({
                    subquery: expect.objectContaining({
                      expression: expect.objectContaining({
                        binary: expect.objectContaining({
                          op: BinaryOp_Operator.NE
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

  describe('ne() with nested field access', () => {
    it('should work with nested object fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('profile.name').ne('John'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with deeply nested fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('settings.notifications.email').ne(true));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with array index access', () => {
      const query = r
        .db('mydb')
        .table('posts')
        .filter((row) => row.field('tags.0').ne('technology'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('ne() with typed interfaces', () => {
    interface User {
      id: string;
      name: string;
      email: string;
      age?: number;
      active: boolean;
      profile?: {
        bio: string;
        verified: boolean;
      };
    }

    it('should work with typed row access', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('name').ne('John'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with optional typed fields', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('age').ne(25));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle type-safe comparisons', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('active').ne(true));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('ne() error handling', () => {
    it('should handle field not found errors', async () => {
      const error = new Error('Field not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('nonexistent').ne('value'));

      await expect(query.run(mockConnection)).rejects.toThrow('Field not found');
    });

    it('should handle type mismatch errors', async () => {
      const error = new Error('Type mismatch');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').ne('invalid'));

      await expect(query.run(mockConnection)).rejects.toThrow('Type mismatch');
    });

    it('should handle table not found errors', async () => {
      const error = new Error('Table not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('nonexistent')
        .filter((row) => row.field('name').ne('John'));

      await expect(query.run(mockConnection)).rejects.toThrow('Table not found');
    });
  });

  describe('ne() with pagination', () => {
    it('should handle ne queries with cursor options', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'Alice', active: false },
          { id: 'user2', name: 'Bob', active: false }
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
        .filter((row) => row.field('name').ne('John'));
      const result = await query.run(mockConnection, {
        startKey: 'start',
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
          startKey: 'start',
          batchSize: 50
        },
        options: {
          explain: false,
          timeoutMs: 0
        }
      });
      expect(result).toBeDefined();
    });
  });

  describe('ne() with query options', () => {
    it('should pass query options through ne operations', async () => {
      const mockResult = {
        items: [{ id: 'user1', name: 'Jane' }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').ne('John'));
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

  describe('ne() with default database', () => {
    it('should work with default database', () => {
      const query = r
        .db()
        .table('users')
        .filter((row) => row.field('name').ne('John'));

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

  describe('ne() performance considerations', () => {
    it('should work efficiently with indexed fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('email').ne('john@example.com'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with orderBy after ne filtering', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('active').ne(false))
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

    it('should work with limit after ne filtering', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').ne(25))
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
