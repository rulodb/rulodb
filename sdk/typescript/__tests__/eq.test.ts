import { Connection } from '../src/connection';
import { r, RQuery } from '../src/query';
import { BinaryOp_Operator, NullValue } from '../src/rulo';

// Mock the Connection class
jest.mock('../src/connection');
const MockedConnection = Connection as jest.MockedClass<typeof Connection>;

describe('Equality (eq) Operations', () => {
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

  describe('eq() with string values', () => {
    it('should create equality comparison with string value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').eq('John'));

      expect(query).toBeInstanceOf(RQuery);
      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle empty string comparison', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').eq(''));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle string with special characters', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('email').eq('user@example.com'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle unicode strings', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').eq('José García'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute string equality query successfully', async () => {
      const mockResult = {
        items: [{ id: 'user1', name: 'John', email: 'john@example.com' }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').eq('John'));
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
                  op: BinaryOp_Operator.EQ,
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

  describe('eq() with numeric values', () => {
    it('should create equality comparison with integer value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').eq(25));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should create equality comparison with float value', () => {
      const query = r
        .db('mydb')
        .table('products')
        .filter((row) => row.field('price').eq(29.99));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle zero comparison', () => {
      const query = r
        .db('mydb')
        .table('counters')
        .filter((row) => row.field('value').eq(0));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle negative number comparison', () => {
      const query = r
        .db('mydb')
        .table('transactions')
        .filter((row) => row.field('amount').eq(-100));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute numeric equality query successfully', async () => {
      const mockResult = {
        items: [{ id: 'user1', name: 'John', age: 25 }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').eq(25));
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
                  op: BinaryOp_Operator.EQ,
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

  describe('eq() with boolean values', () => {
    it('should create equality comparison with true value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('active').eq(true));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should create equality comparison with false value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('deleted').eq(false));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute boolean equality query successfully', async () => {
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
        .filter((row) => row.field('active').eq(true));
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
                  op: BinaryOp_Operator.EQ,
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
                      bool: true
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

  describe('eq() with null values', () => {
    it('should create equality comparison with null value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('deleted_at').eq(null));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should create equality comparison with undefined value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('optional_field').eq(undefined));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute null equality query successfully', async () => {
      const mockResult = {
        items: [{ id: 'user1', name: 'John', deleted_at: null }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('deleted_at').eq(null));
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
                  op: BinaryOp_Operator.EQ,
                  left: expect.objectContaining({
                    subquery: expect.objectContaining({
                      expression: expect.objectContaining({
                        field: expect.objectContaining({
                          path: ['deleted_at'],
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

  describe('eq() with array values', () => {
    it('should create equality comparison with array value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('tags').eq(['admin', 'user']));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle empty array comparison', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('tags').eq([]));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle mixed type arrays', () => {
      const query = r
        .db('mydb')
        .table('data')
        .filter((row) => row.field('values').eq(['string', 123, true, null]));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle nested arrays', () => {
      const query = r
        .db('mydb')
        .table('matrix')
        .filter((row) =>
          row.field('data').eq([
            [1, 2],
            [3, 4]
          ])
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute array equality query successfully', async () => {
      const mockResult = {
        items: [{ id: 'user1', name: 'Admin User', tags: ['admin', 'user'] }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('tags').eq(['admin', 'user']));
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
                  op: BinaryOp_Operator.EQ,
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

  describe('eq() with object values', () => {
    it('should create equality comparison with object value', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('profile').eq({ name: 'John', age: 25 }));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle empty object comparison', () => {
      const query = r
        .db('mydb')
        .table('data')
        .filter((row) => row.field('metadata').eq({}));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle nested object comparison', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row.field('settings').eq({
            notifications: { email: true, sms: false },
            privacy: { public: false }
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
          row.field('preferences').eq({
            languages: ['en', 'es'],
            themes: ['dark'],
            features: []
          })
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute object equality query successfully', async () => {
      const mockResult = {
        items: [{ id: 'user1', profile: { name: 'John', age: 25 } }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('profile').eq({ name: 'John', age: 25 }));
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
                  op: BinaryOp_Operator.EQ,
                  left: expect.objectContaining({
                    subquery: expect.objectContaining({
                      expression: expect.objectContaining({
                        field: expect.objectContaining({
                          path: ['profile'],
                          separator: '.'
                        })
                      })
                    })
                  }),
                  right: expect.objectContaining({
                    literal: expect.objectContaining({
                      object: expect.any(Object)
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

  describe('eq() with binary data', () => {
    it('should create equality comparison with Uint8Array', () => {
      const binaryData = new Uint8Array([1, 2, 3, 4]);
      const query = r
        .db('mydb')
        .table('files')
        .filter((row) => row.field('signature').eq(binaryData));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle empty binary data', () => {
      const emptyData = new Uint8Array();
      const query = r
        .db('mydb')
        .table('files')
        .filter((row) => row.field('data').eq(emptyData));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute binary equality query successfully', async () => {
      const binaryData = new Uint8Array([1, 2, 3, 4]);
      const mockResult = {
        items: [{ id: 'file1', signature: binaryData }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('files')
        .filter((row) => row.field('signature').eq(binaryData));
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
                  op: BinaryOp_Operator.EQ,
                  left: expect.objectContaining({
                    subquery: expect.objectContaining({
                      expression: expect.objectContaining({
                        field: expect.objectContaining({
                          path: ['signature'],
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

  describe('eq() chaining with logical operations', () => {
    it('should chain eq with AND operation', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').eq('John').and(row.field('active').eq(true)));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should chain eq with OR operation', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('role').eq('admin').or(row.field('role').eq('moderator')));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle complex logical combinations', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) =>
          row
            .field('status')
            .eq('active')
            .and(row.field('role').eq('admin').or(row.field('verified').eq(true)))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should chain multiple eq operations', () => {
      const query = r
        .db('mydb')
        .table('products')
        .filter((row) =>
          row
            .field('category')
            .eq('electronics')
            .and(row.field('brand').eq('Apple'))
            .and(row.field('available').eq(true))
        );

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should execute chained equality query successfully', async () => {
      const mockResult = {
        items: [{ id: 'user1', name: 'John', active: true }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').eq('John').and(row.field('active').eq(true)));
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

  describe('eq() with nested field access', () => {
    it('should work with nested object fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('profile.name').eq('John'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with deeply nested fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('settings.notifications.email').eq(true));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with array index access', () => {
      const query = r
        .db('mydb')
        .table('posts')
        .filter((row) => row.field('tags.0').eq('technology'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('eq() with typed interfaces', () => {
    interface User {
      id: string;
      name: string;
      email: string;
      age: number;
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
        .filter((row) => row.field('name').eq('John'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with optional typed fields', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('profile.verified').eq(true));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should handle type-safe comparisons', () => {
      const query = r
        .db('mydb')
        .table<User>('users')
        .filter((row) => row.field('age').eq(25).and(row.field('active').eq(true)));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });
  });

  describe('eq() error handling', () => {
    it('should handle field not found errors', async () => {
      const error = new Error('Field not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('nonexistent').eq('value'));

      await expect(query.run(mockConnection)).rejects.toThrow('Field not found');
    });

    it('should handle type mismatch errors', async () => {
      const error = new Error('Type mismatch');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('age').eq('not_a_number'));

      await expect(query.run(mockConnection)).rejects.toThrow('Type mismatch');
    });

    it('should handle table not found errors', async () => {
      const error = new Error('Table not found');
      mockConnection.query.mockRejectedValue(error);

      const query = r
        .db('mydb')
        .table('nonexistent')
        .filter((row) => row.field('id').eq('123'));

      await expect(query.run(mockConnection)).rejects.toThrow('Table not found');
    });
  });

  describe('eq() with pagination', () => {
    it('should handle eq queries with cursor options', async () => {
      const mockResult = {
        items: [
          { id: 'user1', name: 'John', active: true },
          { id: 'user2', name: 'Jane', active: true }
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
        .filter((row) => row.field('active').eq(true));
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
  });

  describe('eq() with query options', () => {
    it('should pass query options through eq operations', async () => {
      const mockResult = {
        items: [{ id: 'user1', name: 'John' }],
        cursor: undefined
      };
      mockConnection.query.mockResolvedValue(mockResult);

      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('name').eq('John'));
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

  describe('eq() with default database', () => {
    it('should work with default database', () => {
      const query = r
        .db()
        .table('users')
        .filter((row) => row.field('name').eq('John'));

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

  describe('eq() performance considerations', () => {
    it('should work efficiently with indexed fields', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('email').eq('john@example.com'));

      expect(query._query.filter).toBeDefined();
      expect(query._query.filter!.predicate).toBeDefined();
    });

    it('should work with orderBy after equality filtering', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('active').eq(true))
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

    it('should work with limit after equality filtering', () => {
      const query = r
        .db('mydb')
        .table('users')
        .filter((row) => row.field('active').eq(true))
        .limit(10);

      expect(query._query.limit).toBeDefined();
      expect(query._query.limit!.count).toBe(10);
    });
  });
});
