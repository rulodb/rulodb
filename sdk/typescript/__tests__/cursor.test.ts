import { Client } from '../src/client';
import { Cursor, isCursor } from '../src/cursor';
import { ExecutionResult, Term, TermType } from '../src/terms';

interface MockClient extends Pick<Client, 'send'> {
  send: jest.Mock;
}

describe('isCursor', () => {
  it('should return true for Cursor instances', () => {
    const mockClient = { send: jest.fn() } as unknown as Client;
    const cursor = new Cursor(mockClient, [TermType.Table, ['users']], {});
    expect(isCursor(cursor)).toBe(true);
  });

  it('should return false for ExecutionResult', () => {
    const executionResult: ExecutionResult<string> = {
      result: 'test',
      stats: {
        read_count: 1,
        inserted_count: 0,
        updated_count: 0,
        deleted_count: 0,
        error_count: 0,
        duration_ms: 10
      }
    };
    expect(isCursor(executionResult)).toBe(false);
  });
});

describe('Cursor', () => {
  let mockClient: MockClient;

  beforeEach(() => {
    mockClient = { send: jest.fn() };
  });

  describe('constructor', () => {
    it('should initialize with provided parameters', () => {
      const query: Term = [TermType.Table, ['users']];
      const cursor = new Cursor(mockClient as unknown as Client, query, { batchSize: 10 });

      expect(cursor).toBeInstanceOf(Cursor);
      // Test that the original query is cloned by checking it's not the same reference
      expect(cursor['originalQuery']).toEqual(query);
      expect(cursor['originalQuery']).not.toBe(query);
      expect(cursor['batchSize']).toBe(10);
    });

    it('should initialize without batchSize', () => {
      const query: Term = [TermType.Table, ['users']];
      const cursor = new Cursor(mockClient as unknown as Client, query, {});

      expect(cursor['batchSize']).toBeUndefined();
    });
  });

  describe('async iteration', () => {
    it('should iterate over single batch of results', async () => {
      const mockData = [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' }
      ];

      mockClient.send.mockResolvedValueOnce(mockData).mockResolvedValueOnce([]); // Next batch returns empty to stop pagination

      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['users']], {});
      const results = [];

      for await (const item of cursor) {
        results.push(item);
      }

      expect(results).toEqual(mockData);
      expect(mockClient.send).toHaveBeenCalledTimes(2);
    });

    it('should iterate over multiple batches', async () => {
      const batch1 = [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' }
      ];
      const batch2 = [{ id: '3', name: 'Charlie' }];

      mockClient.send.mockResolvedValueOnce(batch1).mockResolvedValueOnce(batch2);

      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['users']], {
        batchSize: 2
      });
      const results = [];

      for await (const item of cursor) {
        results.push(item);
      }

      expect(results).toEqual([...batch1, ...batch2]);
      expect(mockClient.send).toHaveBeenCalledTimes(2);
    });

    it('should handle empty results', async () => {
      mockClient.send.mockResolvedValueOnce([]);

      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['users']], {});
      const results = [];

      for await (const item of cursor) {
        results.push(item);
      }

      expect(results).toEqual([]);
      expect(mockClient.send).toHaveBeenCalledTimes(1);
    });

    it('should handle single non-array response', async () => {
      const mockData = { id: '1', name: 'Alice' };
      mockClient.send.mockResolvedValueOnce(mockData);

      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['users']], {});
      const results = [];

      for await (const item of cursor) {
        results.push(item);
      }

      expect(results).toEqual([mockData]);
    });

    it('should filter out null values', async () => {
      const mockData = [{ id: '1', name: 'Alice' }, null, { id: '2', name: 'Bob' }, null];

      mockClient.send.mockResolvedValueOnce(mockData).mockResolvedValueOnce([]); // Next batch returns empty to stop pagination

      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['users']], {});
      const results = [];

      for await (const item of cursor) {
        results.push(item);
      }

      expect(results).toEqual([
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' }
      ]);
    });

    it('should handle fetch errors', async () => {
      mockClient.send.mockRejectedValueOnce(new Error('Network error'));

      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['users']], {});

      await expect(async () => {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        for await (const _res of cursor) {
          // This should throw
        }
      }).rejects.toThrow('Failed to fetch next batch: Error: Network error');
    });

    it('should stop pagination when batch size is not met', async () => {
      const batch1 = [{ id: '1', name: 'Alice' }];

      mockClient.send.mockResolvedValueOnce(batch1);

      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['users']], {
        batchSize: 5
      });
      const results = [];

      for await (const item of cursor) {
        results.push(item);
      }

      expect(results).toEqual(batch1);
      expect(mockClient.send).toHaveBeenCalledTimes(1);
    });

    it('should handle items without id field', async () => {
      const mockData = [{ name: 'Alice' }, { name: 'Bob' }];

      mockClient.send.mockResolvedValueOnce(mockData);

      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['users']], {});
      const results = [];

      for await (const item of cursor) {
        results.push(item);
      }

      expect(results).toEqual(mockData);
    });
  });

  describe('toArray', () => {
    it('should collect all results into array', async () => {
      const mockData = [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' }
      ];

      mockClient.send.mockResolvedValueOnce(mockData).mockResolvedValueOnce([]); // Next batch returns empty to stop pagination

      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['users']], {});
      const results = await cursor.toArray();

      expect(results).toEqual(mockData);
    });

    it('should handle empty results', async () => {
      mockClient.send.mockResolvedValueOnce([]);

      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['users']], {});
      const results = await cursor.toArray();

      expect(results).toEqual([]);
    });
  });

  describe('return', () => {
    it('should close cursor and return done iterator result', async () => {
      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['users']], {});
      const result = await cursor.return();

      expect(result.done).toBe(true);
      expect(cursor['done']).toBe(true);
    });
  });

  describe('close', () => {
    it('should mark cursor as done', () => {
      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['users']], {});
      cursor.close();

      expect(cursor['done']).toBe(true);
    });
  });

  describe('executeImmediate', () => {
    it('should execute original query immediately', async () => {
      const mockResponse: ExecutionResult<string> = {
        result: 'immediate result',
        stats: {
          read_count: 1,
          inserted_count: 0,
          updated_count: 0,
          deleted_count: 0,
          error_count: 0,
          duration_ms: 5
        }
      };

      mockClient.send.mockResolvedValueOnce(mockResponse);

      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['users']], {});
      const result = await cursor.executeImmediate();

      expect(result).toEqual(mockResponse);
      expect(mockClient.send).toHaveBeenCalledWith([TermType.Table, ['users']]);
    });
  });

  describe('pagination injection', () => {
    it('should inject pagination into Table terms', async () => {
      const mockData = [{ id: '1', name: 'Alice' }];
      mockClient.send.mockResolvedValueOnce(mockData);

      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['users']], {
        batchSize: 10
      });

      // Trigger fetchNextBatch by starting iteration
      const iterator = cursor[Symbol.asyncIterator]();
      await iterator.next();

      expect(mockClient.send).toHaveBeenCalledWith([TermType.Table, ['users'], { batch_size: 10 }]);
    });

    it('should inject start_key for subsequent batches', async () => {
      const batch1 = [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' }
      ];
      const batch2 = [{ id: '3', name: 'Charlie' }];

      mockClient.send
        .mockResolvedValueOnce(batch1)
        .mockResolvedValueOnce(batch2)
        .mockResolvedValueOnce([]);

      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['users']], {
        batchSize: 2
      });

      // Consume all results to trigger multiple batches
      const results = [];
      for await (const item of cursor) {
        results.push(item);
      }

      expect(mockClient.send).toHaveBeenNthCalledWith(1, [
        TermType.Table,
        ['users'],
        { batch_size: 2 }
      ]);
      expect(mockClient.send).toHaveBeenNthCalledWith(2, [
        TermType.Table,
        ['users'],
        { batch_size: 2, start_key: '2' }
      ]);
    });

    it('should preserve existing optArgs when injecting pagination', async () => {
      const mockData = [{ id: '1', name: 'Alice' }];
      mockClient.send.mockResolvedValueOnce(mockData);

      const cursor = new Cursor(
        mockClient as unknown as Client,
        [TermType.Table, ['users'], { index: 'name' }],
        { batchSize: 10 }
      );

      const iterator = cursor[Symbol.asyncIterator]();
      await iterator.next();

      expect(mockClient.send).toHaveBeenCalledWith([
        TermType.Table,
        ['users'],
        { index: 'name', batch_size: 10 }
      ]);
    });

    it('should handle non-Table terms by recursively processing args', async () => {
      const mockData = [{ id: '1', name: 'Alice' }];
      mockClient.send.mockResolvedValueOnce(mockData);

      const nestedQuery: Term = [
        TermType.Filter,
        [
          [TermType.Table, ['users']],
          [TermType.Eq, [['field'], 'value']]
        ]
      ];

      const cursor = new Cursor(mockClient as unknown as Client, nestedQuery, { batchSize: 10 });

      const iterator = cursor[Symbol.asyncIterator]();
      await iterator.next();

      expect(mockClient.send).toHaveBeenCalledWith([
        TermType.Filter,
        [
          [TermType.Table, ['users'], { batch_size: 10 }],
          [TermType.Eq, [['field'], 'value']]
        ]
      ]);
    });

    it('should preserve 2-element term structure when no optArgs exist', async () => {
      const mockData = [{ id: '1', name: 'Alice' }];
      mockClient.send.mockResolvedValueOnce(mockData);

      const cursor = new Cursor(
        mockClient as unknown as Client,
        [TermType.Filter, [['table'], ['predicate']]],
        {}
      );

      const iterator = cursor[Symbol.asyncIterator]();
      await iterator.next();

      expect(mockClient.send).toHaveBeenCalledWith([TermType.Filter, [['table'], ['predicate']]]);
    });

    it('should handle empty start_key by stopping pagination', async () => {
      const batch1 = [
        { id: '1', name: 'Alice' },
        { id: '', name: 'Bob' } // Empty id
      ];

      mockClient.send.mockResolvedValueOnce(batch1);

      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['users']], {
        batchSize: 10
      });

      const results = [];
      for await (const item of cursor) {
        results.push(item);
      }

      expect(results).toEqual(batch1);
      expect(mockClient.send).toHaveBeenCalledTimes(1); // Should not fetch next batch
    });
  });

  describe('cleanup', () => {
    it('should close cursor when async iteration completes', async () => {
      const mockData = [{ id: '1', name: 'Alice' }];
      mockClient.send.mockResolvedValueOnce(mockData).mockResolvedValueOnce([]); // Next batch returns empty to stop pagination

      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['users']], {});

      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      for await (const _item of cursor) {
        // Consume results
      }

      expect(cursor['done']).toBe(true);
    });

    it('should close cursor when async iteration throws error', async () => {
      mockClient.send.mockRejectedValueOnce(new Error('Test error'));

      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['users']], {});

      try {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        for await (const _res of cursor) {
          // This should throw
        }
      } catch (error) {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        // Expected
      }

      expect(cursor['done']).toBe(true);
    });
  });
});
