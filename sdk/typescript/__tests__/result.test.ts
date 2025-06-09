import { Client } from '../src/client';
import { Cursor } from '../src/cursor';
import { createQueryResult } from '../src/result';
import { ExecutionResult, TermType } from '../src/terms';

interface MockClient extends Pick<Client, 'send'> {
  send: jest.Mock;
}

describe('QueryResult', () => {
  let mockClient: MockClient;

  beforeEach(() => {
    mockClient = { send: jest.fn() };
  });

  describe('Immediate Operations', () => {
    it('should handle single result from ExecutionResult', async () => {
      const executionResult: ExecutionResult<{ id: string; name: string }> = {
        result: { id: '123', name: 'test' },
        stats: {
          read_count: 1,
          inserted_count: 0,
          updated_count: 0,
          deleted_count: 0,
          error_count: 0,
          duration_ms: 10
        }
      };

      const queryResult = createQueryResult(executionResult);

      expect(queryResult.isImmediate).toBe(true);
      expect(queryResult.isStreaming).toBe(false);
      expect(queryResult.result).toEqual({ id: '123', name: 'test' });

      const first = await queryResult.first();
      expect(first).toEqual({ id: '123', name: 'test' });

      const array = await queryResult.toArray();
      expect(array).toEqual([{ id: '123', name: 'test' }]);
    });

    it('should handle array result from ExecutionResult', async () => {
      const executionResult: ExecutionResult<Array<{ id: string; name: string }>> = {
        result: [
          { id: '123', name: 'Alice' },
          { id: '456', name: 'Bob' }
        ],
        stats: {
          read_count: 0,
          inserted_count: 2,
          updated_count: 0,
          deleted_count: 0,
          error_count: 0,
          duration_ms: 15
        }
      };

      const queryResult = createQueryResult(executionResult);

      expect(queryResult.isImmediate).toBe(true);
      expect(queryResult.result).toEqual([
        { id: '123', name: 'Alice' },
        { id: '456', name: 'Bob' }
      ]);

      const first = await queryResult.first();
      expect(first).toEqual({ id: '123', name: 'Alice' });

      const array = await queryResult.toArray();
      expect(array).toEqual([
        { id: '123', name: 'Alice' },
        { id: '456', name: 'Bob' }
      ]);
    });

    it('should handle null result from ExecutionResult', async () => {
      const executionResult: ExecutionResult<null> = {
        result: null,
        stats: {
          read_count: 1,
          inserted_count: 0,
          updated_count: 0,
          deleted_count: 0,
          error_count: 0,
          duration_ms: 5
        }
      };

      const queryResult = createQueryResult(executionResult);

      expect(queryResult.result).toBeNull();

      const first = await queryResult.first();
      expect(first).toBeNull();

      const array = await queryResult.toArray();
      expect(array).toEqual([]);
    });

    it('should support forEach with immediate results', async () => {
      const executionResult: ExecutionResult<Array<{ id: string; name: string }>> = {
        result: [
          { id: '123', name: 'Alice' },
          { id: '456', name: 'Bob' }
        ],
        stats: {
          read_count: 0,
          inserted_count: 2,
          updated_count: 0,
          deleted_count: 0,
          error_count: 0,
          duration_ms: 15
        }
      };

      const queryResult = createQueryResult(executionResult);
      const collected: Array<{ item: unknown; index: number }> = [];

      await queryResult.forEach((item, index) => {
        collected.push({ item, index });
      });

      expect(collected).toEqual([
        { item: { id: '123', name: 'Alice' }, index: 0 },
        { item: { id: '456', name: 'Bob' }, index: 1 }
      ]);
    });

    it('should support async iteration with immediate results', async () => {
      const executionResult: ExecutionResult<Array<{ id: string; name: string }>> = {
        result: [
          { id: '123', name: 'Alice' },
          { id: '456', name: 'Bob' }
        ],
        stats: {
          read_count: 0,
          inserted_count: 2,
          updated_count: 0,
          deleted_count: 0,
          error_count: 0,
          duration_ms: 15
        }
      };

      const queryResult = createQueryResult(executionResult);
      const collected = [];

      for await (const item of queryResult) {
        collected.push(item);
      }

      expect(collected).toEqual([
        { id: '123', name: 'Alice' },
        { id: '456', name: 'Bob' }
      ]);
    });
  });

  describe('Streaming Operations', () => {
    it('should handle Cursor', async () => {
      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['test']], {});
      const queryResult = createQueryResult(cursor);

      expect(queryResult.isStreaming).toBe(true);
      expect(queryResult.isImmediate).toBe(false);
      expect(queryResult.result).toBeUndefined();
    });

    it('should delegate toArray to cursor', async () => {
      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['test']], {});
      const mockData = [
        { id: '1', name: 'Alice' },
        { id: '2', name: 'Bob' }
      ];

      // Mock the cursor's toArray method
      jest.spyOn(cursor, 'toArray').mockResolvedValue(mockData);

      const queryResult = createQueryResult(cursor);
      const result = await queryResult.toArray();

      expect(result).toEqual(mockData);
      expect(cursor.toArray).toHaveBeenCalled();
    });

    it('should delegate close to cursor', () => {
      const cursor = new Cursor(mockClient as unknown as Client, [TermType.Table, ['test']], {});
      jest.spyOn(cursor, 'close').mockImplementation();

      const queryResult = createQueryResult(cursor);
      queryResult.close();

      expect(cursor.close).toHaveBeenCalled();
    });
  });

  describe('Edge cases', () => {
    it('should handle empty arrays', async () => {
      const executionResult: ExecutionResult<Array<unknown>> = {
        result: [],
        stats: {
          read_count: 0,
          inserted_count: 0,
          updated_count: 0,
          deleted_count: 0,
          error_count: 0,
          duration_ms: 1
        }
      };

      const queryResult = createQueryResult(executionResult);

      expect(await queryResult.toArray()).toEqual([]);
      expect(await queryResult.first()).toBeNull();

      const collected: unknown[] = [];
      await queryResult.forEach((item) => {
        collected.push(item);
      });
      expect(collected).toEqual([]);
    });

    it('should handle undefined result', async () => {
      const executionResult: ExecutionResult<undefined> = {
        result: undefined,
        stats: {
          read_count: 0,
          inserted_count: 0,
          updated_count: 0,
          deleted_count: 0,
          error_count: 0,
          duration_ms: 1
        }
      };

      const queryResult = createQueryResult(executionResult);

      expect(await queryResult.toArray()).toEqual([]);
      expect(await queryResult.first()).toBeNull();
    });
  });
});
