import { Client } from '../src/client';
import { Cursor, isCursor } from '../src/cursor';
import { Term, TermType } from '../src/types';

// Mock the client
jest.mock('../src/client');

describe('Cursor', () => {
  let mockClient: jest.Mocked<Client>;
  let mockQuery: jest.Mocked<Term>;

  beforeEach(() => {
    mockClient = {
      send: jest.fn(),
      close: jest.fn()
    } as any;

    mockQuery = {
      toAST: jest.fn().mockReturnValue([TermType.Table, ['db', 'table']])
    };
  });

  describe('Constructor', () => {
    it('should create cursor with query and default options', () => {
      const cursor = new Cursor(mockClient, mockQuery);

      expect(cursor).toBeDefined();
      expect(cursor).toBeInstanceOf(Cursor);
    });

    it('should create cursor with custom options', () => {
      const options = { batchSize: 100 };
      const cursor = new Cursor(mockClient, mockQuery, options);

      expect(cursor).toBeDefined();
    });
  });

  describe('Type guard', () => {
    it('should identify valid cursor objects', () => {
      const cursor = new Cursor(mockClient, mockQuery);
      expect(isCursor(cursor)).toBe(true);
    });

    it('should reject non-cursor objects', () => {
      expect(isCursor(null)).toBe(false);
      expect(isCursor(undefined)).toBe(false);
      expect(isCursor({})).toBe(false);
      expect(isCursor({ toArray: 'not a function' })).toBe(false);
      expect(isCursor([])).toBe(false);
      expect(isCursor('string')).toBe(false);
      expect(isCursor(42)).toBe(false);
    });

    it('should identify cursor-like objects', () => {
      const cursorLike = {
        toArray: jest.fn()
      };
      expect(isCursor(cursorLike)).toBe(true);
    });
  });

  describe('Basic operations', () => {
    it('should handle toArray with simple data', async () => {
      const mockData = [
        { id: 'item1', name: 'Item 1' },
        { id: 'item2', name: 'Item 2' }
      ];

      mockClient.send.mockResolvedValueOnce(mockData).mockResolvedValueOnce([]);

      const cursor = new Cursor(mockClient, mockQuery, { batchSize: 10 });
      const result = await cursor.toArray();

      expect(result).toEqual(mockData);
    });

    it('should handle empty cursor', async () => {
      mockClient.send.mockResolvedValue([]);

      const cursor = new Cursor(mockClient, mockQuery);
      const result = await cursor.toArray();

      expect(result).toEqual([]);
    });

    it('should return iterator result on return()', async () => {
      const cursor = new Cursor(mockClient, mockQuery);
      const result = await cursor.return();

      expect(result).toEqual({
        value: undefined,
        done: true
      });
    });

    it('should close cursor properly', () => {
      const cursor = new Cursor(mockClient, mockQuery);

      cursor.close();

      // Cursor should be marked as done
      expect((cursor as any).done).toBe(true);
    });
  });

  describe('Execute immediate', () => {
    it('should execute query immediately without cursor logic', async () => {
      const expectedResult = { result: 'immediate' };
      mockClient.send.mockResolvedValue(expectedResult);

      const cursor = new Cursor(mockClient, mockQuery);
      const result = await cursor.executeImmediate();

      expect(result).toEqual(expectedResult);
      expect(mockClient.send).toHaveBeenCalledWith(mockQuery.toAST());
    });

    it('should handle immediate execution errors', async () => {
      const error = new Error('Immediate execution failed');
      mockClient.send.mockRejectedValue(error);

      const cursor = new Cursor(mockClient, mockQuery);

      await expect(cursor.executeImmediate()).rejects.toThrow('Immediate execution failed');
    });
  });

  describe('Pagination basics', () => {
    it('should inject pagination into Table terms', () => {
      const tableQuery: Term = {
        toAST: () => [TermType.Table, ['db', 'table']]
      };

      const cursor = new Cursor(mockClient, tableQuery, { batchSize: 50 });

      // Access private method through any cast for testing
      const injectedQuery = (cursor as any).injectPagination(tableQuery, 'start123', 50);
      const ast = injectedQuery.toAST();

      expect(ast).toHaveLength(3);
      expect(ast[0]).toBe(TermType.Table);
      expect(ast[1]).toEqual(['db', 'table']);
      expect(ast[2]).toMatchObject({
        batch_size: 50,
        start_key: 'start123'
      });
    });

    it('should preserve existing options in Table terms', () => {
      const tableQuery: Term = {
        toAST: () => [TermType.Table, ['db', 'table'], { existing_option: 'value' } as any]
      };

      const cursor = new Cursor(mockClient, tableQuery, { batchSize: 40 });
      const injectedQuery = (cursor as any).injectPagination(tableQuery, 'start999', 40);
      const ast = injectedQuery.toAST();

      expect(ast[2]).toMatchObject({
        existing_option: 'value',
        batch_size: 40,
        start_key: 'start999'
      });
    });
  });

  describe('Error handling', () => {
    it('should handle network timeouts gracefully', async () => {
      const timeoutError = new Error('Request timeout');
      timeoutError.name = 'TimeoutError';

      mockClient.send.mockRejectedValue(timeoutError);

      const cursor = new Cursor(mockClient, mockQuery);

      await expect(cursor.toArray()).rejects.toThrow(
        'Failed to fetch next batch: TimeoutError: Request timeout'
      );
    });
  });

  describe('Type safety', () => {
    it('should maintain generic type through cursor operations', async () => {
      interface User {
        id: string;
        name: string;
        email: string;
      }

      const userData: User[] = [
        { id: 'user1', name: 'John', email: 'john@example.com' },
        { id: 'user2', name: 'Jane', email: 'jane@example.com' }
      ];

      mockClient.send.mockResolvedValueOnce(userData).mockResolvedValueOnce([]);

      const cursor = new Cursor<User>(mockClient, mockQuery);
      const results = await cursor.toArray();

      expect(results).toEqual(userData);
      // TypeScript should infer results as User[]
    });
  });
});
