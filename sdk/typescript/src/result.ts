import { Cursor } from './cursor';
import { ExecutionResult } from './terms';

/**
 * Unified result interface that works for both streaming and immediate operations.
 * Users don't need to know about the underlying cursor vs result implementation.
 */
export class QueryResult<T> implements AsyncIterable<T> {
  private cursor?: Cursor<T>;
  private immediateResult?: ExecutionResult<T>;

  constructor(cursorOrResult: Cursor<T> | ExecutionResult<T>) {
    if (this.isCursor(cursorOrResult)) {
      this.cursor = cursorOrResult;
    } else {
      this.immediateResult = cursorOrResult;
    }
  }

  private isCursor(value: unknown): value is Cursor<T> {
    return (
      value != null &&
      typeof value === 'object' &&
      'toArray' in value &&
      typeof value.toArray === 'function'
    );
  }

  /**
   * Get all results as an array.
   * For immediate operations, returns the result(s) as an array.
   * For streaming operations, collects all results into an array.
   */
  async toArray(): Promise<T[]> {
    if (this.cursor) {
      const results = await this.cursor.toArray();
      return results;
    } else if (this.immediateResult) {
      const result = this.immediateResult.result;
      if (Array.isArray(result)) {
        return result;
      } else if (result !== null && result !== undefined) {
        return [result];
      } else {
        return [];
      }
    }
    return [];
  }

  /**
   * Get the first result, or null if no results.
   * Useful for get operations or when you only need the first item.
   */
  async first(): Promise<T | null> {
    if (this.cursor) {
      for await (const item of this.cursor) {
        return item;
      }
      return null;
    } else if (this.immediateResult) {
      const result = this.immediateResult.result;
      if (Array.isArray(result)) {
        return result.length > 0 ? result[0] : null;
      } else {
        return result !== undefined ? (result as T) : null;
      }
    }
    return null;
  }

  /**
   * Execute a callback for each result.
   * For immediate operations, calls the callback for each result.
   * For streaming operations, calls the callback as results are streamed.
   */
  async forEach(callback: (item: T, index: number) => void | Promise<void>): Promise<void> {
    if (this.cursor) {
      let index = 0;
      for await (const item of this.cursor) {
        await callback(item, index++);
      }
    } else if (this.immediateResult) {
      const result = this.immediateResult.result;
      if (Array.isArray(result)) {
        for (let i = 0; i < result.length; i++) {
          await callback(result[i], i);
        }
      } else if (result !== null && result !== undefined) {
        await callback(result as T, 0);
      }
    }
  }

  /**
   * Get the raw result for immediate operations.
   * Returns undefined for streaming operations.
   */
  get result(): T | T[] | null | undefined {
    return this.immediateResult?.result;
  }

  /**
   * Check if this is a streaming operation.
   */
  get isStreaming(): boolean {
    return !!this.cursor;
  }

  /**
   * Check if this is an immediate operation.
   */
  get isImmediate(): boolean {
    return !!this.immediateResult;
  }

  /**
   * Async iterator support for streaming through results.
   */
  async *[Symbol.asyncIterator](): AsyncIterator<T> {
    if (this.cursor) {
      for await (const item of this.cursor) {
        yield item;
      }
    } else if (this.immediateResult) {
      const result = this.immediateResult.result;
      if (Array.isArray(result)) {
        for (const item of result) {
          yield item;
        }
      } else if (result !== null && result !== undefined) {
        yield result as T;
      }
    }
  }

  /**
   * Close the underlying cursor if this is a streaming operation.
   */
  close(): void {
    if (this.cursor) {
      this.cursor.close();
    }
  }
}

/**
 * Factory function to create QueryResult from cursor or execution result
 */
export function createQueryResult<T>(
  cursorOrResult: Cursor<T> | ExecutionResult<T>
): QueryResult<T> {
  return new QueryResult(cursorOrResult);
}
