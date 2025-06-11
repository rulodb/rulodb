import { Client } from './client';
import { Term, TermAST, TermType } from './types';

export type CursorOptions = {
  batchSize?: number;
};

// Type guard to check if a result is a Cursor
export function isCursor<T>(result: unknown): result is Cursor<T> {
  return (
    result != null &&
    typeof result === 'object' &&
    'toArray' in result &&
    typeof result.toArray === 'function'
  );
}

export class Cursor<T> implements AsyncIterable<T> {
  private client: Client;
  private readonly originalQuery: Term;
  private readonly options: CursorOptions;
  private startKey?: string;
  private buffer: T[] = [];
  private done = false;

  constructor(client: Client, query: Term, options: CursorOptions = {}) {
    this.client = client;
    this.originalQuery = query;
    this.options = options;
  }

  private async fetchNextBatch(): Promise<void> {
    if (this.done) return;

    const updatedQuery = this.injectPagination(
      this.originalQuery,
      this.startKey,
      this.options.batchSize
    );

    try {
      const response = await this.client.send<TermAST, T[]>(updatedQuery.toAST());
      const raw = Array.isArray(response) ? response : [response];

      if (raw.length === 0) {
        this.done = true;
        return;
      }

      this.buffer.push(...raw.filter((i) => i !== null));

      const lastItem = raw[raw.length - 1];

      if (lastItem && typeof lastItem === 'object' && lastItem !== null && 'id' in lastItem) {
        const nextStartKey = lastItem.id as string;
        if (!nextStartKey) {
          this.done = true;
          return;
        }

        // If we got fewer results than requested batch size, we're done
        if (this.options.batchSize && raw.length < this.options.batchSize) {
          this.done = true;
          return;
        }

        this.startKey = nextStartKey;
      } else {
        this.done = true;
        return;
      }
    } catch (err) {
      this.done = true;
      throw Error(`Failed to fetch next batch: ${err}`);
    }
  }

  private injectPagination(query: Term, startKey?: string, batchSize?: number): Term {
    const ast = query.toAST();
    const [termType, args, optArgs] = ast;

    // Check if this is a Table term
    if (termType === TermType.Table) {
      // Always produce the 3-element form for Table with pagination
      const newOptArgs = {
        ...((optArgs as Record<string, unknown>) || {}),
        ...(batchSize !== undefined ? { batch_size: batchSize } : {}),
        ...(startKey !== undefined ? { start_key: startKey } : {})
      };

      return {
        toAST: () => [termType, args, newOptArgs as Record<string, unknown>]
      };
    }

    // Recursively process arguments if they are Terms
    const newArgs = (Array.isArray(args) ? args : []).map((arg) => {
      if (arg && typeof arg === 'object' && 'toAST' in arg) {
        return this.injectPagination(arg as Term, startKey, batchSize);
      }
      return arg;
    });

    // Preserve the original structure (2 or 3 elements)
    if (ast.length === 3) {
      return {
        toAST: () => [termType, newArgs, optArgs as Record<string, unknown>]
      };
    } else {
      return {
        toAST: () => [termType, newArgs]
      };
    }
  }

  public async *[Symbol.asyncIterator](): AsyncIterator<T> {
    try {
      while (!this.done || this.buffer.length > 0) {
        if (this.buffer.length === 0) {
          await this.fetchNextBatch();
        }

        while (this.buffer.length > 0) {
          yield this.buffer.shift()!;
        }
      }
    } finally {
      this.close();
    }
  }

  public async toArray(): Promise<T[]> {
    const out: T[] = [];
    for await (const row of this) {
      out.push(row);
    }
    return out;
  }

  public return(): Promise<IteratorResult<T>> {
    this.close();
    return Promise.resolve({ value: undefined as unknown, done: true });
  }

  public close(): void {
    this.done = true;
  }

  public async executeImmediate<R = unknown>(): Promise<R> {
    const response = await this.client.send<TermAST, R>(this.originalQuery.toAST());
    return response;
  }
}
