import { Connection, ConnectionOptions } from './connection';
import { RQuery } from './query';
import { Query } from './rulo';
import {
  Cursor,
  CursorOptions,
  CursorResult,
  QueryOptions,
  QueryResult,
  QueryState
} from './types';

/**
 * RuloDB client that combines connection management with query building
 */
export class Client {
  private connection: Connection;

  constructor(options: ConnectionOptions = {}) {
    this.connection = new Connection(options);
  }

  /**
   * Connect to the database
   */
  async connect(): Promise<void> {
    await this.connection.connect();
  }

  /**
   * Disconnect from the database
   */
  async disconnect(): Promise<void> {
    await this.connection.disconnect();
  }

  /**
   * Check if connected to the database
   */
  isConnected(): boolean {
    return this.connection.isConnected();
  }

  /**
   * Execute a query directly with explicit type override
   */
  async run<T = unknown>(
    query: RQuery<QueryState<unknown>, string, unknown>,
    options?: QueryOptions & CursorOptions
  ): Promise<T> {
    // If cursor options are provided, ensure we create a proper cursor query
    const enhancedOptions =
      options && (options.batchSize || options.startKey)
        ? {
            ...options,
            batchSize: options.batchSize || 50
          }
        : options;

    const result = await query.run(this.connection, enhancedOptions);

    // If the result is a cursor result, wrap it in a Cursor instance
    if (result && typeof result === 'object' && 'items' in result && 'cursor' in result) {
      const cursorResult = result as CursorResult<T>;

      // Ensure cursor has proper state for iteration
      const enhancedCursorResult: CursorResult<T> = {
        ...cursorResult,
        cursor: cursorResult.cursor
          ? {
              startKey: cursorResult.cursor.startKey,
              batchSize: cursorResult.cursor.batchSize || enhancedOptions?.batchSize || 50
            }
          : undefined
      };

      return new Cursor(enhancedCursorResult, this.connection, query._query) as T;
    }

    return result as T;
  }

  /**
   * Get the underlying connection for direct use
   */
  getConnection(): Connection {
    return this.connection;
  }

  /**
   * Add event listeners for connection events
   */
  on(
    event: 'connect' | 'disconnect' | 'error' | 'close',
    listener: (...args: unknown[]) => void
  ): this {
    this.connection.on(event, listener);
    return this;
  }

  /**
   * Remove event listeners
   */
  off(
    event: 'connect' | 'disconnect' | 'error' | 'close',
    listener: (...args: unknown[]) => void
  ): this {
    this.connection.off(event, listener);
    return this;
  }

  /**
   * Execute a raw query with the connection
   */
  async queryRaw<T = unknown>(query: Query): Promise<QueryResult<T> | Cursor<T>> {
    const result = await this.connection.query(query);

    // If the result is a cursor result, wrap it in a Cursor instance
    if (result && typeof result === 'object' && 'items' in result && 'cursor' in result) {
      const cursorResult = result as CursorResult<T>;

      // Ensure cursor has proper state for iteration
      const enhancedCursorResult: CursorResult<T> = {
        ...cursorResult,
        cursor: cursorResult.cursor
          ? {
              startKey: cursorResult.cursor.startKey,
              batchSize: cursorResult.cursor.batchSize
            }
          : undefined
      };

      return new Cursor(enhancedCursorResult, this.connection, query);
    }

    return result as QueryResult<T>;
  }

  /**
   * Create a cursor from query results with proper state tracking
   */
  private createCursor<T>(cursorResult: CursorResult<T>, query: Query): Cursor<T> {
    // Ensure cursor has all necessary state for proper iteration
    const enhancedResult: CursorResult<T> = {
      ...cursorResult,
      cursor: cursorResult.cursor
        ? {
            startKey: cursorResult.cursor.startKey,
            batchSize: cursorResult.cursor.batchSize
          }
        : {
            startKey: '',
            batchSize: 50
          }
    };

    return new Cursor(enhancedResult, this.connection, query);
  }
}

/**
 * Create a new RuloDB client instance
 */
export function createClient(options: ConnectionOptions = {}): Client {
  return new Client(options);
}

/**
 * Default export for convenience
 */
export default Client;
