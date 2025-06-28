import type { Connection } from './connection';
import type { EnhancedValueQuery } from './query';
import type { Query } from './rulo';

export interface DatabaseQuery<TDoc = unknown> {
  _type: 'database';
  _docType: TDoc;
}

export interface TableQuery<TDoc = unknown> {
  _type: 'table';
  _docType: TDoc;
}

export interface SelectionQuery<TDoc = unknown> {
  _type: 'selection';
  _docType: TDoc;
}

export interface StreamQuery<TDoc = unknown> {
  _type: 'stream';
  _docType: TDoc;
}

export interface ValueQuery<T = unknown> {
  _type: 'value';
  _valueType: T;
}

export interface ArrayQuery<T = unknown> {
  _type: 'array';
  _elementType: T;
}

export interface ObjectQuery<T = unknown> {
  _type: 'object';
  _objectType: T;
}

// Union type for all query states
export type QueryState<TDoc = unknown> =
  | DatabaseQuery<TDoc>
  | TableQuery<TDoc>
  | SelectionQuery<TDoc>
  | StreamQuery<TDoc>
  | ValueQuery
  | ArrayQuery
  | ObjectQuery;

export interface QueryResult<T = unknown> {
  result: T;
  metadata?: {
    queryId: string;
    timestamp: string;
    serverVersion: string;
  };
}

export interface CursorResult<T = unknown> {
  items: T[];
  cursor?: {
    startKey?: string;
    batchSize?: number;
  };
  metadata?: {
    queryId: string;
    timestamp: string;
    serverVersion: string;
  };
}

// Helper type to extract element type from Cursor
export type ExtractCursorType<T> = T extends Cursor<infer U> ? U : T;

// Type inference helpers for run method
export type InferRunResult<TState extends QueryState<unknown>> =
  TState extends TableQuery<infer TDoc>
    ? Cursor<TDoc>
    : TState extends StreamQuery<infer TDoc>
      ? Cursor<TDoc>
      : TState extends SelectionQuery<infer TDoc>
        ? TDoc | null
        : TState extends ValueQuery<infer T>
          ? T
          : TState extends ArrayQuery<infer T>
            ? Cursor<T>
            : TState extends ObjectQuery<infer T>
              ? T
              : TState extends DatabaseQuery<unknown>
                ? never
                : unknown;

// Helper type to infer document type from query state
export type InferDocType<TState extends QueryState<unknown>> =
  TState extends DatabaseQuery<infer TDoc>
    ? TDoc
    : TState extends TableQuery<infer TDoc>
      ? TDoc
      : TState extends SelectionQuery<infer TDoc>
        ? TDoc
        : TState extends StreamQuery<infer TDoc>
          ? TDoc
          : unknown;

// Error types
export interface RuloError extends Error {
  code: number;
  type: string;
  line?: number;
  column?: number;
}

// Query options
export interface QueryOptions {
  timeout?: number;
  explain?: boolean;
}

// Sort direction
export type SortDirection = 'asc' | 'desc';

// Sort field specification
export interface SortField {
  field: string;
  direction?: SortDirection;
}

// Cursor options for pagination
export interface CursorOptions {
  startKey?: string;
  batchSize?: number;
  sort?: SortField[];
}

// Document types - make them more flexible to accept typed interfaces
export type Document = Record<string, unknown> | { [K in string]: unknown };

// Update operations
export type UpdateObject = Record<string, unknown>;
export type PatchObject = Record<string, unknown>;

// Filter predicates
export type Predicate<T = unknown> = (row: T) => boolean;

// Type helpers for method chaining
export type QueryMethod<TState, TResult> = TState extends never
  ? never
  : (...args: unknown[]) => TResult;

// Utility types for extracting value types
export type ExtractValueType<T> = T extends ValueQuery<infer U> ? U : unknown;
export type ExtractElementType<T> = T extends ArrayQuery<infer U> ? U : unknown;
export type ExtractObjectType<T> = T extends ObjectQuery<infer U> ? U : unknown;

// Enhanced nested field access types up to 5 levels deep
export type NestedKeyOf<T> =
  T extends Record<string, unknown>
    ? {
        [K in keyof T]: K extends string
          ? T[K] extends Record<string, unknown>
            ? K | `${K}.${NestedKeyOf<T[K]>}`
            : K
          : never;
      }[keyof T]
    : string;

// Get nested property type by path
export type NestedPropertyType<T, K extends string> = K extends keyof T
  ? T[K]
  : K extends `${infer P}.${infer R}`
    ? P extends keyof T
      ? T[P] extends Record<string, unknown>
        ? NestedPropertyType<T[P], R>
        : unknown
      : unknown
    : unknown;

// Row type for providing field hints - now supports nested access
export type RowType<T = Record<string, unknown>> =
  T extends Record<string, unknown> ? T : Record<string, unknown>;

// Row query interface that supports both direct property access and field method
export interface IRowQuery<TRow> {
  field<K extends NestedKeyOf<TRow>>(name: K): EnhancedValueQuery<NestedPropertyType<TRow, K>>;
  field(name: string): EnhancedValueQuery<unknown>;
}

// Combined type for RowQuery with property access
export type RowQueryWithFields<TRow> = IRowQuery<TRow> & {
  readonly [K in keyof TRow]: EnhancedValueQuery<TRow[K]>;
};

export function isDatabaseQuery<TDoc = unknown>(
  state: QueryState<TDoc>
): state is DatabaseQuery<TDoc> {
  return state._type === 'database';
}

export function isTableQuery<TDoc = unknown>(state: QueryState<TDoc>): state is TableQuery<TDoc> {
  return state._type === 'table';
}

export function isSelectionQuery<TDoc = unknown>(
  state: QueryState<TDoc>
): state is SelectionQuery<TDoc> {
  return state._type === 'selection';
}

export function isStreamQuery<TDoc = unknown>(state: QueryState<TDoc>): state is StreamQuery<TDoc> {
  return state._type === 'stream';
}

export function isValueQuery(state: QueryState<unknown>): state is ValueQuery {
  return state._type === 'value';
}

export function isArrayQuery(state: QueryState<unknown>): state is ArrayQuery {
  return state._type === 'array';
}

export function isObjectQuery(state: QueryState<unknown>): state is ObjectQuery {
  return state._type === 'object';
}

export class Cursor<T> implements AsyncIterable<T> {
  private items: T[];
  private cursor?: {
    startKey?: string;
    batchSize?: number;
  };
  private metadata?: {
    queryId: string;
    timestamp: string;
    serverVersion: string;
  };
  private connection: Connection;
  private originalQuery: Query;
  private currentIndex: number = 0;
  private exhausted: boolean = false;
  private totalItemsReturned: number = 0;
  private originalLimit?: number;

  constructor(cursorResult: CursorResult<T>, connection: Connection, query: Query) {
    this.items = cursorResult.items;
    this.cursor = cursorResult.cursor;
    this.metadata = cursorResult.metadata;
    this.connection = connection;
    this.originalQuery = query;

    const batchSize = this.cursor?.batchSize || 50;
    // We're exhausted if:
    // 1. We got fewer items than expected (normal end of data)
    // 2. We have no startKey for next batch (server indicates no more data)
    this.exhausted = this.items.length < batchSize || !this.cursor?.startKey;
    this.totalItemsReturned = this.items.length;

    this.originalLimit = this.extractLimitFromQuery(query);
  }

  /**
   * Async iterator implementation
   */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<T> {
    while (this.currentIndex < this.items.length || this.canFetchMore()) {
      while (this.currentIndex < this.items.length) {
        yield this.items[this.currentIndex++];
      }

      if (this.canFetchMore()) {
        await this.fetchNextBatch();
      } else {
        break; // Exit loop if we can't fetch more and have no items left
      }
    }
  }

  /**
   * Collect all remaining items into an array
   */
  async toArray(): Promise<T[]> {
    const result: T[] = [...this.items.slice(this.currentIndex)];

    // Fetch remaining batches if any
    while (!this.exhausted && this.hasMore()) {
      const previousLength = this.items.length;
      await this.fetchNextBatch();

      result.push(...this.items.slice(previousLength));
    }

    this.currentIndex = this.items.length;

    return result;
  }

  /**
   * Get the next single item
   */
  async next(): Promise<T | undefined> {
    if (this.currentIndex < this.items.length) {
      return this.items[this.currentIndex++];
    }

    if (!this.exhausted && this.hasMore()) {
      await this.fetchNextBatch();
      if (this.currentIndex < this.items.length) {
        return this.items[this.currentIndex++];
      }
    }

    return undefined;
  }

  /**
   * Check if there are more items available
   */
  hasMore(): boolean {
    return this.currentIndex < this.items.length || this.canFetchMore();
  }

  /**
   * Check if we can fetch more data from the server
   * We can fetch more if we have a startKey for the next batch and we're not exhausted
   */
  private canFetchMore(): boolean {
    return !this.exhausted && !!this.cursor?.startKey;
  }

  /**
   * Get the current start key being used for pagination
   */
  getCurrentStartKey(): string | undefined {
    return this.cursor?.startKey;
  }

  /**
   * Get the key that will be used for the next batch (from server response)
   * Returns undefined if there are no more batches available
   */
  getNextStartKey(): string | undefined {
    return this.cursor?.startKey;
  }

  /**
   * Get the current batch size
   */
  getBatchSize(): number | undefined {
    return this.cursor?.batchSize;
  }

  /**
   * Check if this cursor has been exhausted (no more data available)
   */
  isExhausted(): boolean {
    return this.exhausted && this.currentIndex >= this.items.length;
  }

  /**
   * Get cursor metadata
   */
  getMetadata() {
    return this.metadata;
  }

  /**
   * Get current cursor state
   */
  getCursorState() {
    return this.cursor;
  }

  /**
   * Debug method to inspect cursor state
   */
  debugState(): void {
    console.log('[Cursor Debug State]', {
      currentIndex: this.currentIndex,
      itemsLength: this.items.length,
      exhausted: this.exhausted,
      totalItemsReturned: this.totalItemsReturned,
      originalLimit: this.originalLimit,
      hasMore: this.hasMore(),
      canFetchMore: this.canFetchMore(),
      isExhausted: this.isExhausted(),
      cursor: this.cursor,
      metadata: this.metadata
    });
  }

  /**
   * Fetch the next batch of items from the server
   */
  private async fetchNextBatch(): Promise<void> {
    if (this.exhausted || !this.cursor?.startKey) {
      this.exhausted = true;
      return;
    }

    // Check if we've already reached the original limit
    if (this.originalLimit && this.totalItemsReturned >= this.originalLimit) {
      this.exhausted = true;
      return;
    }

    try {
      // Create a continuation query by removing skip (already applied) and adjusting limit
      const continuationQuery = this.createContinuationQuery(
        this.cursor.startKey,
        this.cursor.batchSize || 50
      );

      const result = await this.connection.query(continuationQuery);

      if ('items' in result) {
        // This is a CursorResult - append new items to existing ones
        const newItems = result.items as T[];

        this.items = [...this.items, ...newItems];
        this.totalItemsReturned += newItems.length;

        // Update cursor state with new information from server
        this.cursor = {
          startKey: result.cursor?.startKey, // Next batch's start key from server
          batchSize: result.cursor?.batchSize || this.cursor.batchSize
        };

        const expectedBatchSize = this.cursor?.batchSize || 1000;
        this.exhausted =
          newItems.length < expectedBatchSize ||
          !result.cursor?.startKey ||
          (this.originalLimit !== undefined && this.totalItemsReturned >= this.originalLimit);

        // Update metadata if provided
        if (result.metadata) {
          this.metadata = result.metadata;
        }
      } else {
        // This shouldn't happen for cursor queries, but handle gracefully
        this.exhausted = true;
      }
    } catch (error) {
      this.exhausted = true;
      throw error;
    }
  }

  /**
   * Extract the limit value from the query structure
   */
  private extractLimitFromQuery(query: Query): number | undefined {
    // The query structure has operations as direct properties
    if (query.limit) {
      return query.limit.count;
    }

    // Check all operations that can have a source for nested limits
    const operationsWithSource = [
      'skip',
      'filter',
      'orderBy',
      'table',
      'get',
      'getAll',
      'count',
      'pluck',
      'without',
      'insert',
      'update',
      'delete',
      'tableCreate',
      'tableDrop',
      'databaseCreate',
      'databaseDrop',
      'tableList',
      'databaseList'
    ] as const;

    for (const op of operationsWithSource) {
      const operation = query[op as keyof Query] as { source?: Query } | undefined;
      if (operation?.source) {
        const nestedLimit = this.extractLimitFromQuery(operation.source);
        if (nestedLimit !== undefined) {
          return nestedLimit;
        }
      }
    }

    return undefined;
  }

  /**
   * Create a continuation query by removing skip and adjusting limit
   */
  private createContinuationQuery(startKey: string, batchSize: number): Query {
    // Create a new query based on the original, but with transformations
    const continuationQuery: Query = {};

    // Copy options if present
    if (this.originalQuery.options) {
      continuationQuery.options = { ...this.originalQuery.options };
    }

    // Set the continuation cursor
    continuationQuery.cursor = {
      startKey,
      batchSize
    };

    // Transform the query operations
    this.transformQueryOperations(this.originalQuery, continuationQuery);

    return continuationQuery;
  }

  /**
   * Transform query operations by removing skip and adjusting limit
   */
  private transformQueryOperations(source: Query, target: Query): void {
    // Skip operation should not be included in continuation queries
    if (source.skip) {
      // Skip has already been applied, so continue with its source
      if (source.skip.source) {
        this.transformQueryOperations(source.skip.source as Query, target);
      }
      return;
    }

    // Handle Limit operation - adjust based on items already returned
    if (source.limit) {
      const adjustedLimit = this.originalLimit
        ? Math.max(0, this.originalLimit - this.totalItemsReturned)
        : source.limit.count;

      if (adjustedLimit > 0) {
        target.limit = {
          ...source.limit,
          count: adjustedLimit
        };

        // Transform the source recursively
        if (source.limit.source) {
          target.limit.source = {};
          this.transformQueryOperations(source.limit.source as Query, target.limit.source as Query);
        }
      }
      return;
    }

    const operations = [
      'table',
      'get',
      'getAll',
      'filter',
      'orderBy',
      'count',
      'insert',
      'update',
      'delete',
      'databaseCreate',
      'databaseDrop',
      'databaseList',
      'tableCreate',
      'tableDrop',
      'tableList',
      'expression',
      'subquery'
    ] as const;

    for (const op of operations) {
      if (source[op as keyof Query]) {
        const sourceValue = source[op as keyof Query];
        if (sourceValue) {
          (target as Record<string, unknown>)[op] = { ...sourceValue };

          const sourceOp = sourceValue as Record<string, unknown>;
          if (sourceOp && typeof sourceOp === 'object' && sourceOp.source) {
            const currentTarget = (target as Record<string, unknown>)[op] as Record<
              string,
              unknown
            >;
            if (currentTarget && typeof currentTarget === 'object') {
              (target as Record<string, unknown>)[op] = {
                ...currentTarget,
                source: {}
              };
            } else {
              (target as Record<string, unknown>)[op] = { source: {} };
            }
            this.transformQueryOperations(
              sourceOp.source as Query,
              ((target as Record<string, unknown>)[op] as Record<string, unknown>).source as Query
            );
          }
        }
      }
    }
  }
}
