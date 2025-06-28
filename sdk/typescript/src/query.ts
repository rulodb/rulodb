import { Connection } from './connection';
import {
  BinaryOp_Operator,
  Datum,
  DatumObject,
  Expression,
  NullValue,
  Query,
  SortDirection,
  SortField,
  TableRef
} from './rulo';
import {
  ArrayQuery,
  Cursor,
  CursorOptions,
  CursorResult,
  DatabaseQuery,
  Document,
  InferRunResult,
  NestedKeyOf,
  NestedPropertyType,
  QueryOptions,
  QueryResult,
  QueryState,
  RowQueryWithFields,
  SelectionQuery,
  SortField as TypeSortField,
  StreamQuery,
  TableQuery,
  ValueQuery
} from './types';

// Forward declaration to avoid circular dependency
interface RuloClient {
  run<T = unknown>(
    query: RQuery<QueryState<unknown>, string, unknown>,
    options?: QueryOptions
  ): Promise<T | Cursor<T>>;
  getConnection(): Connection;
}

/**
 * Main query builder class with fluent API and strong typing
 */
export class RQuery<
  TState extends QueryState<TDoc> = ValueQuery,
  TDbName extends string = string,
  TDoc = unknown
> {
  public _query: Query;
  public _dbName: TDbName | undefined;

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  constructor(query: Query = {}, _state: TState = { _type: 'value' } as TState, dbName?: TDbName) {
    this._query = query;
    this._dbName = dbName;
  }

  // ========== Database Operations ==========

  /**
   * Select a database. If no name provided, uses the default database.
   */
  db<TName extends string = 'default'>(
    name?: TName
  ): RQuery<DatabaseQuery<unknown>, TName, unknown> {
    const dbName = (name || 'default') as TName;
    return new RQuery<DatabaseQuery<unknown>, TName, unknown>(
      {},
      { _type: 'database', _docType: undefined as unknown },
      dbName
    );
  }

  /**
   * Create a database (only available on root queries)
   */
  dbCreate(name: string): RQuery<ValueQuery<{ created: number }>, string, unknown> {
    const query: Query = {
      databaseCreate: { name }
    };
    return new RQuery<ValueQuery<{ created: number }>, string, unknown>(query, {
      _type: 'value',
      _valueType: undefined
    } as unknown as ValueQuery<{ created: number }>);
  }

  /**
   * Drop a database (only available on root queries)
   */
  dbDrop(name: string): RQuery<ValueQuery<{ dropped: number }>, string, unknown> {
    const query: Query = {
      databaseDrop: { name }
    };
    return new RQuery<ValueQuery<{ dropped: number }>, string, unknown>(query, {
      _type: 'value',
      _valueType: undefined
    } as unknown as ValueQuery<{ dropped: number }>);
  }

  /**
   * List all databases (only available on root queries)
   */
  dbList(): RQuery<ArrayQuery<string>, string, unknown> {
    const query: Query = {
      databaseList: {}
    };
    return new RQuery<ArrayQuery<string>, string, unknown>(query, {
      _type: 'array',
      _elementType: undefined
    } as unknown as ArrayQuery<string>);
  }

  // ========== Table Operations (only available on DatabaseQuery) ==========

  /**
   * Select a table from this database (only available on DatabaseQuery)
   */
  table<TTableDoc = Record<string, unknown>>(
    this: RQuery<DatabaseQuery<unknown>, TDbName, unknown>,
    name: string
  ): RQuery<TableQuery<TTableDoc>, TDbName, TTableDoc> {
    const dbName = this._dbName || 'default';
    const tableRef: TableRef = {
      database: { name: dbName },
      name
    };
    const query: Query = {
      table: { table: tableRef }
    };
    return new RQuery<TableQuery<TTableDoc>, TDbName, TTableDoc>(
      query,
      { _type: 'table', _docType: undefined as TTableDoc },
      this._dbName
    );
  }

  /**
   * Create a table (only available on DatabaseQuery)
   */
  tableCreate(
    this: RQuery<DatabaseQuery<unknown>, TDbName, unknown>,
    name: string
  ): RQuery<ValueQuery<{ created: number }>, TDbName, unknown> {
    const dbName = this._dbName || 'default';
    const tableRef: TableRef = {
      database: { name: dbName },
      name
    };
    const query: Query = {
      tableCreate: { table: tableRef }
    };
    return new RQuery<ValueQuery<{ created: number }>, TDbName, unknown>(
      query,
      {
        _type: 'value',
        _valueType: undefined
      } as unknown as ValueQuery<{ created: number }>,
      this._dbName
    );
  }

  /**
   * Drop a table (only available on DatabaseQuery)
   */
  tableDrop(
    this: RQuery<DatabaseQuery<unknown>, TDbName, unknown>,
    name: string
  ): RQuery<ValueQuery<{ dropped: number }>, TDbName, unknown> {
    const dbName = this._dbName || 'default';
    const tableRef: TableRef = {
      database: { name: dbName },
      name
    };
    const query: Query = {
      tableDrop: { table: tableRef }
    };
    return new RQuery<ValueQuery<{ dropped: number }>, TDbName, unknown>(
      query,
      {
        _type: 'value',
        _valueType: undefined
      } as unknown as ValueQuery<{ dropped: number }>,
      this._dbName
    );
  }

  /**
   * List all tables in this database (only available on DatabaseQuery)
   */
  tableList(
    this: RQuery<DatabaseQuery<unknown>, TDbName, unknown>
  ): RQuery<ArrayQuery<string>, TDbName, unknown> {
    const dbName = this._dbName || 'default';
    const query: Query = {
      tableList: { database: { name: dbName } }
    };
    return new RQuery<ArrayQuery<string>, TDbName, unknown>(
      query,
      { _type: 'array', _elementType: undefined } as unknown as ArrayQuery<string>,
      this._dbName
    );
  }

  // ========== Document Retrieval (only available on TableQuery) ==========

  /**
   * Get a single document by primary key (only available on TableQuery)
   */
  get(
    this: RQuery<TableQuery<TDoc>, TDbName, TDoc>,
    key: string
  ): RQuery<SelectionQuery<TDoc>, TDbName, TDoc> {
    const keyDatum = this.convertValueToDatum(key);
    const query: Query = {
      get: {
        source: this._query,
        key: keyDatum
      }
    };
    return new RQuery<SelectionQuery<TDoc>, TDbName, TDoc>(
      query,
      { _type: 'selection', _docType: undefined as TDoc },
      this._dbName
    );
  }

  /**
   * Get multiple documents by primary keys (only available on TableQuery)
   */
  getAll(
    this: RQuery<TableQuery<TDoc>, TDbName, TDoc>,
    ...keys: string[]
  ): RQuery<StreamQuery<TDoc>, TDbName, TDoc> {
    const keyData = keys.map((key) => this.convertValueToDatum(key));
    const query: Query = {
      getAll: {
        source: this._query,
        keys: keyData
      }
    };
    return new RQuery<StreamQuery<TDoc>, TDbName, TDoc>(
      query,
      { _type: 'stream', _docType: undefined as TDoc },
      this._dbName
    );
  }

  // ========== Filtering and Transformation ==========

  /**
   * Filter documents (available on TableQuery and StreamQuery)
   */
  filter(
    this: RQuery<TableQuery<TDoc> | StreamQuery<TDoc>, TDbName, TDoc>,
    predicate: Document | ((row: RowQueryWithFields<TDoc>) => EnhancedValueQuery<boolean>)
  ): RQuery<StreamQuery<TDoc>, TDbName, TDoc> {
    let predicateExpr: Expression;

    if (typeof predicate === 'function') {
      const rowQuery = createRowQuery<TDoc>();
      const result = predicate(rowQuery);
      predicateExpr = this.convertQueryToExpression(result._query);
    } else {
      predicateExpr = this.convertObjectToExpression(predicate);
    }

    const query: Query = {
      filter: {
        source: this._query,
        predicate: predicateExpr
      }
    };
    return new RQuery<StreamQuery<TDoc>, TDbName, TDoc>(
      query,
      { _type: 'stream', _docType: undefined as TDoc },
      this._dbName
    );
  }

  // ========== Ordering and Limiting ==========

  /**
   * Order results (available on TableQuery and StreamQuery)
   */
  orderBy(
    this: RQuery<TableQuery<TDoc> | StreamQuery<TDoc>, TDbName, TDoc>,
    ...fields: (string | TypeSortField)[]
  ): RQuery<StreamQuery<TDoc>, TDbName, TDoc> {
    const sortFields: SortField[] = fields.map((field) => {
      if (typeof field === 'string') {
        return {
          fieldName: field,
          direction: SortDirection.ASC
        };
      } else {
        return {
          fieldName: field.field,
          direction: field.direction === 'desc' ? SortDirection.DESC : SortDirection.ASC
        };
      }
    });

    const query: Query = {
      orderBy: {
        source: this._query,
        fields: sortFields
      }
    };
    return new RQuery<StreamQuery<TDoc>, TDbName, TDoc>(
      query,
      { _type: 'stream', _docType: undefined as TDoc },
      this._dbName
    );
  }

  /**
   * Limit number of results (available on TableQuery and StreamQuery)
   */
  limit(
    this: RQuery<TableQuery<TDoc> | StreamQuery<TDoc>, TDbName, TDoc>,
    count: number
  ): RQuery<StreamQuery<TDoc>, TDbName, TDoc> {
    const query: Query = {
      limit: {
        source: this._query,
        count
      }
    };
    return new RQuery<StreamQuery<TDoc>, TDbName, TDoc>(
      query,
      { _type: 'stream', _docType: undefined as TDoc },
      this._dbName
    );
  }

  /**
   * Skip number of results (available on TableQuery and StreamQuery)
   */
  skip(
    this: RQuery<TableQuery<TDoc> | StreamQuery<TDoc>, TDbName, TDoc>,
    count: number
  ): RQuery<StreamQuery<TDoc>, TDbName, TDoc> {
    // Check if the current query has a limit operation at the top level
    // If so, we need to reorder to ensure skip comes before limit
    if (this._query.limit) {
      // Extract the limit operation and its source
      const limitOp = this._query.limit;
      const limitSource = limitOp.source;

      // Create new query with skip wrapping the limit's source, then limit on top
      const query: Query = {
        limit: {
          source: {
            skip: {
              source: limitSource,
              count
            }
          },
          count: limitOp.count
        }
      };

      return new RQuery<StreamQuery<TDoc>, TDbName, TDoc>(
        query,
        { _type: 'stream', _docType: undefined as TDoc },
        this._dbName
      );
    }

    // No limit operation, proceed normally
    const query: Query = {
      skip: {
        source: this._query,
        count
      }
    };
    return new RQuery<StreamQuery<TDoc>, TDbName, TDoc>(
      query,
      { _type: 'stream', _docType: undefined as TDoc },
      this._dbName
    );
  }

  // ========== Aggregation ==========

  /**
   * Count documents (available on TableQuery and StreamQuery)
   */
  count(
    this: RQuery<TableQuery<TDoc> | StreamQuery<TDoc>, TDbName, TDoc>
  ): RQuery<ValueQuery<number>, TDbName, TDoc> {
    const query: Query = {
      count: {
        source: this._query
      }
    };
    return new RQuery<ValueQuery<number>, TDbName, TDoc>(
      query,
      { _type: 'value', _valueType: undefined } as unknown as ValueQuery<number>,
      this._dbName
    );
  }

  /**
   * Pluck specific fields from documents (available on TableQuery and StreamQuery)
   */
  pluck<TResult = TDoc, K extends NestedKeyOf<TResult> = NestedKeyOf<TResult>>(
    this: RQuery<TableQuery<TDoc> | StreamQuery<TDoc>, TDbName, TDoc>,
    ...fields: K[]
  ): RQuery<StreamQuery<TResult>, TDbName, TResult>;
  pluck<TResult = TDoc, K extends NestedKeyOf<TResult> = NestedKeyOf<TResult>>(
    this: RQuery<TableQuery<TDoc> | StreamQuery<TDoc>, TDbName, TDoc>,
    fields: K[]
  ): RQuery<StreamQuery<TResult>, TDbName, TResult>;
  pluck<TResult = TDoc, K extends NestedKeyOf<TResult> = NestedKeyOf<TResult>>(
    this: RQuery<TableQuery<TDoc> | StreamQuery<TDoc>, TDbName, TDoc>,
    fields: K[],
    options: { separator?: string }
  ): RQuery<StreamQuery<TResult>, TDbName, TResult>;
  pluck<TResult = TDoc, K extends NestedKeyOf<TResult> = NestedKeyOf<TResult>>(
    fieldsOrFirst: K[] | K,
    ...args: unknown[]
  ): RQuery<StreamQuery<TResult>, TDbName, TResult> {
    let fields: string[];
    let separator = '.';

    if (Array.isArray(fieldsOrFirst)) {
      fields = fieldsOrFirst;
      if (
        args.length > 0 &&
        typeof args[0] === 'object' &&
        args[0] !== null &&
        !Array.isArray(args[0])
      ) {
        separator = (args[0] as { separator?: string }).separator || '.';
      }
    } else {
      fields = [fieldsOrFirst, ...(args as K[])];
    }

    const fieldRefs = fields.map((field) => ({
      path: String(field).split(separator),
      separator
    }));

    const query: Query = {
      pluck: {
        source: this._query,
        fields: fieldRefs
      }
    };

    // Always return stream query type - the actual behavior is handled at runtime by the backend
    return new RQuery<StreamQuery<TResult>, TDbName, TResult>(
      query,
      { _type: 'stream', _docType: undefined as TResult },
      this._dbName
    );
  }

  // ========== Modification Operations ==========

  /**
   * Insert documents (only available on TableQuery)
   */
  insert(
    this: RQuery<TableQuery<TDoc>, TDbName, TDoc>,
    documents: TDoc | TDoc[]
  ): RQuery<ValueQuery<{ inserted: number; generatedKeys: string[] }>, TDbName, TDoc> {
    const docs = Array.isArray(documents) ? documents : [documents];
    const datumObjects = docs.map((doc) => this.convertObjectToDatumObject(doc));

    const query: Query = {
      insert: {
        source: this._query,
        documents: datumObjects
      }
    };
    return new RQuery<ValueQuery<{ inserted: number; generatedKeys: string[] }>, TDbName, TDoc>(
      query,
      { _type: 'value', _valueType: undefined } as unknown as ValueQuery<{
        inserted: number;
        generatedKeys: string[];
      }>,
      this._dbName
    );
  }

  /**
   * Update documents (available on SelectionQuery, TableQuery, and StreamQuery)
   */
  update(
    this: RQuery<SelectionQuery<TDoc> | TableQuery<TDoc> | StreamQuery<TDoc>, TDbName, TDoc>,
    patch: Partial<TDoc>
  ): RQuery<ValueQuery<{ updated: number }>, TDbName, TDoc> {
    const patchObject = this.convertObjectToDatumObject(patch);

    const query: Query = {
      update: {
        source: this._query,
        patch: patchObject
      }
    };
    return new RQuery<ValueQuery<{ updated: number }>, TDbName, TDoc>(
      query,
      { _type: 'value', _valueType: undefined } as unknown as ValueQuery<{ updated: number }>,
      this._dbName
    );
  }

  /**
   * Delete documents (available on SelectionQuery, TableQuery, and StreamQuery)
   */
  delete(
    this: RQuery<SelectionQuery<TDoc> | TableQuery<TDoc> | StreamQuery<TDoc>, TDbName, TDoc>
  ): RQuery<ValueQuery<{ deleted: number }>, TDbName, TDoc> {
    const query: Query = {
      delete: {
        source: this._query
      }
    };
    return new RQuery<ValueQuery<{ deleted: number }>, TDbName, TDoc>(
      query,
      { _type: 'value', _valueType: undefined } as unknown as ValueQuery<{ deleted: number }>,
      this._dbName
    );
  }

  // ========== Execution ==========

  /**
   * Execute the query with automatic type inference
   */
  async run(
    connection: Connection | RuloClient,
    options?: QueryOptions & CursorOptions
  ): Promise<InferRunResult<TState>>;

  /**
   * Execute the query with explicit type override
   */
  async run<TResult = InferRunResult<TState>>(
    connection: Connection | RuloClient,
    options?: QueryOptions & CursorOptions
  ): Promise<TResult>;

  async run<TResult = InferRunResult<TState>>(
    connection: Connection | RuloClient,
    options?: QueryOptions & CursorOptions
  ): Promise<TResult> {
    const query: Query = {
      ...this._query
    };

    // Only add cursor if cursor options are provided
    if (options && (options.startKey !== undefined || options.batchSize !== undefined)) {
      query.cursor = {
        startKey: options.startKey,
        batchSize: options.batchSize || 50
      };
    }

    // Always add options if any options object is provided
    if (options) {
      query.options = {
        timeoutMs: options.timeout || 0,
        explain: options.explain || false
      };
    }

    let result: unknown;
    if ('getConnection' in connection) {
      // This is a Client instance, use its run method which handles cursor wrapping
      result = await (connection as RuloClient).run(
        this as RQuery<QueryState<unknown>, string, unknown>,
        options
      );
      return result as TResult; // Client handles the result conversion
    } else {
      // This is a Connection instance, query directly and handle cursor wrapping
      result = await (connection as Connection).query(query);

      // Return the appropriate result based on query type
      if (typeof result === 'object' && result !== null && 'items' in result) {
        // CursorResult - return Cursor instance for async iteration
        const cursorResult = result as CursorResult<TResult>;

        // Ensure cursor state is properly initialized for tracking
        const enhancedCursorResult: CursorResult<TResult> = {
          ...cursorResult,
          cursor: cursorResult.cursor
            ? {
                startKey: cursorResult.cursor.startKey,
                batchSize: cursorResult.cursor.batchSize || options?.batchSize || 50
              }
            : {
                startKey: options?.startKey,
                batchSize: options?.batchSize || 50
              }
        };

        return new Cursor(enhancedCursorResult, connection as Connection, query) as TResult;
      } else if (typeof result === 'object' && result !== null && 'result' in result) {
        // QueryResult - return single result
        return (result as QueryResult<TResult>).result as TResult;
      }

      return result as TResult;
    }
  }

  // ========== Helper Methods ==========

  protected convertValueToDatum(value: unknown): Datum {
    if (value === null || value === undefined) {
      return { null: NullValue.NULL_VALUE };
    }

    if (typeof value === 'boolean') {
      return { bool: value };
    }

    if (typeof value === 'number') {
      if (Number.isInteger(value)) {
        return { int: value.toString() };
      } else {
        return { float: value };
      }
    }

    if (typeof value === 'string') {
      return { string: value };
    }

    if (value instanceof Uint8Array) {
      return { binary: value };
    }

    if (Array.isArray(value)) {
      return {
        array: {
          items: value.map((item) => this.convertValueToDatum(item)),
          elementType: ''
        }
      };
    }

    if (typeof value === 'object') {
      return {
        object: this.convertObjectToDatumObject(value)
      };
    }

    throw new Error(`Unsupported value type: ${typeof value}`);
  }

  private convertObjectToDatumObject(obj: unknown): DatumObject {
    const fields: { [key: string]: Datum } = {};
    if (obj && typeof obj === 'object') {
      for (const [key, value] of Object.entries(obj)) {
        fields[key] = this.convertValueToDatum(value);
      }
    }
    return {
      fields
    };
  }

  private convertObjectToExpression(obj: Document): Expression {
    // For simple object comparison, create a series of AND expressions
    const entries = Object.entries(obj);
    if (entries.length === 0) {
      return { literal: { bool: true } };
    }

    let expr: Expression = this.createFieldEqualsExpression(entries[0][0], entries[0][1]);

    for (let i = 1; i < entries.length; i++) {
      const [field, value] = entries[i];
      const fieldExpr = this.createFieldEqualsExpression(field, value);
      expr = {
        binary: {
          op: BinaryOp_Operator.AND,
          left: expr,
          right: fieldExpr
        }
      };
    }

    return expr;
  }

  private createFieldEqualsExpression(field: string, value: unknown): Expression {
    return {
      binary: {
        op: BinaryOp_Operator.EQ,
        left: {
          field: {
            path: [field],
            separator: '.'
          }
        },
        right: {
          literal: this.convertValueToDatum(value)
        }
      }
    };
  }

  private convertQueryToExpression(query: Query): Expression {
    return {
      subquery: query
    };
  }
}

/**
 * Enhanced value query that provides comparison methods with logical operators
 */
export class EnhancedValueQuery<T> {
  public _query: Query;

  constructor(query: Query) {
    this._query = query;
  }

  private createExpressionQuery(expression: Expression): Query {
    return { expression };
  }

  eq(value: T): EnhancedValueQuery<boolean> {
    const expression: Expression = {
      binary: {
        op: BinaryOp_Operator.EQ,
        left: { subquery: this._query },
        right: { literal: this.convertValueToDatum(value) }
      }
    };
    return new EnhancedValueQuery<boolean>(this.createExpressionQuery(expression));
  }

  ne(value: T): EnhancedValueQuery<boolean> {
    const expression: Expression = {
      binary: {
        op: BinaryOp_Operator.NE,
        left: { subquery: this._query },
        right: { literal: this.convertValueToDatum(value) }
      }
    };
    return new EnhancedValueQuery<boolean>(this.createExpressionQuery(expression));
  }

  lt(value: T): EnhancedValueQuery<boolean> {
    const expression: Expression = {
      binary: {
        op: BinaryOp_Operator.LT,
        left: { subquery: this._query },
        right: { literal: this.convertValueToDatum(value) }
      }
    };
    return new EnhancedValueQuery<boolean>(this.createExpressionQuery(expression));
  }

  le(value: T): EnhancedValueQuery<boolean> {
    const expression: Expression = {
      binary: {
        op: BinaryOp_Operator.LE,
        left: { subquery: this._query },
        right: { literal: this.convertValueToDatum(value) }
      }
    };
    return new EnhancedValueQuery<boolean>(this.createExpressionQuery(expression));
  }

  gt(value: T): EnhancedValueQuery<boolean> {
    const expression: Expression = {
      binary: {
        op: BinaryOp_Operator.GT,
        left: { subquery: this._query },
        right: { literal: this.convertValueToDatum(value) }
      }
    };
    return new EnhancedValueQuery<boolean>(this.createExpressionQuery(expression));
  }

  ge(value: T): EnhancedValueQuery<boolean> {
    const expression: Expression = {
      binary: {
        op: BinaryOp_Operator.GE,
        left: { subquery: this._query },
        right: { literal: this.convertValueToDatum(value) }
      }
    };
    return new EnhancedValueQuery<boolean>(this.createExpressionQuery(expression));
  }

  and(other: EnhancedValueQuery<boolean>): EnhancedValueQuery<boolean> {
    const expression: Expression = {
      binary: {
        op: BinaryOp_Operator.AND,
        left: { subquery: this._query },
        right: { subquery: other._query }
      }
    };
    return new EnhancedValueQuery<boolean>(this.createExpressionQuery(expression));
  }

  or(other: EnhancedValueQuery<boolean>): EnhancedValueQuery<boolean> {
    const expression: Expression = {
      binary: {
        op: BinaryOp_Operator.OR,
        left: { subquery: this._query },
        right: { subquery: other._query }
      }
    };
    return new EnhancedValueQuery<boolean>(this.createExpressionQuery(expression));
  }

  match(pattern: string, flags: string = ''): EnhancedValueQuery<boolean> {
    const expression: Expression = {
      match: {
        value: { subquery: this._query },
        pattern,
        flags
      }
    };
    return new EnhancedValueQuery<boolean>(this.createExpressionQuery(expression));
  }

  private convertValueToDatum(value: unknown): Datum {
    if (value === null || value === undefined) {
      return { null: NullValue.NULL_VALUE };
    }

    if (typeof value === 'boolean') {
      return { bool: value };
    }

    if (typeof value === 'number') {
      if (Number.isInteger(value)) {
        return { int: value.toString() };
      } else {
        return { float: value };
      }
    }

    if (typeof value === 'string') {
      return { string: value };
    }

    if (value instanceof Uint8Array) {
      return { binary: value };
    }

    if (Array.isArray(value)) {
      return {
        array: {
          items: value.map((item) => this.convertValueToDatum(item)),
          elementType: ''
        }
      };
    }

    if (typeof value === 'object') {
      const fields: { [key: string]: Datum } = {};
      for (const [key, val] of Object.entries(value)) {
        fields[key] = this.convertValueToDatum(val);
      }
      return {
        object: {
          fields
        }
      };
    }

    throw new Error(`Unsupported value type: ${typeof value}`);
  }
}

/**
 * Special query builder for row operations in filters and updates with enhanced field access
 */
export class RowQuery<TRow = Record<string, unknown>> {
  /**
   * Reference a field in the current row with nested field support
   */
  field<K extends NestedKeyOf<TRow>>(name: K): EnhancedValueQuery<NestedPropertyType<TRow, K>>;
  field(name: string): EnhancedValueQuery<unknown>;
  field(name: string): EnhancedValueQuery<unknown> {
    const fieldPath = String(name).split('.');
    const expression: Expression = {
      field: {
        path: fieldPath,
        separator: '.'
      }
    };
    const query: Query = {
      expression
    };
    return new EnhancedValueQuery<unknown>(query);
  }
}

/**
 * Factory function to create a properly typed RowQuery with property access
 */
function createRowQuery<TRow = Record<string, unknown>>(): RowQueryWithFields<TRow> {
  const rowQuery = new RowQuery<TRow>();

  return new Proxy(rowQuery, {
    get(target: RowQuery<TRow>, prop: string | symbol) {
      if (typeof prop === 'string') {
        // Handle the field method
        if (prop === 'field') {
          return target.field.bind(target);
        }

        // Skip internal/prototype methods
        if (prop.startsWith('_') || prop in Object.prototype || prop === 'constructor') {
          return (target as unknown as Record<string, unknown>)[prop];
        }

        // For any other property, create a field accessor
        return target.field(prop);
      }

      return (target as unknown as Record<string | symbol, unknown>)[prop];
    }
  }) as RowQueryWithFields<TRow>;
}

/**
 * Main query entry point
 */
export const r = new RQuery<ValueQuery, string, unknown>({}, {
  _type: 'value',
  _valueType: undefined
} as ValueQuery);

/**
 * Helper function to create a row reference for use in filters with enhanced nested field support
 */
export function row<TRow = Record<string, unknown>>(): RowQueryWithFields<TRow> {
  return createRowQuery<TRow>();
}
