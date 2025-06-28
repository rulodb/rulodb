import { EventEmitter } from 'events';
import * as net from 'net';

import {
  Cursor,
  Datum,
  Envelope,
  ErrorInfo,
  MessageType,
  ProtocolVersion,
  Query,
  QueryResult as ProtoQueryResult,
  Response,
  ResponseMetadata
} from './rulo';
import { CursorResult, QueryResult, RuloError } from './types';

export interface ConnectionOptions {
  host?: string;
  port?: number;
  timeout?: number;
  retries?: number;
  poolSize?: number;
}

export class Connection extends EventEmitter {
  private socket: net.Socket | null = null;
  private connected = false;
  private reconnecting = false;
  private pendingQueries = new Map<
    string,
    {
      resolve: (value: unknown) => void;
      reject: (error: Error) => void;
      timeout?: NodeJS.Timeout;
    }
  >();
  private buffer = Buffer.alloc(0);
  private queryCounter = 0;

  constructor(private options: ConnectionOptions = {}) {
    super();
    this.options = {
      host: 'localhost',
      port: 6090,
      timeout: 30000,
      retries: 3,
      poolSize: 10,
      ...options
    };
  }

  async connect(): Promise<void> {
    if (this.connected || this.reconnecting) {
      return;
    }

    this.reconnecting = true;

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.socket?.destroy();
        reject(new Error(`Connection timeout after ${this.options.timeout}ms`));
      }, this.options.timeout);

      this.socket = new net.Socket();

      this.socket.on('connect', () => {
        clearTimeout(timeout);
        this.connected = true;
        this.reconnecting = false;
        this.emit('connect');
        resolve();
      });

      this.socket.on('data', (data) => {
        this.handleData(data);
      });

      this.socket.on('error', (error) => {
        clearTimeout(timeout);
        this.connected = false;
        this.reconnecting = false;
        this.emit('error', error);
        reject(error);
      });

      this.socket.on('close', () => {
        this.connected = false;
        this.emit('close');
        // Reject all pending queries
        for (const [queryId, { reject }] of this.pendingQueries) {
          reject(new Error(`Connection closed, query "${queryId}" dropped`));
        }
        this.pendingQueries.clear();
      });

      this.socket.connect(this.options.port!, this.options.host!);
    });
  }

  async disconnect(): Promise<void> {
    if (!this.socket) {
      return;
    }

    return new Promise((resolve) => {
      this.socket!.once('close', () => {
        this.socket = null;
        this.connected = false;
        resolve();
      });
      this.socket!.end();
    });
  }

  isConnected(): boolean {
    return this.connected;
  }

  async query(query: Query): Promise<QueryResult | CursorResult> {
    if (!this.connected) {
      throw new Error('Not connected to database');
    }

    const queryId = this.generateQueryId();
    const envelope = this.createEnvelope(queryId, MessageType.QUERY, Query.encode(query).finish());

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingQueries.delete(queryId);
        reject(new Error(`Query timeout after ${this.options.timeout}ms`));
      }, this.options.timeout);

      this.pendingQueries.set(queryId, {
        resolve: (result: unknown) => {
          clearTimeout(timeout);
          resolve(result as QueryResult | CursorResult);
        },
        reject: (error: Error) => {
          clearTimeout(timeout);
          reject(error);
        },
        timeout
      });

      this.sendEnvelope(envelope);
    });
  }

  private generateQueryId(): string {
    return `query-${++this.queryCounter}-${Date.now()}`;
  }

  private createEnvelope(queryId: string, type: MessageType, payload: Uint8Array): Envelope {
    return {
      version: ProtocolVersion.VERSION_1,
      queryId,
      type,
      payload
    };
  }

  private sendEnvelope(envelope: Envelope): void {
    if (!this.socket) {
      throw new Error('Socket not available');
    }

    const encodedEnvelope = Envelope.encode(envelope).finish();
    const length = Buffer.allocUnsafe(4);
    length.writeUInt32BE(encodedEnvelope.length, 0);

    this.socket.write(Buffer.concat([length, Buffer.from(encodedEnvelope)]));
  }

  private handleData(data: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, data]);

    while (this.buffer.length >= 4) {
      const messageLength = this.buffer.readUInt32BE(0);

      if (this.buffer.length < 4 + messageLength) {
        // Not enough data for complete message
        break;
      }

      const messageData = this.buffer.subarray(4, 4 + messageLength);
      this.buffer = this.buffer.subarray(4 + messageLength);

      try {
        const envelope = Envelope.decode(messageData);
        this.handleEnvelope(envelope);
      } catch (error) {
        this.emit('error', new Error(`Failed to decode envelope: ${error}`));
      }
    }
  }

  private handleEnvelope(envelope: Envelope): void {
    const pending = this.pendingQueries.get(envelope.queryId);
    if (!pending) {
      this.emit('error', new Error(`Received response for unknown query: ${envelope.queryId}`));
      return;
    }

    this.pendingQueries.delete(envelope.queryId);

    try {
      switch (envelope.type) {
        case MessageType.RESPONSE: {
          const response = Response.decode(envelope.payload);
          this.handleResponse(response, pending);
          break;
        }

        case MessageType.ERROR: {
          const errorInfo = ErrorInfo.decode(envelope.payload);
          const error = this.createRuloError(errorInfo);
          pending.reject(error);
          break;
        }

        default:
          pending.reject(new Error(`Unexpected message type: ${envelope.type}`));
      }
    } catch (error) {
      pending.reject(new Error(`Failed to handle response: ${error}`));
    }
  }

  private handleResponse(
    response: Response,
    pending: { resolve: (value: unknown) => void; reject: (error: Error) => void }
  ): void {
    if (response.error) {
      const error = this.createRuloError(response.error);
      pending.reject(error);
      return;
    }

    if (response.query) {
      const result = this.convertQueryResult(response.query, response.metadata);
      pending.resolve(result);
      return;
    }

    if (response.pong) {
      pending.resolve(response.pong);
      return;
    }

    if (response.plan) {
      pending.resolve(response.plan);
      return;
    }

    if (response.authResult) {
      pending.resolve(response.authResult);
      return;
    }

    pending.reject(new Error('Invalid response format'));
  }

  private convertQueryResult(
    queryResult: ProtoQueryResult,
    metadata?: ResponseMetadata
  ): QueryResult | CursorResult {
    const resultMetadata = metadata
      ? {
          queryId: metadata.queryId || '',
          timestamp: metadata.timestamp || '0',
          serverVersion: metadata.serverVersion || ''
        }
      : undefined;

    // Handle single value results
    if (queryResult.literal) {
      return {
        result: this.convertDatum(queryResult.literal.value),
        metadata: resultMetadata
      };
    }

    if (queryResult.get) {
      return {
        result: this.convertDatum(queryResult.get.document),
        metadata: resultMetadata
      };
    }

    if (queryResult.count) {
      return {
        result: parseInt(queryResult.count.count, 10),
        metadata: resultMetadata
      };
    }

    // Handle array results with potential cursors
    if (queryResult.getAll) {
      return this.convertArrayResult(
        queryResult.getAll.documents,
        queryResult.getAll.cursor,
        resultMetadata
      );
    }

    if (queryResult.table) {
      return this.convertArrayResult(
        queryResult.table.documents,
        queryResult.table.cursor,
        resultMetadata
      );
    }

    if (queryResult.filter) {
      return this.convertArrayResult(
        queryResult.filter.documents,
        queryResult.filter.cursor,
        resultMetadata
      );
    }

    if (queryResult.orderBy) {
      return this.convertArrayResult(
        queryResult.orderBy.documents,
        queryResult.orderBy.cursor,
        resultMetadata
      );
    }

    if (queryResult.limit) {
      return this.convertArrayResult(
        queryResult.limit.documents,
        queryResult.limit.cursor,
        resultMetadata
      );
    }

    if (queryResult.skip) {
      return this.convertArrayResult(
        queryResult.skip.documents,
        queryResult.skip.cursor,
        resultMetadata
      );
    }

    if (queryResult.pluck) {
      // Handle new PluckResult structure with oneof result
      if (queryResult.pluck.document) {
        // Single document result
        return {
          result: this.convertDatum(queryResult.pluck.document),
          metadata: resultMetadata
        };
      } else if (queryResult.pluck.collection) {
        // Collection result with cursor
        return this.convertArrayResult(
          queryResult.pluck.collection.documents,
          queryResult.pluck.collection.cursor,
          resultMetadata
        );
      } else {
        // Fallback for backwards compatibility
        return this.convertArrayResult([], undefined, resultMetadata);
      }
    }

    if (queryResult.without) {
      // Handle new WithoutResult structure with oneof result
      if (queryResult.without.document) {
        // Single document result
        return {
          result: this.convertDatum(queryResult.without.document),
          metadata: resultMetadata
        };
      } else if (queryResult.without.collection) {
        // Collection result with cursor
        return this.convertArrayResult(
          queryResult.without.collection.documents,
          queryResult.without.collection.cursor,
          resultMetadata
        );
      } else {
        // Fallback for backwards compatibility
        return this.convertArrayResult([], undefined, resultMetadata);
      }
    }

    // Handle operation results
    if (queryResult.insert) {
      return {
        result: {
          inserted: parseInt(queryResult.insert.inserted, 10),
          generatedKeys:
            queryResult.insert.generatedKeys?.map((key) => this.convertDatum(key)) || []
        },
        metadata: resultMetadata
      };
    }

    if (queryResult.delete) {
      return {
        result: { deleted: parseInt(queryResult.delete.deleted, 10) },
        metadata: resultMetadata
      };
    }

    if (queryResult.update) {
      return {
        result: { updated: parseInt(queryResult.update.updated, 10) },
        metadata: resultMetadata
      };
    }

    // Handle database operations
    if (queryResult.databaseCreate) {
      return {
        result: { created: parseInt(queryResult.databaseCreate.created, 10) },
        metadata: resultMetadata
      };
    }

    if (queryResult.databaseDrop) {
      return {
        result: { dropped: parseInt(queryResult.databaseDrop.dropped, 10) },
        metadata: resultMetadata
      };
    }

    if (queryResult.databaseList) {
      return {
        items: queryResult.databaseList.databases,
        cursor: queryResult.databaseList.cursor
          ? {
              startKey: queryResult.databaseList.cursor.startKey,
              batchSize: queryResult.databaseList.cursor.batchSize
            }
          : undefined,
        metadata: resultMetadata
      };
    }

    // Handle table operations
    if (queryResult.tableCreate) {
      return {
        result: { created: parseInt(queryResult.tableCreate.created, 10) },
        metadata: resultMetadata
      };
    }

    if (queryResult.tableDrop) {
      return {
        result: { dropped: parseInt(queryResult.tableDrop.dropped, 10) },
        metadata: resultMetadata
      };
    }

    if (queryResult.tableList) {
      return {
        items: queryResult.tableList.tables,
        cursor: queryResult.tableList.cursor
          ? {
              startKey: queryResult.tableList.cursor.startKey,
              batchSize: queryResult.tableList.cursor.batchSize
            }
          : undefined,
        metadata: resultMetadata
      };
    }

    throw new Error('Invalid query result format');
  }

  private convertArrayResult(
    documents: Datum[],
    cursor: Cursor | undefined,
    metadata: ResponseMetadata | undefined
  ): CursorResult {
    const items = documents?.map((item) => this.convertDatum(item)) || [];
    const convertedCursor = cursor
      ? {
          startKey: cursor.startKey,
          batchSize: cursor.batchSize
        }
      : undefined;

    return {
      items,
      cursor: convertedCursor,
      metadata
    };
  }

  private convertDatum(datum: Datum | undefined): unknown {
    if (!datum) {
      return null;
    }

    if (datum.null !== undefined) {
      return null;
    }

    if (datum.bool !== undefined) {
      return datum.bool;
    }

    if (datum.int !== undefined) {
      return parseInt(datum.int, 10);
    }

    if (datum.float !== undefined) {
      return datum.float;
    }

    if (datum.string !== undefined) {
      return datum.string;
    }

    if (datum.binary !== undefined) {
      return datum.binary;
    }

    if (datum.object) {
      const result: Record<string, unknown> = {};
      for (const [key, value] of Object.entries(datum.object.fields || {})) {
        result[key] = this.convertDatum(value);
      }
      return result;
    }

    if (datum.array) {
      return datum.array.items?.map((item: Datum) => this.convertDatum(item)) || [];
    }

    return null;
  }

  private createRuloError(errorInfo: ErrorInfo): RuloError {
    const message = errorInfo.message || `Error ${errorInfo.code}: ${errorInfo.type || 'Unknown'}`;
    const error = new Error(message) as RuloError;
    error.name = 'RuloError';
    error.code = errorInfo.code;
    error.type = errorInfo.type;
    error.line = errorInfo.line;
    error.column = errorInfo.column;
    return error;
  }
}
