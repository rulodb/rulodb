import { createPool, Options as PoolOptions, Pool } from 'generic-pool';
import { decode, encode } from 'msgpackr';
import { connect, Socket } from 'net';

const DEFAULT_HOST = '127.0.0.1';
const DEFAULT_PORT = 6969;
const DEFAULT_TIMEOUT = 5000;

export const DEFAULT_POOL_OPTIONS: PoolOptions = {
  max: 10,
  min: 2,
  idleTimeoutMillis: 30000
};

export interface ClientOptions {
  host: string;
  port: number;
  poolOptions?: PoolOptions;
  timeout?: number;
}

export class Client {
  private pool: Pool<Socket>;
  private host: string;
  private port: number;
  private timeout: number;

  constructor(
    options: ClientOptions | undefined = {
      host: DEFAULT_HOST,
      port: DEFAULT_PORT,
      poolOptions: DEFAULT_POOL_OPTIONS,
      timeout: DEFAULT_TIMEOUT
    }
  ) {
    this.host = options.host;
    this.port = options.port;
    this.timeout = options.timeout || DEFAULT_TIMEOUT;

    this.pool = createPool<Socket>(
      {
        create: () => this.createConnection(),
        destroy: (socket) => this.destroyConnection(socket),
        validate: (socket) => this.validateConnection(socket)
      },
      options.poolOptions
    );
  }

  private createConnection(): Promise<Socket> {
    return new Promise((resolve, reject) => {
      const socket = connect({ host: this.host, port: this.port }, () => {
        socket.setTimeout(this.timeout);
        resolve(socket);
      });
      socket.on('error', reject);
    });
  }

  private destroyConnection(socket: Socket): Promise<void> {
    return new Promise((resolve) => {
      socket.end(() => {
        socket.destroy();
        resolve();
      });
    });
  }

  private validateConnection(socket: Socket): Promise<boolean> {
    return Promise.resolve(!socket.destroyed);
  }

  public async send<T = unknown, R = unknown>(data: T): Promise<R> {
    const socket = await this.pool.acquire();

    try {
      const payload = encode(data);
      const framed = Buffer.alloc(4 + payload.length);
      framed.writeUInt32BE(payload.length, 0);
      payload.copy(framed, 4);

      const response = await this.sendMessage(socket, framed);
      return decode(response);
    } finally {
      this.pool.release(socket);
    }
  }

  private sendMessage(socket: Socket, message: Buffer): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      let buffer = Buffer.alloc(0);

      const onData = (chunk: Buffer) => {
        buffer = Buffer.concat([buffer, chunk]);

        if (buffer.length >= 4) {
          const expectedLength = buffer.readUInt32BE(0);

          if (buffer.length >= 4 + expectedLength) {
            const messageBuffer: Buffer = buffer.subarray(4, 4 + expectedLength);

            cleanup();
            resolve(messageBuffer);
          }
        }
      };

      const onError = (err: Error) => {
        cleanup();
        reject(err);
      };

      const cleanup = () => {
        socket.off('data', onData);
        socket.off('error', onError);
      };

      socket.on('data', onData);
      socket.once('error', onError);
      socket.write(message);
    });
  }

  public async close(): Promise<void> {
    await this.pool.drain();
    await this.pool.clear();
  }
}
