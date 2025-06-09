import { createPool, Options as PoolOptions, Pool } from 'generic-pool';
import { decode, encode } from 'msgpackr';
import net from 'net';

export interface ClientOptions {
  host: string;
  port: number;
  poolOptions?: PoolOptions;
  timeout?: number;
}

export class Client {
  private pool: Pool<net.Socket>;
  private host: string;
  private port: number;
  private timeout: number;

  constructor({
    host = '127.0.0.1',
    port = 6969,
    poolOptions = undefined,
    timeout = 5000
  }: ClientOptions) {
    this.host = host;
    this.port = port;
    this.timeout = timeout;

    this.pool = createPool<net.Socket>(
      {
        create: () => this.createConnection(),
        destroy: (socket) => this.destroyConnection(socket),
        validate: (socket) => this.validateConnection(socket)
      },
      {
        max: 10,
        min: 2,
        idleTimeoutMillis: 30000,
        ...(poolOptions || {})
      }
    );
  }

  private createConnection(): Promise<net.Socket> {
    return new Promise((resolve, reject) => {
      const socket = net.connect({ host: this.host, port: this.port }, () => {
        socket.setTimeout(this.timeout);
        resolve(socket);
      });
      socket.on('error', reject);
    });
  }

  private destroyConnection(socket: net.Socket): Promise<void> {
    return new Promise((resolve) => {
      socket.end(() => {
        socket.destroy();
        resolve();
      });
    });
  }

  private validateConnection(socket: net.Socket): Promise<boolean> {
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

  private sendMessage(socket: net.Socket, message: Buffer): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      let buffer = Buffer.alloc(0);

      const onData = (chunk: Buffer) => {
        buffer = Buffer.concat([buffer, chunk]);

        if (buffer.length >= 4) {
          const expectedLength = buffer.readUInt32BE(0);
          if (buffer.length >= 4 + expectedLength) {
            const messageBuffer = buffer.slice(4, 4 + expectedLength);
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

export async function connect(...args: ConstructorParameters<typeof Client>): Promise<Client> {
  return new Client(...args);
}
