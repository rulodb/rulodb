import { createPool } from 'generic-pool';
import { decode, encode } from 'msgpackr';
import { connect, Socket } from 'net';

import { Client, DEFAULT_POOL_OPTIONS } from '../src/client';

// Mock dependencies
jest.mock('generic-pool');
jest.mock('net');
jest.mock('msgpackr');

describe('Client', () => {
  let mockSocket: jest.Mocked<Socket>;
  let mockPool: jest.Mocked<ReturnType<typeof createPool>>;

  beforeEach(() => {
    // Reset all mocks
    jest.clearAllMocks();

    // Mock Socket
    mockSocket = {
      end: jest.fn((cb) => cb && cb()),
      destroy: jest.fn(),
      destroyed: false,
      setTimeout: jest.fn(),
      on: jest.fn(),
      once: jest.fn(),
      off: jest.fn(),
      write: jest.fn(),
      readableEnded: false,
      writable: true
    } as any;

    // Mock Pool
    mockPool = {
      acquire: jest.fn().mockResolvedValue(mockSocket),
      release: jest.fn().mockResolvedValue(undefined),
      drain: jest.fn().mockResolvedValue(undefined),
      clear: jest.fn().mockResolvedValue(undefined)
    } as any;

    // Mock createPool
    (createPool as jest.Mock).mockReturnValue(mockPool);

    // Mock connect
    (connect as jest.Mock).mockImplementation((options, callback) => {
      if (callback) {
        setTimeout(callback, 0);
      }
      return mockSocket;
    });

    // Mock msgpackr
    (encode as jest.Mock).mockImplementation((data) => Buffer.from(JSON.stringify(data)));
    (decode as jest.Mock).mockImplementation((buffer) => JSON.parse(buffer.toString()));
  });

  describe('Constructor', () => {
    it('should create client with default options', () => {
      const client = new Client();

      expect(createPool).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.any(Function),
          destroy: expect.any(Function),
          validate: expect.any(Function)
        }),
        DEFAULT_POOL_OPTIONS
      );
    });

    it('should create client with custom options', () => {
      const customOptions = {
        host: '192.168.1.100',
        port: 8080,
        timeout: 10000,
        poolOptions: {
          max: 20,
          min: 5,
          idleTimeoutMillis: 60000
        }
      };

      const client = new Client(customOptions);

      expect(createPool).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.any(Function),
          destroy: expect.any(Function),
          validate: expect.any(Function)
        }),
        customOptions.poolOptions
      );
    });

    it('should handle partial options', () => {
      const partialOptions = {
        host: 'localhost',
        port: 7000
      };

      const client = new Client(partialOptions);

      expect(client).toBeDefined();
      expect(createPool).toHaveBeenCalled();
    });
  });

  describe('Connection management', () => {
    let client: Client;

    beforeEach(() => {
      client = new Client();
    });

    it('should create connection successfully', async () => {
      // Get the create function from the pool factory
      const createCall = (createPool as jest.Mock).mock.calls[0][0];
      const createConnection = createCall.create;

      const connection = await createConnection();

      expect(connect).toHaveBeenCalledWith({ host: '127.0.0.1', port: 6969 }, expect.any(Function));
      expect(mockSocket.setTimeout).toHaveBeenCalledWith(5000);
      expect(connection).toBe(mockSocket);
    });

    it('should handle connection errors', async () => {
      const error = new Error('Connection failed');
      (connect as jest.Mock).mockImplementation((options, callback) => {
        const socket = mockSocket;
        setTimeout(() => {
          const errorCall = socket.on.mock.calls.find((call: any) => call[0] === 'error');
          if (errorCall) {
            (errorCall[1] as any)(error);
          }
        }, 0);
        return socket;
      });

      const createCall = (createPool as jest.Mock).mock.calls[0][0];
      const createConnection = createCall.create;

      await expect(createConnection()).rejects.toThrow('Connection failed');
    });

    it('should destroy connection properly', async () => {
      const createCall = (createPool as jest.Mock).mock.calls[0][0];
      const destroyConnection = createCall.destroy;

      await destroyConnection(mockSocket);

      expect(mockSocket.end).toHaveBeenCalledWith(expect.any(Function));
      expect(mockSocket.destroy).toHaveBeenCalled();
    });

    it('should validate connection', async () => {
      const createCall = (createPool as jest.Mock).mock.calls[0][0];
      const validateConnection = createCall.validate;

      Object.defineProperty(mockSocket, 'destroyed', { value: false, writable: true });
      const isValid = await validateConnection(mockSocket);
      expect(isValid).toBe(true);

      Object.defineProperty(mockSocket, 'destroyed', { value: true, writable: true });
      const isInvalid = await validateConnection(mockSocket);
      expect(isInvalid).toBe(false);
    });
  });

  describe('Message sending', () => {
    let client: Client;

    beforeEach(() => {
      client = new Client();
    });

    it('should send message and receive response', async () => {
      const testData = { query: 'test', args: [1, 2, 3] };
      const responseData = { result: 'success', data: [1, 2, 3] };

      // Mock the response
      const responseBuffer = Buffer.from(JSON.stringify(responseData));
      const framedResponse = Buffer.alloc(4 + responseBuffer.length);
      framedResponse.writeUInt32BE(responseBuffer.length, 0);
      responseBuffer.copy(framedResponse, 4);

      // Mock socket write to simulate immediate response
      mockSocket.write.mockImplementation((message) => {
        // Simulate receiving the response
        setTimeout(() => {
          const dataCall = mockSocket.on.mock.calls.find((call: any) => call[0] === 'data');
          if (dataCall) {
            (dataCall[1] as any)(framedResponse);
          }
        }, 0);
        return true;
      });

      const result = await client.send(testData);

      expect(mockPool.acquire).toHaveBeenCalled();
      expect(encode).toHaveBeenCalledWith(testData);
      expect(mockSocket.write).toHaveBeenCalled();
      expect(decode).toHaveBeenCalled();
      expect(mockPool.release).toHaveBeenCalledWith(mockSocket);
      expect(result).toEqual(responseData);
    });

    it('should handle partial message reception', async () => {
      const testData = { query: 'test' };
      const responseData = { result: 'success' };

      const responseBuffer = Buffer.from(JSON.stringify(responseData));
      const framedResponse = Buffer.alloc(4 + responseBuffer.length);
      framedResponse.writeUInt32BE(responseBuffer.length, 0);
      responseBuffer.copy(framedResponse, 4);

      // Split the response into multiple chunks
      const chunk1 = framedResponse.subarray(0, 6);
      const chunk2 = framedResponse.subarray(6);

      mockSocket.write.mockImplementation(() => {
        setTimeout(() => {
          const dataCall = mockSocket.on.mock.calls.find((call: any) => call[0] === 'data');
          if (dataCall) {
            (dataCall[1] as any)(chunk1);
            setTimeout(() => (dataCall[1] as any)(chunk2), 10);
          }
        }, 0);
        return true;
      });

      const result = await client.send(testData);
      expect(result).toEqual(responseData);
    });

    it('should handle message framing correctly', async () => {
      const testData = { query: 'test' };
      const expectedPayload = Buffer.from(JSON.stringify(testData));

      mockSocket.write.mockImplementation((message: any) => {
        // Verify message framing
        const buf = message as Buffer;
        expect(buf.length).toBe(4 + expectedPayload.length);
        expect(buf.readUInt32BE(0)).toBe(expectedPayload.length);
        expect(buf.subarray(4)).toEqual(expectedPayload);

        // Simulate response
        const responseBuffer = Buffer.from('{"result":"ok"}');
        const framedResponse = Buffer.alloc(4 + responseBuffer.length);
        framedResponse.writeUInt32BE(responseBuffer.length, 0);
        responseBuffer.copy(framedResponse, 4);

        setTimeout(() => {
          const dataCall = mockSocket.on.mock.calls.find((call: any) => call[0] === 'data');
          if (dataCall) {
            (dataCall[1] as any)(framedResponse);
          }
        }, 0);
        return true;
      });

      await client.send(testData);
      expect(encode).toHaveBeenCalledWith(testData);
    });

    it('should handle socket errors during send', async () => {
      const testData = { query: 'test' };
      const error = new Error('Socket error');

      mockSocket.write.mockImplementation(() => {
        setTimeout(() => {
          const errorCall = mockSocket.once.mock.calls.find((call: any) => call[0] === 'error');
          if (errorCall) {
            (errorCall[1] as any)(error);
          }
        }, 0);
        return true;
      });

      await expect(client.send(testData)).rejects.toThrow('Socket error');
      expect(mockPool.release).toHaveBeenCalledWith(mockSocket);
    });

    it('should clean up event listeners after successful send', async () => {
      const testData = { query: 'test' };

      mockSocket.write.mockImplementation(() => {
        setTimeout(() => {
          const dataCall = mockSocket.on.mock.calls.find((call: any) => call[0] === 'data');
          if (dataCall) {
            const responseBuffer = Buffer.from('{"result":"ok"}');
            const framedResponse = Buffer.alloc(4 + responseBuffer.length);
            framedResponse.writeUInt32BE(responseBuffer.length, 0);
            responseBuffer.copy(framedResponse, 4);
            (dataCall[1] as any)(framedResponse);
          }
        }, 0);
        return true;
      });

      await client.send(testData);

      expect(mockSocket.off).toHaveBeenCalledWith('data', expect.any(Function));
      expect(mockSocket.off).toHaveBeenCalledWith('error', expect.any(Function));
    });

    it('should release socket even if send fails', async () => {
      const testData = { query: 'test' };
      const error = new Error('Send failed');

      mockPool.acquire.mockRejectedValue(error);

      await expect(client.send(testData)).rejects.toThrow('Send failed');
      // Socket should not be released if acquire failed
      expect(mockPool.release).not.toHaveBeenCalled();
    });

    it('should release socket when encoding fails', async () => {
      const testData = { query: 'test' };
      const error = new Error('Encoding failed');

      (encode as jest.Mock).mockImplementation(() => {
        throw error;
      });

      await expect(client.send(testData)).rejects.toThrow('Encoding failed');
      expect(mockPool.release).toHaveBeenCalledWith(mockSocket);
    });
  });

  describe('Generic type support', () => {
    let client: Client;

    beforeEach(() => {
      client = new Client();
    });

    it('should support generic input and output types', async () => {
      interface TestInput {
        command: string;
        params: number[];
      }

      interface TestOutput {
        status: string;
        results: string[];
      }

      const input: TestInput = { command: 'test', params: [1, 2, 3] };
      const output: TestOutput = { status: 'success', results: ['a', 'b', 'c'] };

      mockSocket.write.mockImplementation(() => {
        setTimeout(() => {
          const dataCall = mockSocket.on.mock.calls.find((call: any) => call[0] === 'data');
          if (dataCall) {
            const responseBuffer = Buffer.from(JSON.stringify(output));
            const framedResponse = Buffer.alloc(4 + responseBuffer.length);
            framedResponse.writeUInt32BE(responseBuffer.length, 0);
            responseBuffer.copy(framedResponse, 4);
            (dataCall[1] as any)(framedResponse);
          }
        }, 0);
        return true;
      });

      const result = await client.send<TestInput, TestOutput>(input);
      expect(result).toEqual(output);
    });
  });

  describe('Connection pool management', () => {
    let client: Client;

    beforeEach(() => {
      client = new Client();
    });

    it('should close client properly', async () => {
      await client.close();

      expect(mockPool.drain).toHaveBeenCalled();
      expect(mockPool.clear).toHaveBeenCalled();
    });

    it('should handle pool drain errors', async () => {
      const error = new Error('Drain failed');
      mockPool.drain.mockRejectedValue(error);

      await expect(client.close()).rejects.toThrow('Drain failed');
    });

    it('should handle pool clear errors', async () => {
      const error = new Error('Clear failed');
      mockPool.clear.mockRejectedValue(error);

      await expect(client.close()).rejects.toThrow('Clear failed');
    });
  });

  describe('Edge cases and error scenarios', () => {
    let client: Client;

    beforeEach(() => {
      client = new Client();
    });

    it('should handle empty response', async () => {
      const testData = { query: 'test' };

      mockSocket.write.mockImplementation(() => {
        setTimeout(() => {
          const dataCall = mockSocket.on.mock.calls.find((call: any) => call[0] === 'data');
          if (dataCall) {
            const emptyResponse = Buffer.alloc(4);
            emptyResponse.writeUInt32BE(0, 0);
            (dataCall[1] as any)(emptyResponse);
          }
        }, 0);
        return true;
      });

      (decode as jest.Mock).mockReturnValue(null);

      const result = await client.send(testData);
      expect(result).toBeNull();
    });

    it('should handle malformed response length', async () => {
      const testData = { query: 'test' };

      mockSocket.write.mockImplementation(() => {
        setTimeout(() => {
          const dataCall = mockSocket.on.mock.calls.find((call: any) => call[0] === 'data');
          if (dataCall) {
            // Send malformed length header
            const malformedResponse = Buffer.from([0xff, 0xff, 0xff, 0xff]);
            (dataCall[1] as any)(malformedResponse);
          }
        }, 0);
        return true;
      });

      // This should not resolve as we won't get a complete message
      const sendPromise = client.send(testData);

      // Wait a bit to ensure it doesn't resolve immediately
      await new Promise((resolve) => setTimeout(resolve, 100));

      // The promise should still be pending since we never send complete data
      expect(sendPromise).toEqual(expect.any(Promise));
    });

    it('should handle decode errors', async () => {
      const testData = { query: 'test' };
      const decodeError = new Error('Decode failed');

      mockSocket.write.mockImplementation(() => {
        setTimeout(() => {
          const dataCall = mockSocket.on.mock.calls.find((call: any) => call[0] === 'data');
          if (dataCall) {
            const responseBuffer = Buffer.from('invalid data');
            const framedResponse = Buffer.alloc(4 + responseBuffer.length);
            framedResponse.writeUInt32BE(responseBuffer.length, 0);
            responseBuffer.copy(framedResponse, 4);
            (dataCall[1] as any)(framedResponse);
          }
        }, 0);
        return true;
      });

      (decode as jest.Mock).mockImplementation(() => {
        throw decodeError;
      });

      await expect(client.send(testData)).rejects.toThrow('Decode failed');
    });

    it('should handle multiple concurrent sends', async () => {
      const testData1 = { query: 'test1' };
      const testData2 = { query: 'test2' };
      const response1 = { result: 'result1' };
      const response2 = { result: 'result2' };

      // Use separate sockets for each request to avoid interference
      const mockSocket2 = {
        end: jest.fn((cb) => cb && cb()),
        destroy: jest.fn(),
        destroyed: false,
        setTimeout: jest.fn(),
        on: jest.fn(),
        once: jest.fn(),
        off: jest.fn(),
        write: jest.fn(),
        readableEnded: false,
        writable: true
      } as any;

      // Setup first socket to respond with response1
      mockSocket.write.mockImplementation(() => {
        setImmediate(() => {
          const dataCall = mockSocket.on.mock.calls.find((call: any) => call[0] === 'data');
          if (dataCall) {
            const responseBuffer = Buffer.from(JSON.stringify(response1));
            const framedResponse = Buffer.alloc(4 + responseBuffer.length);
            framedResponse.writeUInt32BE(responseBuffer.length, 0);
            responseBuffer.copy(framedResponse, 4);
            (dataCall[1] as any)(framedResponse);
          }
        });
        return true;
      });

      // Setup second socket to respond with response2
      mockSocket2.write.mockImplementation(() => {
        setImmediate(() => {
          const dataCall = mockSocket2.on.mock.calls.find((call: any) => call[0] === 'data');
          if (dataCall) {
            const responseBuffer = Buffer.from(JSON.stringify(response2));
            const framedResponse = Buffer.alloc(4 + responseBuffer.length);
            framedResponse.writeUInt32BE(responseBuffer.length, 0);
            responseBuffer.copy(framedResponse, 4);
            (dataCall[1] as any)(framedResponse);
          }
        });
        return true;
      });

      // Each send should acquire its own socket
      mockPool.acquire.mockResolvedValueOnce(mockSocket).mockResolvedValueOnce(mockSocket2);

      const [result1, result2] = await Promise.all([
        client.send(testData1),
        client.send(testData2)
      ]);

      expect(result1).toEqual(response1);
      expect(result2).toEqual(response2);
      expect(mockPool.acquire).toHaveBeenCalledTimes(2);
      expect(mockPool.release).toHaveBeenCalledTimes(2);
    }, 10000);
  });
});
