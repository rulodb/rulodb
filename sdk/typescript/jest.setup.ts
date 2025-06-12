// Jest setup file for RuloDB TypeScript SDK tests
import { afterAll, beforeAll, jest } from '@jest/globals';

// Global test timeout
jest.setTimeout(30000);

// Setup before all tests
beforeAll(async () => {
  // Any global setup can go here
});

// Cleanup after all tests
afterAll(async () => {
  // Any global cleanup can go here
});

// Handle unhandled promise rejections in tests
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

// Handle uncaught exceptions in tests
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
});
