import pino from 'pino';

export function createLogger(level = 'info') {
  const logger = pino({
    level,
    transport: {
      target: 'pino-pretty',
      options: {
        colorize: true,
        translateTime: 'SYS:standard',
        ignore: 'pid,hostname'
      }
    }
  });
  return logger;
}
