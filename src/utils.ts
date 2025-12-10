const logLevel = process.env.LOG_LEVEL || 'debug'
const LOG_LEVELS = ['debug', 'info', 'warn', 'error']
let currentLogLevelIndex = LOG_LEVELS.indexOf(logLevel)
if (currentLogLevelIndex === -1) currentLogLevelIndex = 0

function shouldLog(level: string): boolean {
    return LOG_LEVELS.indexOf(level) >= currentLogLevelIndex
}

/**
 * Simple console logger.
 */
export const logger = {
    debug: (...args: any[]) => {
        if (shouldLog('debug'))
            console.debug(new Date().toISOString(), 'DEBUG', ...args)
    },
    info: (...args: any[]) => {
        if (shouldLog('info'))
            console.info(new Date().toISOString(), 'INFO ', ...args)
    },
    warn: (...args: any[]) => {
        if (shouldLog('warn'))
            console.warn(new Date().toISOString(), 'WARN ', ...args)
    },
    error: (...args: any[]) => {
        if (shouldLog('error'))
            console.error(new Date().toISOString(), 'ERROR', ...args)
    },
}

/**
 * Sleep for n seconds.
 * @param n - Number in seconds
 */
export async function sleep(n: number) {
    await new Promise((resolve) => setTimeout(resolve, n * 1000))
}
