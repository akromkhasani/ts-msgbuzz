import type { SupabaseClient } from '@supabase/supabase-js'
import {
    ConsumerConfirm,
    MessageBus,
    MessageCallback,
    NoMessageCallback,
    RetryOptions,
} from './generic'
import { logger, sleep } from './utils'

type Client = SupabaseClient<any, any, 'pgmq_public', any, any>

export class SupabaseMessageBus implements MessageBus {
    private readonly client: Client
    private readonly messageTimeoutSecods: number
    private readonly _subscribers = new Map<string, SubscriberInfo>()
    private readonly _consumers: SupabaseConsumer[] = []

    /**
     * Don't use `new SupabaseMessageBus(...)`, but use `await SupabaseMessageBus.create(...)`
     */
    constructor(
        client: Client,
        kwargs: { messageTimeoutSecods?: number } = {}
    ) {
        this.client = client
        const { messageTimeoutSecods = 600 } = kwargs
        this.messageTimeoutSecods = messageTimeoutSecods
    }

    static async create(
        supabaseUrl: string,
        supabaseKey: string,
        kwargs: { messageTimeoutSecods?: number } = {}
    ): Promise<SupabaseMessageBus> {
        let mod: typeof import('@supabase/supabase-js')

        try {
            mod = await import('@supabase/supabase-js')
        } catch {
            throw new Error(
                'Supabase support requires @supabase/supabase-js.\n' +
                    'Install it with:\n' +
                    '  npm install "@supabase/supabase-js@^2.87.3"'
            )
        }

        const client = mod.createClient(supabaseUrl, supabaseKey, {
            db: { schema: 'pgmq_public' },
        })
        return new SupabaseMessageBus(client, kwargs)
    }

    public async close(): Promise<void> {}

    async publish(
        topicName: string,
        message: Buffer,
        options?: any
    ): Promise<void> {
        const { error } = await this.client.rpc('send', {
            queue_name: topicName,
            message: { _body: message.toString() },
        })
        if (error) {
            throw new Error(error.message)
        }
    }

    /**
     * For workers that need short time to process the message, e.g. less than 1 minute.
     * @param topicName - string
     * @param clientGroup - string (value not used)
     * @param callback - callback fn
     * @param options - workers count, batch size, and check interval
     */
    on(
        topicName: string,
        clientGroup: string,
        callback: MessageCallback,
        options: {
            workers?: number
            batchSize?: number
            checkIntervalSeconds?: number
        } = {}
    ): void {
        const { workers = 1, batchSize = 1, checkIntervalSeconds = 5 } = options
        this._subscribers.set(topicName, {
            callback,
            workers: Math.max(1, workers),
            batchSize: Math.max(1, batchSize),
            checkIntervalSeconds: Math.max(1, checkIntervalSeconds),
        })
    }

    /**
     * For workers that need long time to process the message, e.g. more than 1 minute.
     * @param topicName - string
     * @param clientGroup - string (value not used)
     * @param callback - callback fn
     * @param options - workers count, batch size, check interval, and noMessageCallback
     */
    on2(
        topicName: string,
        clientGroup: string,
        callback: MessageCallback,
        options: {
            workers?: number
            batchSize?: number
            checkIntervalSeconds?: number
            noMessageCallback?: NoMessageCallback
        } = {}
    ): void {
        const {
            workers = 1,
            batchSize = 1,
            checkIntervalSeconds = 5,
            noMessageCallback,
        } = options
        this._subscribers.set(topicName, {
            callback,
            workers: Math.max(1, workers),
            batchSize: Math.max(1, batchSize),
            checkIntervalSeconds: Math.max(1, checkIntervalSeconds),
            noMessageCallback,
        })
    }

    async startConsuming(): Promise<void> {
        if (this._subscribers.size === 0) {
            return
        }

        const promises: Promise<void>[] = []
        for (const [topicName, info] of this._subscribers.entries()) {
            for (let i = 0; i < info.workers; i++) {
                const consumer = new SupabaseConsumer(
                    this.client,
                    topicName,
                    this.messageTimeoutSecods,
                    info.batchSize,
                    info.checkIntervalSeconds,
                    info.callback,
                    info.noMessageCallback
                )
                this._consumers.push(consumer)
                promises.push(consumer.run())
            }
        }

        // In Node.js, we don't need separate processes for I/O-bound tasks.
        // We run all consumers concurrently in the same process using async/await.
        await Promise.all(promises)

        // Graceful shutdown
        const shutdown = async () => {
            logger.warn('Shutting down consumers...')
            await Promise.all(this._consumers.map((c) => c.stop()))
        }

        process.on('SIGINT', shutdown)
        process.on('SIGTERM', shutdown)
    }
}

interface SubscriberInfo {
    callback: MessageCallback
    workers: number
    batchSize: number
    checkIntervalSeconds: number
    noMessageCallback?: NoMessageCallback
}

class SupabaseConsumer {
    protected readonly client: Client
    protected readonly topicName: string
    protected readonly messageTimeoutSeconds: number
    protected readonly batchSize: number
    protected readonly checkIntervalSeconds: number
    protected readonly callback: MessageCallback
    protected readonly noMessageCallback?: NoMessageCallback
    protected readonly breaker: { break: boolean }

    constructor(
        client: Client,
        topicName: string,
        messageTimeoutSeconds: number,
        batchSize: number,
        checkIntervalSeconds: number,
        callback: MessageCallback,
        noMessageCallback?: NoMessageCallback
    ) {
        this.client = client
        this.topicName = topicName
        this.messageTimeoutSeconds = messageTimeoutSeconds
        this.batchSize = batchSize
        this.checkIntervalSeconds = checkIntervalSeconds
        this.callback = callback
        this.noMessageCallback = noMessageCallback
        this.breaker = { break: false }
    }

    public async stop(): Promise<void> {
        this.breaker.break = true
    }

    protected messageExpired(msg: { _headers?: Record<string, any> }): boolean {
        const headers = (msg._headers || {}) as Record<string, any>
        const retryCount = headers['x-retry-count']
        const maxRetries = headers['x-max-retries']

        if (typeof retryCount === 'number' && typeof maxRetries === 'number') {
            return retryCount > maxRetries
        }
        return false
    }

    public async run(): Promise<void> {
        try {
            logger.info(
                `Waiting for messages on topic: ${this.topicName}. To exit press Ctrl+C`
            )
            while (true) {
                if (this.breaker.break) break

                const { error, data } = await this.client.rpc('read', {
                    queue_name: this.topicName,
                    sleep_seconds: this.messageTimeoutSeconds,
                    n: this.batchSize,
                })
                if (error) {
                    throw new Error(error.message)
                }

                if (!Array.isArray(data)) continue

                if (data.length === 0) {
                    if (this.noMessageCallback) {
                        await this.noMessageCallback()
                    }
                    await sleep(this.checkIntervalSeconds)
                    continue
                }

                const promises = []
                for (const item of data) {
                    const { msg_id, message } = item
                    const confirm = new SupabaseConsumerConfirm(
                        this.client,
                        this.topicName,
                        msg_id,
                        message
                    )
                    if (this.messageExpired(message)) {
                        await confirm.nack()
                        continue
                    }
                    promises.push(
                        this.callback(confirm, Buffer.from(message._body))
                    )
                }
                await Promise.all(promises)
            }
        } catch (err) {
            logger.error(err)
            await this.stop()
        }
    }
}

class SupabaseConsumerConfirm implements ConsumerConfirm {
    constructor(
        private readonly client: Client,
        private readonly topicName: string,
        private readonly messageId: number,
        private readonly message: {
            _headers?: Record<string, any>
            _body?: any
        }
    ) {}

    async ack(): Promise<void> {
        const { error } = await this.client.rpc('delete', {
            queue_name: this.topicName,
            message_id: this.messageId,
        })
        if (error) {
            throw new Error(error.message)
        }
    }

    async nack(): Promise<void> {
        const { error } = await this.client.rpc('archive', {
            queue_name: this.topicName,
            message_id: this.messageId,
        })
        if (error) {
            throw new Error(error.message)
        }
    }

    async retry({
        delay = 60000,
        maxRetries = 3,
        ack = true,
    }: RetryOptions = {}): Promise<void> {
        delay = Math.max(delay, 0)
        maxRetries = Math.max(maxRetries, 1)

        const _headers = (this.message._headers || {}) as Record<string, any>
        _headers['x-retry-count'] = (_headers['x-retry-count'] || 0) + 1
        _headers['x-max-retries'] = maxRetries

        try {
            const { error } = await this.client.rpc('send', {
                queue_name: this.topicName,
                message: { _headers, _body: this.message._body },
                sleep_seconds: Math.ceil(delay / 1000),
            })
            if (error) {
                throw new Error(error.message)
            }

            if (ack) {
                await this.ack()
            }
        } catch (err) {
            logger.error('Failed to retry message', err)
            if (ack) {
                // If retry fails, nack the message to send it to DLX
                await this.nack()
            }
        }
    }
}
