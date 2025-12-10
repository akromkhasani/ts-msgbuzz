import amqp, {
    Channel,
    ChannelModel as Connection,
    ConsumeMessage,
    GetMessage,
    Options,
} from 'amqplib'
import { ConsumerConfirm, MessageBus, MessageCallback } from './generic'
import { logger, sleep } from './utils'

export class RabbitMqMessageBus implements MessageBus {
    private readonly connParams: string | Options.Connect
    private _conn: Connection | null = null
    private readonly _subscribers = new Map<string, SubscriberInfo>()
    private readonly _subscribers2 = new Map<string, SubscriberInfo>()
    private readonly _consumers: RabbitMqConsumer[] = []

    constructor(connParams: string | Options.Connect) {
        this.connParams = connParams
    }

    public async close(): Promise<void> {
        logger.info('Closing message bus')
        if (this._conn) {
            await this._conn.close()
            this._conn = null
        }
    }

    private async getConnection(): Promise<Connection> {
        if (this._conn === null) {
            // amqplib doesn't have an `is_closed` property, but trying to connect again is safe.
            // The library handles reconnects internally in some cases, but for a robust system,
            // you might want a more advanced reconnect strategy (e.g., with exponential backoff).
            this._conn = await amqp.connect(this.connParams)
            this._conn.on('error', (err) => {
                logger.error('RabbitMQ connection error', err)
                this._conn = null // Reset connection on error
            })
            this._conn.on('close', () => {
                logger.info('RabbitMQ connection closed')
                this._conn = null // Reset connection on close
            })
        }
        return this._conn
    }

    async publish(
        topicName: string,
        message: Buffer,
        options: {
            priority?: number
            persistent?: boolean
            createExchange?: boolean
        } = {}
    ): Promise<void> {
        const {
            priority = 0,
            persistent = false,
            createExchange = false,
        } = options
        let chan: Channel | null = null
        try {
            const conn = await this.getConnection()
            chan = await conn.createChannel()

            const exchangeName = new RabbitMqQueueNameGenerator(
                topicName,
                ''
            ).exchangeName()
            if (createExchange) {
                await chan.assertExchange(exchangeName, 'fanout', {
                    durable: true,
                })
            }

            chan.publish(exchangeName, '', message, {
                priority,
                deliveryMode: persistent ? 2 : 1,
            })
        } catch (err) {
            logger.error(`Failed to publish message to topic ${topicName}`, err)
            // Re-throw to allow caller to handle the failure
            throw err
        } finally {
            if (chan) {
                await chan.close()
            }
        }
    }

    /**
     * For workers that need short time to process the message, e.g. less than 1 minute.
     * @param topicName - string
     * @param clientGroup - string
     * @param callback - callback fn
     * @param options - workers count and priority
     */
    on(
        topicName: string,
        clientGroup: string,
        callback: MessageCallback,
        options: { workers?: number; maxPriority?: number } = {}
    ): void {
        const { workers = 1, maxPriority } = options
        this._subscribers.set(topicName, {
            clientGroup,
            callback,
            workers,
            maxPriority,
        })
    }

    /**
     * For workers that need long time to process the message, e.g. more than 1 minute.
     * @param topicName - string
     * @param clientGroup - string
     * @param callback - callback fn
     * @param options - workers count and priority
     */
    on2(
        topicName: string,
        clientGroup: string,
        callback: MessageCallback,
        options: { workers?: number; maxPriority?: number } = {}
    ): void {
        const { workers = 1, maxPriority } = options
        this._subscribers2.set(topicName, {
            clientGroup,
            callback,
            workers,
            maxPriority,
        })
    }

    async startConsuming(): Promise<void> {
        if (this._subscribers.size === 0 && this._subscribers2.size === 0) {
            return
        }

        const promises: Promise<void>[] = []
        for (const [topicName, info] of this._subscribers.entries()) {
            for (let i = 0; i < info.workers; i++) {
                const consumer = new RabbitMqConsumer(
                    this.connParams,
                    topicName,
                    info.clientGroup,
                    info.callback,
                    info.maxPriority
                )
                this._consumers.push(consumer)
                promises.push(consumer.run())
            }
        }
        for (const [topicName, info] of this._subscribers2.entries()) {
            for (let i = 0; i < info.workers; i++) {
                const consumer = new RabbitMqConsumer(
                    this.connParams,
                    topicName,
                    info.clientGroup,
                    info.callback,
                    info.maxPriority
                )
                this._consumers.push(consumer)
                promises.push(consumer.run2())
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
    clientGroup: string
    callback: MessageCallback
    workers: number
    maxPriority?: number
}

class BaseConsumer {
    protected readonly connParams: string | Options.Connect
    protected readonly callback: MessageCallback
    protected readonly maxPriority?: number
    protected readonly nameGenerator: RabbitMqQueueNameGenerator
    protected conn: Connection | null = null

    constructor(
        connParams: string | Options.Connect,
        topicName: string,
        clientGroup: string,
        callback: MessageCallback,
        maxPriority?: number
    ) {
        this.connParams = connParams
        this.callback = callback
        this.maxPriority = maxPriority
        this.nameGenerator = new RabbitMqQueueNameGenerator(
            topicName,
            clientGroup
        )
    }

    public async stop(): Promise<void> {
        if (this.conn) {
            await this.conn.close()
            this.conn = null
        }
        logger.info('Consumer stopped')
    }

    protected async registerQueues(channel: Channel): Promise<void> {
        const qNames = this.nameGenerator

        // 1. Create DLX exchange and queue
        await channel.assertExchange(qNames.dlxExchange(), 'direct', {
            durable: true,
        })
        await channel.assertQueue(qNames.dlxQueueName(), { durable: true })
        await channel.bindQueue(
            qNames.dlxQueueName(),
            qNames.dlxExchange(),
            qNames.dlxQueueName()
        ) // Use queue name as routing key

        // 2. Create main fanout exchange for pub/sub
        await channel.assertExchange(qNames.exchangeName(), 'fanout', {
            durable: true,
        })

        // 3. Create the consumer's queue
        const queueArgs: any = {
            'x-dead-letter-exchange': qNames.dlxExchange(),
            'x-dead-letter-routing-key': qNames.dlxQueueName(),
        }
        if (this.maxPriority !== undefined) {
            queueArgs['x-max-priority'] = this.maxPriority
        }
        await channel.assertQueue(qNames.queueName(), {
            durable: true,
            arguments: queueArgs,
        })

        // 4. Bind the consumer's queue to the main exchange
        await channel.bindQueue(
            qNames.queueName(),
            qNames.exchangeName(),
            qNames.queueName()
        )

        // 5. Create retry queue and bind it for dead-lettering back to the main queue
        await channel.assertExchange(qNames.retryExchange(), 'direct', {
            durable: true,
        })
        await channel.bindQueue(
            qNames.queueName(),
            qNames.retryExchange(),
            qNames.queueName()
        )
        await channel.assertQueue(qNames.retryQueueName(), {
            durable: true,
            arguments: {
                'x-dead-letter-exchange': qNames.retryExchange(),
                'x-dead-letter-routing-key': qNames.queueName(),
            },
        })
    }

    protected messageExpired(msg: ConsumeMessage | GetMessage): boolean {
        const headers = msg.properties.headers || {}
        const xDeath = headers['x-death'] as any[]
        const maxRetries = headers['x-max-retries'] as number

        if (xDeath && typeof maxRetries === 'number') {
            // Find the death count for our retry queue
            const retryDeath = xDeath.find(
                (d) => d.queue === this.nameGenerator.retryQueueName()
            )
            if (retryDeath) {
                return retryDeath.count >= maxRetries
            }
        }
        return false
    }
}

class RabbitMqConsumer extends BaseConsumer {
    /**
     * Run in consume-style
     */
    public async run(): Promise<void> {
        try {
            this.conn = await amqp.connect(this.connParams)
            const chan = await this.conn.createChannel()

            await this.registerQueues(chan)

            // Set prefetch count (QoS) to 1 to ensure a worker only gets one message at a time.
            chan.prefetch(1)

            logger.info(
                `Waiting for messages on topic: ${this.nameGenerator.exchangeName()}. To exit press Ctrl+C`
            )

            const wrappedCallback = async (msg: ConsumeMessage | null) => {
                if (!msg) return

                if (this.messageExpired(msg)) {
                    logger.warn(
                        `Message exceeded max retries. Nacking message ${msg.fields.deliveryTag}`
                    )
                    return chan.nack(msg, false, false) // Send to DLX
                }

                const confirm = new RabbitMqConsumerConfirm(
                    this.nameGenerator,
                    chan,
                    msg
                )
                await this.callback(confirm, msg.content)
            }
            await chan.consume(
                this.nameGenerator.queueName(),
                wrappedCallback,
                {
                    noAck: false, // Manual acknowledgment
                }
            )
        } catch (err) {
            logger.error(err)
            await this.stop()
        }
    }

    /**
     * Run in manual-get-style
     */
    public async run2(): Promise<void> {
        try {
            this.conn = await amqp.connect(this.connParams)
            const chan = await this.conn.createChannel()

            await this.registerQueues(chan)

            logger.info(
                `Waiting for messages on topic: ${this.nameGenerator.exchangeName()}. To exit press Ctrl+C`
            )

            while (true) {
                const msg = await chan.get(this.nameGenerator.queueName(), {
                    noAck: false, // Manual acknowledgment
                })
                if (!msg) {
                    await sleep(1)
                    continue
                }

                if (this.messageExpired(msg)) {
                    logger.warn(
                        `Message exceeded max retries. Nacking message ${msg.fields.deliveryTag}`
                    )
                    chan.nack(msg, false, false) // Send to DLX
                    continue
                }

                const confirm = new RabbitMqConsumerConfirm(
                    this.nameGenerator,
                    chan,
                    msg
                )
                await this.callback(confirm, msg.content)
            }
        } catch (err) {
            logger.error(err)
            await this.stop()
        }
    }
}

class RabbitMqQueueNameGenerator {
    private readonly _topicName: string
    private readonly _clientGroup: string

    constructor(topicName: string, clientGroup: string) {
        this._topicName = topicName
        this._clientGroup = clientGroup
    }

    exchangeName(): string {
        return this._topicName
    }

    queueName(): string {
        return `${this._topicName}.${this._clientGroup}`
    }

    retryExchange(): string {
        return this.retryQueueName()
    }

    retryQueueName(): string {
        return `${this.queueName()}__retry`
    }

    dlxExchange(): string {
        return this.dlxQueueName()
    }

    dlxQueueName(): string {
        return `${this.queueName()}__failed`
    }
}

class RabbitMqConsumerConfirm implements ConsumerConfirm {
    constructor(
        private readonly namesGen: RabbitMqQueueNameGenerator,
        private readonly channel: Channel,
        private readonly msg: ConsumeMessage | GetMessage
    ) {}

    async ack(): Promise<void> {
        this.channel.ack(this.msg)
    }

    async nack(): Promise<void> {
        // requeue=false sends it to the DLX if configured
        this.channel.nack(this.msg, false, false)
    }

    async retry(delay: number = 60000, maxRetries: number = 3): Promise<void> {
        try {
            const properties: Options.Publish = {
                ...this.msg.properties,
                expiration: String(delay),
                headers: {
                    ...this.msg.properties.headers,
                    'x-max-retries': maxRetries,
                },
            }

            // Publish to the retry queue which will dead-letter back to the main queue after `delay`
            this.channel.publish(
                '', // default exchange
                this.namesGen.retryQueueName(),
                this.msg.content,
                properties
            )

            // Ack the original message
            await this.ack()
        } catch (err) {
            logger.error('Failed to retry message', err)
            // If retry fails, nack the message to send it to DLX
            await this.nack()
        }
    }
}
