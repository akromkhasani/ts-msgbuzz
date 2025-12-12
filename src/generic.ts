export interface ConsumerConfirm {
    ack(): Promise<void>
    nack(): Promise<void>
    /**
     * Retry the message
     * @param delay - Number, delay in milliseconds
     * @param maxRetries - Number, max retry attempt
     * @param ack - Boolean, ack/nack the message upon retry success/failure
     */
    retry(opts: {
        delay?: number
        maxRetries?: number
        ack: boolean
    }): Promise<void>
}

export type MessageCallback = (
    confirm: ConsumerConfirm,
    message: Buffer
) => Promise<void>

export type NoMessageCallback = () => Promise<void>

export interface MessageBus {
    publish(topicName: string, message: Buffer, ...args: any[]): Promise<void>
    on(
        topicName: string,
        clientGroup: string,
        callback: MessageCallback,
        ...args: any[]
    ): void
    on2(
        topicName: string,
        clientGroup: string,
        callback: MessageCallback,
        ...args: any[]
    ): void
    startConsuming(): Promise<void>
}
