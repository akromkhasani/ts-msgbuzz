export interface ConsumerConfirm {
    ack(): Promise<void>
    nack(): Promise<void>
    /**
     * Retry the message
     * @param delay delay in milliseconds
     * @param maxRetries max retry attempt
     */
    retry(delay?: number, maxRetries?: number): Promise<void>
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
