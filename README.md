# ts-msgbuzz

Message Bus implementation in typescript.

Available implementations:

-   RabbitMQ
-   Supabase queue

## Installation

Install directly from GitHub:

```bash
npm install akromkhasani/ts-msgbuzz
```

Or with a specific version/branch:

```bash
npm install akromkhasani/ts-msgbuzz#v2.0.0
```

## Usage

1. Subscribe

```typescript
import { ConsumerConfirm, RabbitMqMessageBus } from '@akromkhasani/ts-msgbuzz'

async function main() {
    const msgBus = new RabbitMqMessageBus('amqp://guest:guest@127.0.0.1:5672/')

    msgBus.on('topic1', 'worker', processMessage)
    msgBus.on2('topic2', 'worker', processMessage)

    msgBus.startConsuming().catch(console.log)

    const close = async () => {
        await msgBus.close()
        process.exit(0)
    }

    process.on('SIGINT', close)
    process.on('SIGTERM', close)
}

async function processMessage(
    confirm: ConsumerConfirm,
    msg: Buffer
): Promise<void> {
    console.log(`Received message: ${msg.toString()}`)
    await confirm.ack()
}

main().catch(console.log)
```

2. Publish

```typescript
import { ConsumerConfirm, RabbitMqMessageBus } from '@akromkhasani/ts-msgbuzz'

async function main() {
    const msgBus = new RabbitMqMessageBus('amqp://guest:guest@127.0.0.1:5672/')

    await msgBus.publish('topic1', Buffer.from('messsage1'))
    await msgBus.publish('topic2', Buffer.from('messsage2'))

    await msgBus.close()
}

main().catch(console.log)
```

## License

MIT
