const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'customer-app',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();

async function run() {
    await producer.connect();
    console.log('Producer connected');

    let orderId = 1;
    const items = ['Pizza 🍕', 'Burger 🍔', 'Sushi 🍣', 'Tacos 🌮'];
    setInterval(async () => {
        const randomItem =  items[Math.floor(Math.random() * items.length)];
        const order = {
            id: orderId++,
            item: randomItem,
            timestamp: new Date().toISOString()
        };
        await producer.send({
            topic: 'orders',
            messages: [
                {key:  String(order.id), value: JSON.stringify(order) }

            ]
        });
        console.log(`customer  Ordered: ${order.item} (Order #$${order.id})`);
    }, 2000)
}

run().catch(console.error);