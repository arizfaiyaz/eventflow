const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'kitchen-app',
  brokers: ['localhost:9092']
});

// Consumers must belong to a "Group". 
// Multiple consumers in the same group share the workload (load balancing).
const consumer = kafka.consumer({ groupId: 'kitchen-staff-group-v2' });

async function run() {
  await consumer.connect();
  console.log('✅ Kitchen consumer connected successfully.');
  
  // Subscribe to the topic. 'fromBeginning: true' ensures we process 
  // old messages if the kitchen starts up late.
  await consumer.subscribe({ topic: 'orders', fromBeginning: true });
  
  // Continuously listen for new messages
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());
      console.log(`👨‍🍳 Kitchen preparing: ${order.item} (Order #${order.id})`);
    },
  });
}

run().catch(console.error);