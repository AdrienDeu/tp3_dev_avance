const { Kafka } = require('kafkajs')
const redis = require('redis')

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['my-broker-url']
})

const consumer = kafka.consumer({ groupId: 'my-group' })
const redisClient = redis.createClient('redis://my-redis-url')

const run = async () => {
    await consumer.connect()
    console.log('Connected!')
    await consumer.subscribe({ topic: 'my-topic', fromBeginning: true })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const timestamp = new Date(parseInt(message.timestamp)).toLocaleString('fr-FR')
            const words = message.value.toString().split(' ')
            words.forEach(word => {
                redisClient.incr(word, function(err, reply) {
                    console.log(`Word: ${word}, Count: ${reply}`)
                })
            })
            console.log(`Received message ${message.value.toString()} at ${timestamp}`)
        },
    })
}

run().catch(console.error)