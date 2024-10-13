const { Kafka } = require('kafkajs');
const fs = require("fs");
const jks = require('jks-js');


const MyPartitioner = () => {
    return ({ topic, partitionMetadata, message }) => {
        // select a partition based on some logic
        // return the partition number
        return 0
    }
}


const run = async () => {

    // const keystore = jks.toPem(
    //     fs.readFileSync('kafka.client.truststore.jks'),
    //     'changeit'
    // );
    const keystore = jks.toPem(
        fs.readFileSync('./kafka.client.keystore.jks'),
        'changeit'
    );

    // const { cert, key, ca } = keystore['caroot'];
    const { cert, key } = keystore['localhost'];
    const { ca } = keystore['caroot'];
    // const { ca: ca1 } = keystore['CN=Kafka'];

    const kafka = new Kafka({
        clientId: 'kafka-ssl',
        brokers: ['localhost:9092'],
        // ssl: {
        //     rejectUnauthorized: false,
        //     // ca: [ca],
        //     // key: key,
        //     cert: cert
        //   },
        // ssl: {
        //     rejectUnauthorized: false,
        //     ca: [fs.readFileSync('./tmp/consumer-ca-signed.crt', 'utf-8')],
        //     key: fs.readFileSync('./secrets/consumer.key', 'utf-8'),
        //     cert: fs.readFileSync('./secrets/consumer.pem', 'utf-8')
        //   },
    });
    console.log('kafka 1: ');

    // const producer = kafka.producer({
    //     createPartitioner: MyPartitioner
    // });

    // console.log('kafka producer  ');

    // await producer.connect();
    // await producer.send({
    //     topic: 'test-topic',
    //     messages: [ { value: 'Hello KafkaJS user!' }, ],
    // });

    // console.log('kafka producer send  ');

    // await producer.disconnect();

      
    const consumer = kafka.consumer({ groupId: 'test-group' })

    await consumer.connect();
    await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log('SUBSCRIBED!!!!', {
                value: message?.value?.toString(),
            })
        },
    });
}


run().then((res) => {
    console.log('ALL OK: ', res);
}).catch((e) => {
    console.error('FINAL e: ', e);
});