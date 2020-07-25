const kafka = require('./config/kakfa');
const express = require('./config/express');

const app = express();

app.post('/new-order', async (req, res) => {

    const value = req.body;

    const producer = kafka.producer()

    await producer.connect()
    
    await producer.send({
      topic: 'ECOMMERCE_NEW_ORDER',
      messages: [
        { key: JSON.stringify(value), value: JSON.stringify(value) },
      ],
    }).then(data => {
        console.log(data);
    })
    
    await producer.disconnect()

    res.send({message: 'Order sent with success!'});
});

const PORT = 4001;

app.listen(PORT, () => {
    console.log(`API - new Order Producer running on port ${PORT}`)
})
