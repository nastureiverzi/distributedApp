const express = require('express');
const bodyParser = require('body-parser');
const uuidv4 = require('uuid/v4');
const Redis = require('ioredis');
const moment = require('moment');
const axios = require('axios');
const crypto = require('crypto');
const asyncify = require('express-asyncify');
const ajv = require('ajv')();
const assert = require('assert');
const cronParser = require('cron-parser');

const validators = {
    create: ajv.compile({
        "type": "object",
        "properties": {
            "time": {
                "type": "string",
                "minLength": 10
            },
            "postback": {
                "type": "string",
                "format": "uri"
            }
        },
        "additionalProperties": false,
        "required": [
            "time",
            "postback"
        ]
    }),
    update: ajv.compile({
        "type": "object",
        "properties": {
            "time": {
                "type": "string",
                "minLength": 10
            },
            "postback": {
                "type": "string",
                "format": "uri"
            }
        },
        "additionalProperties": false
    })
}

const app = asyncify(express());
app.use(bodyParser.json());
const redis = new Redis();

async function subscribe() {
    const redisSub = new Redis();
    await redisSub.config('SET', "notify-keyspace-events", "Ex");
    await redisSub.psubscribe('_keyevent@0_:expired');
    redisSub.on("pmessage", (channel, message, key) => scheduleNext())
}

const serverId = `servers.${process.pid}`;


(async function main() {
    const now = Date.now();
    //console.log(now);
    const thisSecond = Math.floor(now / 1000);
    console.log(`This second: ${thisSecond}`);
    //    setTimeout(main, (thisSecond + 1) * 1000 - now);

    await redis.set(`${serverId}`, 'server', 'EX', 60);
    const servers = await redis.keys('*servers*');
    const timers = await redis.zrangebyscore('timers', 0, thisSecond);

    console.log(timers);
    if (timers) {
        const timersForServer = timers.map(res => JSON.parse(res)).filter((timer) => serverForTimer(timer, servers) === serverId);

        // timersForServer.forEach((timer) => {
        //     console.log(`ServerId ${serverId} with timer url ${timer.postback}`);
        // })
        console.log('---------------------------------------------------------------------------');

        timersForServer.forEach(async (timer) => {
            const response = await axios.get(timer.postback);
            console.log(`Timer with url ${timer.postback} was triggered from server ${serverId}`);
            //refresh score for timer in redis

            const interval = cronParser.parseExpression(timer.time, { currentDate: new Date(thisSecond * 1000) });
            //check if cron job recurrent, update timer if true, delete timer if false
            await redis.zadd('timers', 'XX', Math.floor(interval.next().toDate().getTime() / 1000), JSON.stringify({
                time: timer.time,
                postback: timer.postback
            }));

            // zadd or delete if no more

        });

        scheduleNext();

        // const now = Date.now(); // now in ms
        // const thisSecond = Math.floor(now / 1000) // this second
        // const nextSecond = thisSecond + 1;
        // const nextSecondInMS = nextSecond * 1000;
        // const deltaBetweenNowAndNextSecondInMS = nextSecondInMS - now;
    }
})();

function scheduleNext() {
    const nextTimer = (await redis.zrange('timers', 0, 0, 'WITHSCORES'))[1];
    console.log('Next timer', nextTimer);

    const now = Date.now();
    const thisSecond = Math.floor(now / 1000);

    setTimeout(main, nextTimer * 1000 - now);
}



function serverForTimer(timer, servers) {
    let maxServer = '';
    let maxHash = '';

    servers.forEach((server) => {
        const hash = crypto.createHash('sha256').update(`${server}.${timer.postback}`).digest('hex');
        if (hash > maxHash) {
            maxHash = hash;
            maxServer = server;
        }
    });

    return maxServer;
}

app.post('/timer', async (req, res) => {
    let interval;
    try {
        assert.ok(validators.create(req.body), 'Invalid input data.');
        interval = cronParser.parseExpression(req.body.time);
        assert.ok(interval.next().toDate() > new Date(), 'Date is in the past.');
    } catch (err) {
        res.status(400).send(err.message);
    }

    await redis.zadd('timers', Math.floor(interval.next().toDate().getTime() / 1000), JSON.stringify({
        time: req.body.time,
        postback: req.body.postback
    }));

    res.header('Location', `/timer/${req.body.postback}`);
    res.status(204).end();
});

app.get('/timer', async (req, res) => {

});

app.get('/timer/:id', async (req, res) => {

});

app.put('/timer/:id', async (req, res) => {

});

app.delete('/timer/:id', async (req, res) => {

});

app.use((err, req, res, next) => {
    if (!res.headersSent) {
        res.status(500);
    }

    console.log(err);
    res.send('Internal application error.');
});

process.on('uncaughtException', function (err) {
    console.log(err);
});

process.on('unhandledRejection', (reason, p) => {
    console.log('Unhandled Rejection at:', p, 'reason:', reason);
});

var listener = app.listen(3000, () => {
    console.log(`Listening on port ${listener.address().port}`);
});

