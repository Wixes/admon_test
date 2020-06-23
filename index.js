const http = require('http');
const url = require('url');
const { promisify } = require('util');
const redis = require('redis');
const { ClickHouse } = require('clickhouse');

const clickhouse = new ClickHouse({
    url: process.env.CLICKHOUSE_HOST,
    port: process.env.CLICKHOUSE_PORT
});

require('dotenv').config();

const client = redis.createClient({
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT
});

const rpushAsync = promisify(client.rpush).bind(client);
const lrangeAsync = promisify(client.lrange).bind(client);

client.on('error', function(error) {
    console.error(error);
});

const constructQuery = (data, keys, tableID) => {
    // Parse array of data to subarrays with data by queries
    let sortedArray = [];
    for (let i = 0; i < data.length; i+=keys.length) {
        sortedArray.push(data.slice(i, i + keys.length));
    }
    // Construct query
    let query = `INSERT INTO ${tableID} (${keys.toString()}) VALUES `;
    sortedArray.forEach(value => {
        let tempvalue = '';
        tempvalue = JSON.stringify(value);
        tempvalue = tempvalue.replace('[', '').replace(']', '');
        query += `(${tempvalue})`;
    });

    return query;
};

// Function for saving queries into clickHouse and deleting key in Redis
const makeLrangeAsync = (tableID, dataKeys) => {
    lrangeAsync(tableID, 0, -1)
        .then((res) => {
            if (Array.isArray(res) && res.length) {
                /* console.log('Data from list: ', res);
                console.log('keys: ', dataKeys); */
                const query = constructQuery(res, dataKeys, tableID);
                console.log('query: ', query);
                // Send query to the clickHouse
                clickhouse.query(query).exec(function (err, rows) {
                    if (err) console.log('Cannot execute query! ', err);
                    else console.log('Query executed. ', rows);
                });
                // Delete key from Redis
                client.del(tableID);
            }
            else console.log('No data for sending to the clickHouse');
        })
        .catch((err) => {
            console.log('Cannot retrieve data from Redis: ', err);
        });
};

// Timer to upload queries to clickHouse after SYNC_TIME
const sendTimer = (tableID, dataKeys) => {
      setTimeout(makeLrangeAsync, process.env.SYNC_TIME, tableID, dataKeys);
};

const requestHandler = (request, response) => {
    const path = url.parse(request.url).pathname;
    let data = [];
    switch (path) {
        case '/':
            request.on('data', (chunk) => {
                data.push(chunk);
            }).on('end', () => {
                data = Buffer.concat(data).toString();
                data = JSON.parse(data);
                // Make arrays with only values and only keys
                let dataValues = [];
                let dataKeys = [];
                for (let key in data){
                    dataValues.push(data[key]);
                    dataKeys.push(key);
                }
                /* console.log('data: ', dataValues);
                console.log('keys: ', dataKeys); */
                const tableID = dataValues[0];
                dataValues.splice(0, 1);
                dataKeys.splice(0, 1);
                const dataCountElements = dataValues.length;
                /* console.log('elements in array: ', dataCountElements); */
                // Save values in Redis list with key tableID
                rpushAsync(tableID, ...dataValues)
                    .then((res) => {
                        const message = {message: 'All data successfuly stored'};
                        if (res / dataCountElements <= process.env.BUFFER_LIMIT) {
                            console.log('Data is saved, total fields: ', res);
                            sendTimer(tableID, dataKeys);
                            response.writeHead(200, {
                                'Content-Type': 'application/json'
                            });
                            response.write(JSON.stringify(message));
                            response.end();
                        } else {
                            console.log('Buffer is full. Sending data to the clickHouse...');
                            response.writeHead(200, {
                                'Content-Type': 'application/json'
                            });
                            response.write(JSON.stringify(message));
                            response.end();
                            makeLrangeAsync(tableID, dataKeys);
                        }
                        
                    })
                    .catch((err) => {
                        console.log('Error while saving: ', err);
                        const message = {message: 'Error while saving'};
                        response.writeHead(404, {
                            'Content-Type': 'application/json'
                        });
                        response.write(JSON.stringify(message));
                        response.end();
                    });
            });
            break;
        default:
            data = Object.assign({}, {"error": "This page is not exist"});
            console.log('data: ', data);
            response.writeHead(404, {
                'Content-Type': 'application/json'
            });
            response.write(JSON.stringify(data));
            response.end();
            break;
    }
    

};

const errorHandler = (err) => {
    if (err.code === 'EADDRINUSE') {
        console.log('Address in use, retrying...');
        setTimeout(() => {
            server.close;
            server.listen(process.env.SERVER_PORT);
        }, 2000);
    }
    console.log('Whoops! something bad happened. ', err);
}

// Create http server and listen port
const server=http.createServer(requestHandler)
                 .listen(process.env.SERVER_PORT, (err) => {
                    if (err)
                        return console.log('Cannot listen on this port!');
                    else 
                        console.log(`Server is listening port ${process.env.SERVER_PORT}`);
                });

// Handle errors
server.on('error', errorHandler);
