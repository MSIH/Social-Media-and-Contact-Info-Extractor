const Apify = require('apify');
const mysql2 = require('mysql2');
const util = require('util');
const { log } = Apify.utils;


function groupByKey(array, key) {
    return array
        .reduce((hash, obj) => {
            if (obj[key] === undefined) return hash;
            return Object.assign(hash, { [obj[key]]: (hash[obj[key]] || []).concat(obj) })
        }, {})
}

function uniqueValuesInObjects(obj) {
    return Object.fromEntries(
        [...new Set(obj.flatMap(d => Object.keys(d)))].map(k => [
            k,
            [... new Set(obj.flatMap(d => d[k] ? d[k] : null).filter(v => v != null && v != undefined))]
        ])
    );
}

async function getPendingRequestCount(requestQueue) {
    const { pendingRequestCount } = await requestQueue.getInfo();
    return pendingRequestCount
}

function getDateYYYYMMDD() {
    const dateYYYYMMDDstring = new Date().toLocaleDateString('sv').replaceAll('-', '');
    return parseInt(dateYYYYMMDDstring, 10)
}

function getYYYMMDDHHSS() {
    const timeStamp = (new Date()).toISOString().replace(/[^0-9]/g, '').slice(0, -3)
    return parseInt(timeStamp, 10)

}

function getPool() {
    const pool = mysql2.createPool({
        connectionLimit: 10,
        host: "msih005.local",
        user: "pricelocal",
        password: "979901979901",
        database: "PriceLocal"
    })

    // Ping database to check for common exception errors.
    pool.getConnection((err, connection) => {
        if (err) {
            if (err.code === 'PROTOCOL_CONNECTION_LOST') {
                console.error('Database connection was closed.')
            }
            if (err.code === 'ER_CON_COUNT_ERROR') {
                console.error('Database has too many connections.')
            }
            if (err.code === 'ECONNREFUSED') {
                console.error('Database connection was refused.')
            }
        }

        if (connection) pool.releaseConnection(connection);
    })

    return pool.promise();
}

module.exports = {
    // crawlFrames: async (page) => { },
    // getDomain(url) {},
    getURLSfromDatabase: async (limitSize = 20, sqlcode) => {

        let websites = [];
        let pool = getPool();

        let random = Math.floor(Math.random() * 1234567);
        let sql = "SELECT website, placeid FROM PriceLocal.Vendors \
            WHERE NOT website = 'none' AND SocialSearchDate < "+ getDateYYYYMMDD() + " ORDER BY id LIMIT " + random + "," + limitSize;
        log.info(sql);
        try {
            const [result, fields] = await pool.query(sql);
            // console.dir(result);
            result.forEach(async (element) => {
                //  console.dir(element.website);
                if (element.website != 'none') {
                    websites.push({ url: element.website, userData: { placeid: element.placeid } });
                    // https://sdk.apify.com/docs/api/request-list
                }
            });
        }
        catch {
            console.log('mysql error')
        }

  //      console.dir(websites);

        return websites;
    },

    putURLSfromDatabaseIntoQueue: async (requestQueue, sql, pendingMin = 10, limitSize = 20) => {

        // if Pending is less than 100, then get 1000
        let pendingRequestCount = await getPendingRequestCount(requestQueue);
        log.info("pendingRequestCount");
        log.info(work);
        console.info(pendingRequestCount);
        if (work) {
            if (pendingRequestCount < pendingMin) {
                work = false;
                var pool = mysql.createConnection({
                    host: "msih005.local",
                    user: "pricelocal",
                    password: "979901979901",
                    database: "PriceLocal"
                });

                let sql = "SELECT Website FROM PriceLocal.Vendors \
            WHERE SocialSearchDate < "+ getDateYYYYMMDD() + " LIMIT " + limitSize;

                log.info(sql);

                const poolQuery = util.promisify(pool.query).bind(pool);
                const poolEnd = util.promisify(pool.end).bind(pool);

                console.log(sql);

                try {
                    const result = await poolQuery(sql);
                    // console.log(result);
                    console.dir(result);
                    result.forEach(async (element) => {
                        console.dir(element.Website);
                        if (element.Website != 'none') {
                            await requestQueue.addRequest({ url: element.Website });
                        }
                    });
                } catch (err) {
                    throw err;
                }
                await poolEnd();
                work = false;
            }
        }
    },

    createDatasetWithDateTitle: async () => {
        // msih start
        // create dataset for data
        var dt = new Date();
        const yyyymmddhhmmss = `${dt.getFullYear().toString().padStart(4, '0')}${(dt.getMonth() + 1).toString().padStart(2, '0')}${dt.getDate().toString().padStart(2, '0')}${dt.getHours().toString().padStart(2, '0')}${dt.getMinutes().toString().padStart(2, '0')}${dt.getSeconds().toString().padStart(2, '0')}`;

        log.info(yyyymmddhhmmss);
        datasetTitle = yyyymmddhhmmss

        // open default dataset
        return await Apify.openDataset(datasetTitle);
        // msih end
    },

    updateWebSite: async (items, groupByKeyValue) => {
        let groupByKeyData = groupByKey(items, groupByKeyValue);

        let pool = getPool();

        const dateYYYYMMMDD = getDateYYYYMMDD();

        //console.dir(groupByKeyData);
        try {
            for (const key in groupByKeyData) {
                //console.dir(key);
                let sql = "UPDATE PriceLocal.Vendors SET SocialSearchDate = " + dateYYYYMMMDD +
                    " WHERE WebSite = '" + key + "';"

                // console.log(sql);
                const [result, fields] = await pool.query(sql);
                // console.log(result);
                log.info(`Updated SocialSearchDate field for ${key}`);
            }
        } catch (err) {
            throw err;
        }
    },

    saveSocial: async (items) => {

        let pool = getPool();

        const dateYYYYMMMDD = getDateYYYYMMDD();

        //console.dir(groupByKeyData);
        try {
            for (const webSite in items) {
                // console.dir(webSite);
                for (const key of ['discords', 'tiktoks', 'youtubes', 'instagrams',
                    'facebooks', 'linkedIns', 'phones', 'emails']) {
                    //console.dir(key);
                    let mySet = new Set;
                    items[webSite][key].forEach(item => {
                        cleanData = (item.endsWith('/') ? item.slice(0, -1) : item).toLowerCase()
                        cleanData = cleanData.replace(/["']/g, "");
                        mySet.add(cleanData);
                    });
                    //   for (const data in items[webSite][key]) {
                    for (const data of mySet) {
                        // console.dir(data);
                        //   let sql = "INSERT INTO PriceLocal." + key + " (`" + key + "`,`Website`) VALUES ('" + items[webSite][key][data] +
                        "','" + webSite + "');"

                        let sql = "INSERT IGNORE INTO PriceLocal." + key + " (`" + key + "`,`Website`,`placeid`) VALUES ('" + data +
                            "','" + webSite + "','" + items[webSite]['placeid'] + "');"
                        // console.log(sql);
                        const [result, fields] = await pool.query(sql);
                        //  console.log(result.insertId);
                    }
                }
                log.info(`Saved Social Data for ${webSite}`);
            }

        } catch (err) {
            console.error(err);
        }

    },

    groupByKeyUniueValuesAndSave: async (items, groupByKeyValue, jsonDataStorage) => {
        const groupByDomain = {}

        let groupByKeyData = groupByKey(items, groupByKeyValue);

        for (const key in groupByKeyData) {
            groupByDomain[key] = uniqueValuesInObjects(groupByKeyData[key]);
        }

        /*
        console.log(items);
        console.log(groupByKeyData);
        console.log(groupByDomain);
        */
        //var endTime = performance.now()
        //console.log(`Finding Unique took ${endTime - startTime} milliseconds`)

        await jsonDataStorage.setValue(datasetTitle + "-groupByDomain", groupByDomain);
        log.info('Group Data by WebSite and Save to folder jsonDataStorage/' + datasetTitle + '-groupByDomain');
        return groupByDomain
    },


    deleteRequestListAndQueue: async (requestList, requestQueue) => {
        // delete RequestList bin file
        //  console.log(requestList.persistRequestsKey);
        const store = await Apify.openKeyValueStore();
        if (requestList.persistRequestsKey !== undefined) {
            //    console.log("key is valid");  
            await store.setValue(requestList.persistRequestsKey, null);
        }
        await store.setValue("SDK_CRAWLER_STATISTICS_0", null);
        await store.setValue("SDK_SESSION_POOL_STATE", null);
        await store.setValue("STATE-REQUESTS-PER-START-URL", null);
        await requestQueue.drop();
        log.info('Delete Request Queue Database and Crawl Meta Files');
    },

    getStats: async (input) => {
        const dateYYYYMMMDDHHSS = getYYYMMDDHHSS();

        getStats = await Apify.getValue('SDK_CRAWLER_STATISTICS_0');

        if (getStats !== null) {
            getStats.requestMinDurationPerSeconds = (getStats.requestMinDurationMillis / 1000);
            getStats.requestMaxDurationPerSeconds = (getStats.requestMaxDurationMillis / 1000);
            getStats.requestAvgFinishedDurationPerSeconds = (getStats.requestAvgFinishedDurationMillis / 1000);
            getStats.TotalDurationMinutes = (getStats.crawlerRuntimeMillis / 1000 / 60);
            getStats.TotalDurationHours = (getStats.crawlerRuntimeMillis / 1000 / 60 / 60);
            getStats.requestPerMinute = getStats.requestsFinishedPerMinute;
            //getStats.customProxy = customProxy;
            //getStats.proxyProvider = proxyProvider;
            getStats.input = input;

            // open perfDataStorage key value store
            const perfDataStorage = await Apify.openKeyValueStore('perfDataStorage');
            // save perf data to file named datasetTitle
            await perfDataStorage.setValue(dateYYYYMMMDDHHSS.toString(), { getStats });
            log.info('Job Stats and Information');
            console.dir(getStats);
        }
    }
};
