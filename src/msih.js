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

let mySQLPoolConnection;
function getPool() {
    if (!mySQLPoolConnection) {
        mySQLPoolConnection = mysql2.createPool({
            connectionLimit: 20,
            host: "msih005.local",
            user: "pricelocal",
            password: "979901979901",
            database: "PriceLocal"
        })
    }

    // Ping database to check for common exception errors.
    mySQLPoolConnection.getConnection((err, connection) => {
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

        if (connection) mySQLPoolConnection.releaseConnection(connection);
    })

    return mySQLPoolConnection.promise();
}

module.exports = {
    // crawlFrames: async (page) => { },
    // getDomain(url) {},
    zipDatastore: async (dataStoreName) => {
        // zip file
        const STORAGE_DIR = './' + process.env.APIFY_LOCAL_STORAGE_DIR;
        const KEY_VALUE_STORES = 'key_value_stores';    
        const kvpPath = STORAGE_DIR + '/' + KEY_VALUE_STORES;
        const dataFilePathAndName = kvpPath + '/jsonDataStorage/' + dataStoreName;
        console.log(dataFilePathAndName);

        const file = new AdmZip();
        file.addLocalFile(dataFilePathAndName + '.json');
        file.writeZip(dataFilePathAndName + '.zip');
        console.log('file Zipped');
    },
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

  
        console.dir(websites);

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
            for (let key in groupByKeyData) {
                //console.dir(key);
               // key = (key.endsWith('/') ? key.slice(0, -1) : key).toLowerCase() //remove slash at end
                key = key.replace(/["']/g, ""); //remove ' on string,  
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

    saveSocial: async (items,debug) => {

        let pool = getPool();

        try {
            for (const webSite in items) {
                // console.dir(webSite);
                let placeid = items[webSite]['placeid'] || "none"; //old runs did not capture this data, so if null put in placeholder
                for (const key of ['discords', 'tiktoks', 'youtubes', 'instagrams',
                    'facebooks', 'linkedIns', 'phones', 'emails']) { //these are the only keys we are captuing data from at this time
                    //console.dir(key);
                    let mySet = new Set;
                    items[webSite][key].forEach(item => {
                        cleanData = (item.endsWith('/') ? item.slice(0, -1) : item).toLowerCase() //remove slash at end
                        cleanData = cleanData.replace(/["']/g, ""); //remove ' on string,                         
                        mySet.add(cleanData); //set only stores unique values
                    });
                    //   for (const data in items[webSite][key]) {
                    for (const data of mySet) {
                        // console.dir(data);
                        let web = webSite.replace(/["',]/g, "");
                        let sql = "INSERT IGNORE INTO PriceLocal." + key + " (`" + key + "`,`Website`,`placeid`) VALUES ('" + data +
                            "','" + web + "','" + placeid + "');"
                        if(debug) console.log(sql);
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

    groupByKeyUniueValuesAndSave: async (items, groupByKeyValue, jsonDataStorage, datasetTitle) => {
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


    deleteRequestListAndQueue: async (requestList, requestQueue,statIndex) => {
        // delete RequestList bin file
        //  console.log(requestList.persistRequestsKey);
        const store = await Apify.openKeyValueStore();
        if (requestList.persistRequestsKey !== undefined) {
            //    console.log("key is valid");  
            await store.setValue(requestList.persistRequestsKey, null);
        }
        await store.setValue("SDK_CRAWLER_STATISTICS_" + statIndex.toString(), null);
        await store.setValue("SDK_SESSION_POOL_STATE", null);
        await store.setValue("STATE-REQUESTS-PER-START-URL", null);
        await requestQueue.drop();
        log.info('Delete Request Queue Database and Crawl Meta Files');
    },

    getStats: async(input, statIndex=0) => {
        const dateYYYYMMMDDHHSS = getYYYMMDDHHSS();

        getStats = await Apify.getValue("SDK_CRAWLER_STATISTICS_" + statIndex.toString());

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
