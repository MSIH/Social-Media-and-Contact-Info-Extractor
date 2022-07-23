const Apify = require('apify');
const mysql = require('mysql');
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

let work = true;

module.exports = {
    // crawlFrames: async (page) => { },
    // getDomain(url) {},
    getURLSfromDatabase: async (limitSize = 20, sqlcode) => {

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

        let websites = [];

        try {
            const result = await poolQuery(sql);
            // console.log(result);
            //console.dir(result);
            result.forEach(async (element) => {
                console.dir(element.Website);
                if (element.Website != 'none') {
                    websites.push({ url: element.Website });
                }
            });
        } catch (err) {
            throw err;
        }
        await poolEnd();
        work = false;
        console.dir(websites);
        return websites;
    },

    getURLSfromDatabase: async (limitSize = 20, sqlcode) => {

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

        let websites = [];

        try {
            const result = await poolQuery(sql);
            // console.log(result);
            //console.dir(result);
            result.forEach(async (element) => {
                console.dir(element.Website);
                if (element.Website != 'none') {
                    websites.push({ url: element.Website });
                }
            });
        } catch (err) {
            throw err;
        }
        await poolEnd();
        work = false;
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
        const groupByDomain = {}

        let groupByKeyData = groupByKey(items, groupByKeyValue);

        for (const key in groupByKeyData) {
            groupByDomain[key] = uniqueValuesInObjects(groupByKeyData[key]);
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
        // return groupByDomain
    },


    deleteRequestListAndQueue: async (requestList, requestQueue) => {
        // delete RequestList bin file
        //let key = requestList.persistRequestsKey;
        const store = await Apify.openKeyValueStore();
        await store.setValue(requestList.persistRequestsKey, null);
        await requestQueue.drop();
    }
};
