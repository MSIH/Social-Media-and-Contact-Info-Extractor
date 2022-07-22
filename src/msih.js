const Apify = require('apify');
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

/

module.exports = {
    // crawlFrames: async (page) => { },
    // getDomain(url) {},
    getURLSfromDatabase: async (sql) => { 
        var pool = mysql.createConnection({
            host: "msih005.local",
            user: "pricelocal",
            password: "979901979901",
            database: "PriceLocal"
        });

        const poolQuery = util.promisify(pool.query).bind(pool);
        const poolEnd = util.promisify(pool.end).bind(pool);

        consolelog(sql);

        try {
            const result = await poolQuery(sql);
        } catch (err) {
            throw err;
        }

        await poolEnd();
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
