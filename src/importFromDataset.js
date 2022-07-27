const Apify = require('apify');
const { object } = require('underscore');
const { normalizeUrls } = require('./helpers');
const helpers = require('./helpers');
const { groupByKeyUniueValuesAndSave } = require('./msih');
const msih = require('./msih');
const { readdirSync } = require('fs');
const { getEnv } = require('apify');

const { log } = Apify.utils;

const PAGE_GOTO_TIMEOUT_SECS = 200;
const WAIT_FOR_BODY_SECS = 60;

Apify.main(async () => {



    const getDirectories = source =>
        readdirSync(source, { withFileTypes: true })
            .filter(dirent => dirent.isDirectory())
            .map(dirent => dirent.name)

    // MSIH start     
    let datasetTitle = "20220727052129"
    const resultsDataset = await Apify.openDataset(); //datasetTitle
    let datadir = process.env.INIT_CWD + "/" + process.env.APIFY_LOCAL_STORAGE_DIR + "/datasets"
   // console.log(datadir);  //localStorageDir
   console.log(getDirectories(datadir));  //localStorageDir
    let datadirectories = getDirectories(datadir);
    for (datasetTitle of datadirectories){   
        console.log(datadir + "/" + datasetTitle);
       
        const resultsDataset = await Apify.openDataset(datasetTitle);

        const { items } = await resultsDataset.getData();
        console.info('Save data to folder jsonDataStorage/' + datasetTitle + 'raw');
        const jsonDataStorage = await Apify.openKeyValueStore('jsonDataStorage');
        await jsonDataStorage.setValue(datasetTitle + 'raw', items);

        let grouppedData = await msih.groupByKeyUniueValuesAndSave(items, 'startUrl', jsonDataStorage, datasetTitle);
        await msih.updateWebSite(items, 'startUrl');
        await msih.saveSocial(grouppedData)

        //await msih.deleteRequestListAndQueue(requestList, requestQueue, i);
       await resultsDataset.drop();
        // MSIH end
    }
});
