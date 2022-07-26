const Apify = require('apify');
const { object } = require('underscore');
const { normalizeUrls } = require('./helpers');
const helpers = require('./helpers');
const { groupByKeyUniueValuesAndSave } = require('./msih');
const msih = require('./msih');

const { log } = Apify.utils;

const PAGE_GOTO_TIMEOUT_SECS = 200;
const WAIT_FOR_BODY_SECS = 60;

Apify.main(async () => {
    
    // MSIH start     
    let datasetTitle = "20220724220817"
    const resultsDataset = await Apify.openDataset(datasetTitle);
    const { items } = await resultsDataset.getData();
    console.info('Save data to folder jsonDataStorage/' + datasetTitle + 'raw');
    const jsonDataStorage = await Apify.openKeyValueStore('jsonDataStorage');
    await jsonDataStorage.setValue(datasetTitle + 'raw', items);

    let grouppedData = await msih.groupByKeyUniueValuesAndSave(items, 'startUrl', jsonDataStorage, datasetTitle);
    await msih.updateWebSite(items, 'startUrl');
    await msih.saveSocial(grouppedData, true)

    //await msih.deleteRequestListAndQueue(requestList, requestQueue, i);
    await resultsDataset.drop(); 
    // MSIH end


});
