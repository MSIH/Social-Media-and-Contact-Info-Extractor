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
    datasetTitle = "20220725084504-groupByDomain"
    const jsonDataStorage = await Apify.openKeyValueStore('jsonDataStorage');
    let grouppedData = await jsonDataStorage.getValue(datasetTitle)
    await msih.saveSocial(grouppedData,true)
    await msih.getStats({});   
    // MSIH end


});
