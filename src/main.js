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

        
        const input = await Apify.getValue('INPUT');
        if (!input) throw new Error('There is no input!');

        const {
            startUrls,
            proxyConfig,
            sameDomain,
            maxDepth,
            considerChildFrames,
            // These are total (kept naming for backward compatibillity)
            maxRequests,
            maxRequestsPerStartUrl,
            numberURLS,
            runs
        } = input;

    for (let i = 0; i < runs; i++) {
        log.info("Run number: " + i.toString());

        // Object with startUrls as keys and counters as values
        const requestsPerStartUrlCounter = (await Apify.getValue('STATE-REQUESTS-PER-START-URL')) || {};

        if (maxRequestsPerStartUrl) {
            const persistRequestsPerStartUrlCounter = async () => {
                await Apify.setValue('STATE-REQUESTS-PER-START-URL', requestsPerStartUrlCounter);
            };
            setInterval(persistRequestsPerStartUrlCounter, 60000);
            Apify.events.on('migrating', persistRequestsPerStartUrlCounter);
        }

        const requestQueue = await Apify.openRequestQueue();
        // const requestList = await Apify.openRequestList('start-urls', normalizeUrls(startUrls));
    
        // msih start
        // create dataset for data
        const resultsDataset = await msih.createDatasetWithDateTitle();
        //console.dir(startUrls);
        let URLSfromDatabase = startUrls;
        if (!startUrls) {
            URLSfromDatabase = await msih.getURLSfromDatabase(numberURLS);
        }
        const requestList = await Apify.openRequestList(null, URLSfromDatabase);

  /*       if (proxyConfig) {
            proxyConfig = {
                proxyUrls: proxyConfig
            }
        } */
        // msih end

        requestList.requests.forEach((req) => {
            req.userData = {
                depth: 0,
                referrer: null,
                startUrl: req.url,
                placeid: req.userData.placeid,
            };
            if (maxRequestsPerStartUrl) {
                if (!requestsPerStartUrlCounter[req.url]) {
                    requestsPerStartUrlCounter[req.url] = {
                        counter: 1,
                        wasLogged: false,
                    };
                }
            }
            req.noRetry = true;
        });

        const proxyConfiguration = await Apify.createProxyConfiguration(proxyConfig);

        // Create the crawler
        const crawlerOptions = {
            requestList,
            requestQueue,
            proxyConfiguration,
            launchContext: {
                useIncognitoPages: true,
                useChrome: false,
                launchOptions: {
                    headless: true,
                }
            },
            browserPoolOptions: {
                useFingerprints: true,
            },
            navigationTimeoutSecs: 10,
            handlePageTimeoutSecs: 10,
            maxRequestRetries: 1,
            handlePageFunction: async ({ page, request }) => {
                log.info(`Processing ${request.url}`);

                // Wait for body tag to load
                await page.waitForSelector('body', {
                    timeout: WAIT_FOR_BODY_SECS * 1000,
                });

                // Set enqueue options
                const linksToEnqueueOptions = {
                    page,
                    requestQueue,
                    selector: 'a',
                    sameDomain,
                    urlDomain: helpers.getDomain(request.url),
                    startUrl: request.userData.startUrl,
                    depth: request.userData.depth,
                    // These options makes the enqueueUrls call stateful. It would be better to refactor this.
                    maxRequestsPerStartUrl,
                    requestsPerStartUrlCounter,
                };

                // Enqueue all links on the page
                if (typeof maxDepth !== 'number' || request.userData.depth < maxDepth) {
                    await helpers.enqueueUrls(linksToEnqueueOptions);
                }

                // Crawl HTML frames
                let frameSocialHandles = {};
                if (considerChildFrames) {
                    frameSocialHandles = await helpers.crawlFrames(page);
                }

                // Generate result
                const { userData: { depth, referrer } } = request;
                const url = page.url();
                const html = await page.content();

                const result = {
                    html,
                    depth,
                    referrerUrl: referrer,
                    url,
                    domain: helpers.getDomain(url),
                    startUrl: request.userData.startUrl,
                    placeid: request.userData.placeid,
                };

                // Extract and save handles, emails, phone numbers
                const socialHandles = Apify.utils.social.parseHandlesFromHtml(html);

                // Merge frames with main
                const mergedSocial = helpers.mergeSocial(frameSocialHandles, socialHandles);
                Object.assign(result, mergedSocial);

                // Clean up
                delete result.html;

                // Store results
                // await Apify.pushData(result);

                // msih start
                await resultsDataset.pushData(result);
                // await msih.getURLSfromDatabase(requestQueue);
                // msih end
            },
            handleFailedRequestFunction: async ({ request }) => {
                log.error(`Request ${request.url} failed ${maxRequestRetries}`);
            },
            gotoFunction: async ({ page, request }) => {
                // Block resources such as images and CSS files, to increase crawling speed
                await Apify.utils.puppeteer.blockRequests(page);

                return page.goto(request.url, {
                    timeout: PAGE_GOTO_TIMEOUT_SECS * 1000,
                    waitUntil: 'domcontentloaded',
                });
            },
        };

        // Limit requests
        if (maxRequests) crawlerOptions.maxRequestsPerCrawl = maxRequests;

        // Create crawler
        const crawler = new Apify.PuppeteerCrawler(crawlerOptions);

        // Run crawler
        log.info(`Starting the crawl...`);
        await crawler.run();
        log.info(`Crawl finished`);

        // MSIH start
        await msih.getStats(input,i);
        const { items } = await resultsDataset.getData();
        console.info('Save data to folder jsonDataStorage/' + datasetTitle + 'raw');
        const jsonDataStorage = await Apify.openKeyValueStore('jsonDataStorage');
        await jsonDataStorage.setValue(datasetTitle + 'raw', items);
   
        let grouppedData = await msih.groupByKeyUniueValuesAndSave(items, 'startUrl', jsonDataStorage);
        await msih.updateWebSite(items, 'startUrl');
        await msih.saveSocial(grouppedData)

        await msih.deleteRequestListAndQueue(requestList, requestQueue,i);
        await resultsDataset.drop();
        // MSIH end

    }
});
