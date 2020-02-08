const cheerio = require('cheerio');
const rp = require('request-promise');
const parse = require('csv-parse');
const stringify = require('csv-stringify')
const xlsx = require('xlsx');
const fs = require('fs');

//------------------------------------------------------------------------------------------
//Utility scraper functions

function loadScraperSite(site) {
    return rp({
        uri: site,
        transform: cheerio.load,
        headers: {
            'User-Agent': 'Request-Promise'
        }
    })
    .catch(err => {
        throw `An error occurred when fetching data from ${site}:\n\n${err}`
    })
}

function findWikipediaTable(site, $) {
    let [_, siteId] = site.split('#')
    siteId = '#' + siteId.replace(/\//g, '\\/')
    return $(siteId).parent()
        .nextAll('table.wikitable')
        .first()
}


function getTableRows(table, $) {
    return table
        .find('tbody tr')
        .filter($('td, th', 'table.wikitable').parent())
}

function extractWikipediaSingleTable(table, $) {
    return table.find('tbody tr')
        .filter($('td', 'table.wikitable').parent())
        .map(function(_i, row) {
            var county = $('td, th', row).first().text().trim().toLowerCase()
            var count = +$('td, th', row)
                .not((_i, element) => $(element).text().match(/^\s*$/))
                .last().text().replace(/,/g, '')

            return [[county, count]]
        }).get()
}

function extractWikipediaSplitPercentagesTable(table, $) {
    var tableRows = table.find('tbody tr').filter($('td', 'table.wikitable').parent())
    var results = new Array(tableRows.length / 2)

    tableRows.each(function(i, row) {
        if (i % 2 == 0) {
            var county = $('td, th', row).first().text().trim().toLowerCase()
            results[i/2] = [county, 0]
        } else {
            var count = +$('td, th', row)
                .not((_i, element) => $(element).text().match(/^\s*$/))
                .last().text().replace(/[,-]/g, '')
            results[(i-1)/2][1] = count
        }
    })

    return results

}

//Extract Wikipedia state election table. 
//A example of the format can be found here: https://en.wikipedia.org/wiki/2016_United_States_presidential_election_in_Tennessee#By_county
function defaultExtractDataSingleTable(site) {
    return loadScraperSite(site).then(function($) {
        var table = findWikipediaTable(site, $)
        return new Map(extractWikipediaSingleTable(table, $))
    })
}

//Extracts Wikipedia table where percentages and totals are split in one row. 
//An example of the format can be found here: https://en.wikipedia.org/wiki/2016_United_States_presidential_election_in_Michigan#Results_by_county
function defaultExtractDataSplitPercentagesTable(site) {
    return loadScraperSite(site).then(function($) {
        let [_, siteId] = site.split('#')
        var table = findWikipediaTable(site, $)
        return new Map(extractWikipediaSplitPercentagesTable(table, $))
    })
}

function addTotalAmount(results) {
    return results.set('total', Array.from(results.values()).reduce((total, c) => total + c, 0))
}

function replaceNames(replacements) {
    return function(results) {
        replacements.forEach(([oldName, newName]) => {
            results.set(newName, results.get(oldName))
            results.delete(oldName)
        })

        return results
    }
}

function replaceTotalName(targetName) {
    return replaceNames([[targetName, 'total']])
}


//--------------------------------------------------------------------------------------------------------
//State specific scrapers


var getAlabamaData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

function getAlaskaResults(site) {
    
    var mapFile = `${__dirname}/local_data/alaska_precinct_to_county.csv`

    var mappingPromise = new Promise((resolve, reject) => {
        var electionPrecinctMap = new Map()
        var precinctMapParser = parse({delimiter: ','})

        fs.createReadStream(mapFile).pipe(precinctMapParser)
            .on('data', ([precinct, county]) => electionPrecinctMap.set(precinct, county.toLowerCase()))
            .on('error', err => reject(err))
            .on('end', () => resolve(electionPrecinctMap))
    })
    

    var sitePromise = rp({
        uri: site,
        headers: {
            'User-Agent': 'Request-Promise'
        }
    })

    return Promise.all([sitePromise, mappingPromise])
        .then(([data, precinctMap]) => extractAlaskaResults(data, precinctMap))

}

//extract Alaska county results which are on a different website
//map precinct to county since Alaska does not keep a county count
//Alaska precinct to county site: https://web.archive.org/web/20170325204053/http://www.elections.alaska.gov/Core/listofpollingplacelocations.php
function extractAlaskaResults(electionData, electionPrecinctMap) {
    var electionDistrictResults = new Map()
    var electionCountyCount = new Map()
    var nonEDKey = 'Non ED'

    var electionDataParser = parse({delimiter: ' ,'})
    
    electionDataParser.on('readable', function() {

        let record
        while (record = electionDataParser.read()) {

            let [precinct, race, candidate, party, _, count] = record
            count = +count

            //aggregate voting results by district
            if (race == 'US PRESIDENT' && (party != 'NP' || candidate == 'Write-in 60')) {
                
                let district

                if (district = precinct.match(/(?<d>\d{2}\-\d{3})/)) {
                    //save number of votes in corresponding county

                    var region = district.groups.d
                    district = region.split('-')[0]
                    var county = electionPrecinctMap.get(region)

                    if (electionDistrictResults.has(district) && electionDistrictResults.get(district).has(county)) {
                        var counties = electionDistrictResults.get(district)
                        counties.set(county, counties.get(county) + count)
                    } else if (electionDistrictResults.has(district)) {
                        var counties = electionDistrictResults.get(district)
                        counties.set(county, count)
                    } else {
                        electionDistrictResults.set(district, new Map([[county, count]]))
                    }                    

                } else if (district = precinct.match(/District (?<d>\d+) \- (Absentee|Early Voting|Question)/)) {
                    //save number of votes in "non election day" results key

                    district = district.groups.d.padStart(2, '0')
                    
                    if (electionDistrictResults.has(district) && electionDistrictResults.get(district).has(nonEDKey)) {
                        var counties = electionDistrictResults.get(district)
                        counties.set(nonEDKey, counties.get(nonEDKey) + count)
                    } else if (electionDistrictResults.has(district)) {
                        var counties = electionDistrictResults.get(district)
                        counties.set(nonEDKey, count)
                    } else {
                        electionDistrictResults.set(district, new Map([[nonEDKey, count]]))
                    }
                }
            }
        }

        var totalCount = 0

        electionDistrictResults.forEach((breakdown) => {
            var totalEDCount = 0

            //add total election day count
            breakdown.forEach((count, county) => {
                if (county != nonEDKey) {
                    totalEDCount += count
                }
            })

            var nonEDTotalCount = breakdown.get(nonEDKey)

            //calculate non election day number of votes in proportion to election day percentage of total count for the county
            //add non election day number of votes to the election count
            breakdown.forEach((count, county) => {
                if (county != nonEDKey) {
                    var nonEDCount = Math.floor(nonEDTotalCount * (count / totalEDCount))

                    if (electionCountyCount.has(county)) {
                        electionCountyCount.set(county, electionCountyCount.get(county) + count + nonEDCount)
                    } else {
                        electionCountyCount.set(county, count + nonEDCount)
                    }

                    totalCount += count + nonEDCount
                }
            })

        })

        electionCountyCount.set('total', totalCount)
    })

    return new Promise(function (resolve, reject) {

        electionDataParser
            .on('error', e => reject(e))
            .on('end', () => resolve(electionCountyCount))

        electionDataParser.write(electionData)
        electionDataParser.end()
    })
}

var getArizonaData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

var getArkansasData = site => defaultExtractDataSingleTable(site)
    .then(replaceNames([['saint francis', 'st. francis']]))
    .then(addTotalAmount)

var getCaliforniaData = defaultExtractDataSplitPercentagesTable

var getColoradoData = site => defaultExtractDataSingleTable(site).then(replaceTotalName('colorado total'))

var getConnecticutData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

var getDelawareData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

function extractDCData(_site) {
    var total = 311268

    return Promise.resolve(new Map([
        ['district of columbia', total],
        ['total', total]
    ]))
}

var getFloridaData = site => defaultExtractDataSingleTable(site)
    .then(replaceNames([['suwanee', 'suwannee']]))
    .then(addTotalAmount)

var getGeorgiaData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

function getHawaiiData(site) {
    return defaultExtractDataSingleTable(site).then(function(results) {

        results.set('kalawao', results.get('kalawao[2]'))
        results.delete('kalawao[2]')
        results.set('total', Array.from(results.values()).reduce((total, c) => total + c))

        return results
    })
}

function getIdahoData(site) {
    return loadScraperSite(site).then(function($) {
        var table = findWikipediaTable(site, $)
        var tableRows = getTableRows(table, $).not((i, _row) => i == 0)
        var totalCount = 0

        var records = tableRows
            .map(function(_i, row) {
                var cells = $('td, th', row)
                var record = [cells.first().text().trim().toLowerCase(), 0]

                cells.filter((i, _row) => i > 0 && i % 2 == 0).each((_i, cell) => {
                    record[1] += +$(cell).text().replace(/,/g, '')
                })

                totalCount += record[1]
                return [record]
            }).get()

        records.push(['total', totalCount])
        return new Map(records)
    })
}

var getIllinoisData = site => defaultExtractDataSingleTable(site)
    .then(replaceNames([['dewitt', 'de witt']]))
    .then(addTotalAmount)

var getIndianaData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

var getIowaData = site => defaultExtractDataSingleTable(site).then(replaceTotalName('iowa total'))

var getKansasData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

var getKentuckyData = site => defaultExtractDataSingleTable(site).then(replaceTotalName('totals'))

var getLouisianaData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

function getMaineData(site) {

    return loadScraperSite(site).then(function($) {
        var table = findWikipediaTable(site, $)
        var tableRows = getTableRows(table, $)
        var totalCount = 0

        var results = tableRows
            .not((i, row) => i < 2 || $('td, th', row).first().text() == 'Overseas Ballots')
            .map(function(_i, row) {
                var count = +$('td, th', row).eq(-3).text().replace(/,/g, '')
                totalCount += count
                return [[$('td, th', row).first().text().trim().toLowerCase(), count]]
            }).get()
        
        results.push(['total', totalCount])
        return new Map(results)
    })
}


function getMarylandData(site) {
    return defaultExtractDataSingleTable(site).then(function(results) {
        var totalCount = 0

        for ([name, count] of results) {
            var newName = name.replace(/\((\w+)\)/, (_m, p1) => p1)
            if (name != newName) {
                results.set(newName, results.get(name))
                results.delete(name)
            }
            totalCount += count
        }

        results.set('total', totalCount)
        return results
    })
}

var getMassachusettsData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

var getMichiganData = defaultExtractDataSplitPercentagesTable

var getMinnesotaData = site => defaultExtractDataSingleTable(site)
    .then(replaceNames([['saint louis county', 'st. louis county']]))
    .then(addTotalAmount)

var getMississippiData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)


//Note: Kansas City is added into Jackson County though parts of the city are in Clay, Platte, and Cass County
function getMissouriData(site) {
    return defaultExtractDataSingleTable(site).then(addTotalAmount).then(function(results) {

        results.set('jackson county', results.get('jackson county without kansas city') + results.get('kansas city'))
        results.delete('kansas city')
        results.delete('jackson county without kansas city')

        return results
    })
}

var getMontanaData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

var getNebraskaData = defaultExtractDataSplitPercentagesTable

var getNevadaData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

var getNewHampshireData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)


function getNewJerseyData(site) {
    return loadScraperSite(site).then(function($) {
        var table = findWikipediaTable(site, $)
        var tableRows = getTableRows(table, $).not((i, _row) => i == 0)
        var totalCount = 0
        
        var records = tableRows
            .map(function(_i, row) {
                var cells = $('td, th', row)
                var record = [cells.first().text().trim().toLowerCase(), 0]
                cells.filter((i, row) => i % 2 == 1).each((_i, cell) => {
                    record[1] += +$(cell).text().replace(/,/g, '')
                })
                totalCount += record[1]
                return [record]
            }).get()
        
        records.push(['total', totalCount])
        return new Map(records)
    })
}

var getNewMexicoData = site => defaultExtractDataSplitPercentagesTable(site)
    .then(replaceNames([['dona ana', 'doña ana']]))


function getNewYorkData(site) {
    return loadScraperSite(site).then(function($) {
        var table = findWikipediaTable(site, $)
        var tableRows = getTableRows(table, $).not((i, _row) => i == 0)
        var totalCount = 0

        var records = tableRows
            .map(function(_i, row) {
                var cells = $('td, th', row)
                var record = [cells.first().text().trim().toLowerCase(), 0]
                cells.filter((i, _row) => i > 0 && i % 2 == 0).each((_i, cell) => {
                    record[1] += +$(cell).text().replace(/,/g, '')
                })
                totalCount += record[1]
                return [record]
            }).get()

        records.push(['total', totalCount])
        return new Map(records)
    })
}

var getNorthCarolinaData = site => defaultExtractDataSingleTable(site).then(replaceTotalName('totals'))

function getNorthDakotaData(site) {
    return loadScraperSite(site).then(function($) {
        var siteId = '#' + site.split('#')[1]
        var table = $(siteId).parent().next().find('table.wikitable')
        var tableRows = getTableRows(table, $).not((i, _row) => i < 2)
        var totalCount = 0

        var records = tableRows
            .map(function(_i, row) {
                var record = [
                    $('td, th', row).first().text().trim().toLowerCase(),
                    +$('td, th', row).eq(-2).text().replace(/,/g, '')
                ]
                totalCount += record[1]
                return [record]
            }).get()
        
        records.push(['total', totalCount])
        return new Map(records)
    })
}

var getOhioData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

var getOklahomaData = site => defaultExtractDataSingleTable(site).then(replaceTotalName('all counties'))


function getOregonData(site) {
    return loadScraperSite(site).then(function($) {
        var table = findWikipediaTable(site, $)
        var tableRows = getTableRows(table, $).not((i, _row) => i == 0)
        var totalCount = 0

        var records = tableRows
            .map(function(_i, row) {
                var cells = $('td, th', row)
                var record = [cells.first().text().trim().toLowerCase(), 0]
                cells.filter((i, _row) => i % 2 == 1).each((_i, cell) => {
                    record[1] += +$(cell).text().replace(/,/g, '')
                })
                totalCount += record[1]
                return [record]
            }).get()
        
        records.push(['total', totalCount])
        return new Map(records)
    })
}

var getPennsylvaniaData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

var getRhodeIslandData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

var getSouthCarolinaData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

var getSouthDakotaData = defaultExtractDataSplitPercentagesTable

var getTennesseeData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

var getTexasData = site => defaultExtractDataSingleTable(site)
    .then(replaceNames([['lasalle', 'la salle']]))
    .then(replaceTotalName('all counties'))

var getUtahData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

var getVermontData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

var getVirginiaData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

var getWashingtonData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

var getWestVirginiaData = site => defaultExtractDataSingleTable(site).then(addTotalAmount)

var getWisconsinData = site => defaultExtractDataSingleTable(site)
    .then(replaceNames([['st croix', 'st. croix'], ['totals', 'total']]))

function getWyomingData(site) {
    return loadScraperSite(site).then(function($) {
        var siteId = '#' + site.split('#')[1]
        var table = $(siteId).parent().next().find('table.wikitable')
        var tableRows = getTableRows(table, $).not((i, _row) => i < 2)

        var records = tableRows
            .map(function(_i, row) {
                var county = $('td, th', row).first().text().trim().toLowerCase()
                if (county == 'state total') {
                    county = 'total'
                }
                return [[county, +$('td, th', row).last().text().replace(/,/g, '')]]
            }).get()
        
        return new Map(records)
    })
}

//-------------------------------------------------------------------------------------------
//Object of scrapers used for each state
const stateScrapers = {
    Alabama: getAlabamaData,
    Alaska: getAlaskaResults,
    Arizona: getArizonaData,
    Arkansas: getArkansasData,
    California: getCaliforniaData,
    Colorado: getColoradoData,
    Connecticut: getConnecticutData,
    Delaware: getDelawareData,
    'Washington D.C.': extractDCData,
    Florida: getFloridaData,
    Georgia: getGeorgiaData,
    Hawaii: getHawaiiData,
    Idaho: getIdahoData,
    Illinois: getIllinoisData,
    Indiana: getIndianaData,
    Iowa: getIowaData,
    Kansas: getKansasData,
    Kentucky: getKentuckyData,
    Louisiana: getLouisianaData,
    Maine: getMaineData,
    Maryland: getMarylandData,
    Massachusetts: getMassachusettsData,
    Michigan: getMichiganData,
    Minnesota: getMinnesotaData,
    Mississippi: getMississippiData,
    Missouri: getMissouriData,
    Montana: getMontanaData,
    Nebraska: getNebraskaData,
    Nevada: getNevadaData,
    'New Hampshire': getNewHampshireData,
    'New Jersey': getNewJerseyData,
    'New Mexico': getNewMexicoData,
    'New York': getNewYorkData,
    'North Carolina': getNorthCarolinaData,
    'North Dakota': getNorthDakotaData,
    Ohio: getOhioData,
    Oklahoma: getOklahomaData,
    Oregon: getOregonData,
    Pennsylvania: getPennsylvaniaData,
    'Rhode Island': getRhodeIslandData,
    'South Carolina': getSouthCarolinaData,
    'South Dakota': getSouthDakotaData,
    Tennessee: getTennesseeData,
    Texas: getTexasData,
    Utah: getUtahData,
    Vermont: getVermontData,
    Virginia: getVirginiaData,
    Washington: getWashingtonData,
    'West Virginia': getWestVirginiaData,
    Wisconsin: getWisconsinData,
    Wyoming: getWyomingData,
}

function parseStateSite(state, site) {
    return stateScrapers[state](site).catch(err => { throw `A problem occurred when parsing scraper for ${state}:\n\n${err}\n` })
}

function getElectionData() {

    return new Promise((resolve, reject) => {
        const regionFile = `${__dirname}/local_data/regions.csv`
        let parser = parse({delimiter: ','})
        let stateScrapers = []

        fs.createReadStream(regionFile).pipe(parser)
            .on('data', ([stateCode, state, site]) => {
                let stateScraper = parseStateSite(state, site).then(electionResult => [stateCode, electionResult])
                stateScrapers.push(stateScraper)
            })
            .on('error', err => reject(err))
            .on('end', () => resolve(
                Promise.all(stateScrapers).then(results => {
                    let electionMap = new Map(results)
                    return replaceCountyNamesWithId(electionMap)
                })
            ))
    })

}

//replaces the county name with its FIPS code (e.g. Cook County Illinois is replaced with its code 17031)
function replaceCountyNamesWithId(electionData) {

     //TODO: either save local Excel file or download Excel file from the Internet
     const requestOptions = {
         uri: "https://www2.census.gov/programs-surveys/popest/geographies/2017/all-geocodes-v2017.xlsx",
         encoding: null,
         transform: xlsx.read,
         headers: { 'User-Agent': 'Request-Promise' }
        }

     return rp(requestOptions).then(workbook => {

        var firstSheetName = workbook.SheetNames[0]
        var worksheet = workbook.Sheets[firstSheetName]
    
        var countySummaryLevel = '050'
        var stateSummaryLevel = '040'
        var firstRow = 6, lastRow = 43915
        
    
        for (var i = firstRow; i <= lastRow; i++) {
            var code = worksheet['A' + i].v
            var stateCode = worksheet['B' + i].v
            var countyCode = worksheet['C' + i].v
            
            if (code == countySummaryLevel && stateCode < '57' && stateCode != '00' && countyCode != '000') {
                //replaces county name with county code

                var name = worksheet['G' + i].v.toLowerCase()
                var countyMap = electionData.get(stateCode)
                let key
    
                if (countyMap.has(name)) {
                    countyMap.set(stateCode + countyCode, {name: worksheet['G' + i].v, count: countyMap.get(name)})
                    countyMap.delete(name)
                } else if (key = Array.from(countyMap.keys()).find(x => name.startsWith(x + ' '))) {
                    countyMap.set(stateCode + countyCode, {name: worksheet['G' + i].v, count: countyMap.get(key)})
                    countyMap.delete(key)
                }
            } else if (code == stateSummaryLevel && stateCode < '57' && stateCode != '00') {
                //replaces "total" with state code

                var name = worksheet['G' + i].v
                var countyMap = electionData.get(stateCode)
                countyMap.set(stateCode, {name: name, count: countyMap.get('total')})
                countyMap.delete('total')
            }
        }
    
        return electionData
     })

}

//saves election data as CSV file
async function saveElectionDataCSV(csvFilename) {
    let stringifier = stringify({delimiter: ','})

    const results = await getElectionData();
    return new Promise((resolve, reject) => {
        stringifier.pipe(fs.createWriteStream(csvFilename).on('finish', () => console.log('Finished writing CSV results to %s', csvFilename)))
            .on('error', reject)
            .on('end', resolve)

        for (let [_, countyMap] of results) {
            for (let [code, { name, count }] of countyMap) {
                stringifier.write([code, name, count]);
            }
        }
        stringifier.end();
    });
}

//saves election data as JSON file
async function saveElectionDataJSON(jsonFilename) {
    const results = await getElectionData()

    return new Promise((resolve, reject) => {
        let writeStream = fs.createWriteStream(jsonFilename)
            .on('error', reject)
            .on('finish', () => {
                console.log("Finished writing JSON results to %s", jsonFilename)
                resolve()
            })
        
        writeStream.write('[')

        let stateIndex = 0
        for (let [stateCode, countyMap] of results) {

            writeStream.write(`{"stateCode": "${stateCode}", "regions": [`)

            let countyIndex = 0
            for (let [code, {name, count}] of countyMap) {
                writeStream.write(JSON.stringify({ regionCode: code, name, count }))
                countyIndex++
                if (countyIndex < countyMap.size) {
                    writeStream.write(',')
                }
            }

            writeStream.write(`] }`)
            stateIndex++
            if (stateIndex < results.size) {
                writeStream.write(',')
            }
        }

        writeStream.write(']')
        writeStream.end()
    })

}

module.exports = {
    saveElectionDataCSV,
    saveElectionDataJSON
}
