const cheerio = require('cheerio');
const rp = require('request-promise');
const parse = require('csv-parse');
const stringify = require('csv-stringify')
const xlsx = require('xlsx');
const fs = require('fs');

const electionResultsFile = "election_results.csv"
const regions = `${__dirname}/local_data/regions.csv`


//------------------------------------------------------------------------------------------
//State scrapers for top level and district


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
}

function _findWikipediaTable(site, $) {
    var siteId = '#' + site.split('#')[1].replace(/\//g, '\\/')
    return $(siteId).parent()

        .nextAll('table.wikitable')
        .first()
}


function findWikipediaTable($, siteId) {
    return $(`#${siteId.replace(/\//g, '\\/')}`).parent()
        .nextAll('table.wikitable').first()
}


function getTableRows(table, $) {
    return table
        .find('tbody tr')
        .filter($('td, th', 'table.wikitable').parent())
}

// function extractWikipediaSingleTable(table, $) {
//     return table.find('tbody tr')
//         .filter($('td', 'table.wikitable').parent())
//         .map(function(_i, row) {
//             return {
//                 county: $('td, th', row).first().text().trim().toLowerCase(),
//                 count: +$('td, th', row)
//                     .not((_i, element) => $(element).text().match(/^\s*$/))
//                     .last().text().replace(/,/g, '')
//             };
//         }).get()
// }


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

//extract table where percentages and totals are split 
//See https://en.wikipedia.org/wiki/2016_United_States_presidential_election_in_Nebraska#Results_by_county for example
// function extractWikipediaSplitPercentagesTable(table, $) {
//     var tableRows = table.find('tbody tr').filter($('td', 'table.wikitable').parent())
//     var results = new Array(tableRows.length / 2)

//     tableRows.each(function(i, row) {
//         if (i % 2 == 0) {
//             var county = $('td, th', row).first().text().trim().toLowerCase()
//             results[i/2] = { county: county, count: 0 }
//         } else {
//             var count = +$('td, th', row)
//                 .not((_i, element) => $(element).text().match(/^\s*$/))
//                 .last().text().replace(/[,-]/g, '')
//             results[(i-1)/2].count = count
//         }
//     })

//     return results

// }


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

// function defaultExtractDataSingleTable(site) {
//     return loadScraperSite(site).then(function($) {
//         var siteId = site.split('#')[1]
//         var table = findWikipediaTable($, siteId)
//         return extractWikipediaSingleTable(table, $)
//     })
// }

function defaultExtractDataSingleTable(site) {
    return loadScraperSite(site).then(function($) {
        var siteId = site.split('#')[1]
        var table = findWikipediaTable($, siteId)
        return new Map(extractWikipediaSingleTable(table, $))
    })
}

// function defaultExtractDataSplitPercentagesTable(site) {
//     return loadScraperSite(site).then(function($) {
//         var siteId = site.split('#')[1]
//         var table = findWikipediaTable($, siteId)
//         return extractWikipediaSplitPercentagesTable(table, $)
//     })
// }

function defaultExtractDataSplitPercentagesTable(site) {
    return loadScraperSite(site).then(function($) {
        var siteId = site.split('#')[1]
        var table = findWikipediaTable($, siteId)
        return new Map(extractWikipediaSplitPercentagesTable(table, $))
    })
}

function extractWikipediaSite(tableFunction, extractionFunction) {
    return function(site) {
        loadScraperSite(site).then(function($) {
            var siteId = site.split('#')[1]
            var table = tableFunction($, siteId)
            return extractionFunction(table, $)
        })
    }
}

// function addTotalAmount(results) {
//     var total = {county: 'Total', count: results.reduce((total, c) => total + c.count, 0)}
//     results.push(total)
//     return results
// }

function addTotalAmount(results) {
    return results.set('total', Array.from(results.values()).reduce((total, c) => total + c))
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
//map precinct to county since Alaska dooes not keep a county count
//Alaska precinct to county site: https://web.archive.org/web/20170325204053/http://www.elections.alaska.gov/Core/listofpollingplacelocations.php
function extractAlaskaResults(electionData, electionPrecinctMap) {
    var electionDistrictResults = new Map()
    var electionCountyCount = new Map()
    var nonEDKey = 'Non ED'

    var electionDataParser = parse({delimiter: ' ,'})
    
    electionDataParser.on('readable', function() {

        let record
        while (record = electionDataParser.read()) {
            
            var race = record[1]
            var candidate = record[2]
            var party = record[3]

            //TODO: handle absentee and early voting numbers, split district numbers proportionally along counties
            //TODO: add total number to the end of the list of counties
            if (race == 'US PRESIDENT' && (party != 'NP' || candidate == 'Write-in 60')) {
                
                var precinct = record[0]
                var count = +record[5]
                let district

                if (district = precinct.match(/(?<d>\d{2}\-\d{3})/)) {
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
            breakdown.forEach((count, county) => {
                if (county != nonEDKey) {
                    totalEDCount += count
                }
            })

            var nonEDTotalCount = breakdown.get(nonEDKey)

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
        var table = _findWikipediaTable(site, $)
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
        var table = _findWikipediaTable(site, $)
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

//Reminder: when joining data make names lowercase
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


//TODO: fill in Missouri site scraper
//Note: document that Kansas City is added into Jackson County though parts of the city are in Clay, Platte, and Cass County
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
        var table = _findWikipediaTable(site, $)
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
        var table = _findWikipediaTable(site, $)
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
        var table = _findWikipediaTable(site, $)
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
    return stateScrapers[state](site)
}


function saveStateElectionData(resultsFile) {

    if (resultsFile == null || resultsFIle == undefined) {
        resultsFile = `${__dirname}/local_data/${electionResultsFile}`
    }

    //if election file exists, read file, parse to JSON, return results
    //otherwise, scrape results from given websites, scrape county code website, map county to county code, save to election file, and return results
    
    return new Promise((resolve, reject) => {
        var filename = `${__dirname}/local_data/regions.csv`
       
        var parser = parse({delimiter: ','})
        var stateScrapers = []

        fs.createReadStream(filename).pipe(parser)
            .on('data', row => {
                var stateCode = row[0]
                var state = row[1]
                var site = row[2]
                var stateScraper = parseStateSite(state, site).then(electionResult => {
                    return {
                        code: stateCode,
                        data: electionResult
                    }
                })

                stateScrapers.push(stateScraper)
            })
            .on('error', e => reject(e))
            .on('end', () => resolve(Promise.all(stateScrapers).then(function(results) {
                var electionMap = new Map()
                results.forEach(({code, data}) => electionMap.set(code, data))
                electionMap = replaceCountyNamesWithId(electionMap)
                return saveElectionDataToCSV(electionMap, resultsFile)
            })))
        })

}

function replaceCountyNamesWithId(electionData) {

    var workbook = xlsx.readFile(`${__dirname}/local_data/all-geocodes-v2017.xlsx`)
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
            var name = worksheet['G' + i].v
            var countyMap = electionData.get(stateCode)
            countyMap.set(stateCode, {name: name, count: countyMap.get('total')})
            countyMap.delete('total')
        }
    }

    return electionData
}

//write data to CSV
function saveElectionDataToCSV(electionData, resultsFile) {
    var stringifier = stringify({delimiter: ','})

    return new Promise((resolve, reject) => {
        stringifier.pipe(fs.createWriteStream(resultsFile))
            .on('error', e => reject(e))
            .on('end', () => resolve())
        
        //header for the csv file
        stringifier.write(['id', 'name', 'count'])
        
        electionData.forEach((countyMap, stateCode) => {
            for ([code, {name, count}] of countyMap) {
                stringifier.write([code, name, count])
            }
        })
        stringifier.end()
    })
}


module.exports = {
    saveStateElectionData: saveStateElectionData
}
