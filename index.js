const fs = require('fs')


const stateResultsFilename = `${__dirname}/local_data/state_results.json`
const countyResultsFilename = `${__dirname}/local_data/county_results.json`


//process flags

//give hint if usage is incorrect

if (process.argv.length != 3) {
    console.log(`Usage: node ${__filename} [<output file>]`)
    process.exit()
}


