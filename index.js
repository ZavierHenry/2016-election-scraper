const scraper = require('./data_scrapers/scrapers')

//give hint if usage is incorrect

if (process.argv.length > 3) {
    console.log(`Usage: node ${__filename} [<output file>]`)
    process.exit()
}

if (process.argv.length == 3) {
    //save to indicated file
    let [_a, _b, location] = process.argv,
        extension = location.slice(location.lastIndexOf('.'))

    switch (extension) {
        case '.json':
            //write to json file
            scraper.saveElectionDataJSON(location)
                .catch(err => console.error(`An error occurred while saving election results to JSON: ${err}`))
            break
        case '.txt':
        case '.csv':
            //write to csv file
            scraper.saveElectionDataCSV(location)
                .catch(err => console.error(`An error occurred while saving election results as CSV: ${err}`))
            break
        default:
            console.error('Unsupported file extension: %s', extension)
            console.error('Supported file extensions: .json, .csv, .txt')
            process.exit(1)

    }

} else {
    let defaultFilename = "election_results.txt"
    scraper.saveElectionDataCSV(defaultFilename)
        .catch(err => console.error(`An error occurred while saving election results to CSV:\n\n${err}`))
}