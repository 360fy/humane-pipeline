# humane-pipeline - v2.0.1

humane-pipeline is a tool for managing pipeline of data source to transformations to destination(s). You can use it to read data from various sources, modify and enhance data through various transformations, add additional data through external data mapper, and finally write data to various destinations (one or many). Pipeline can also be forked into branches for multiple type of transformations and destinations.

It is fully free and open source. The license is LGPL 3.0, meaning you are pretty much free to use it in most ways.
 
It is part of a bigger ecosystem of humane-discovery platform, but is installable, runnable independent of that.

## Installation

`npm install -g humane-pipeline`

## Input Sources

- `file`
- `directory watch`
- `file pattern watch`
- `file tail watch`
- `sql`
- `sql incremental watch`

## Transformations

### Json transformations

- filter
- pick
- pickBy
- omit
- omitBy
- reduce
- groupBy
- keys
- values
- map
- extMap

### Transforming file data to JSON

- log
- json
- jsonArray
- csvToJson

### Transforming JSON to flat

- jsonToCsv

## Output Sources

- `file` - text file or json file
 
## Contributing
 
 All contributions are welcome: ideas, patches, documentation, bug reports, complaints, and even something you drew up on a napkin.
 

