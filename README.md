# ergo-oracle-stats-backend
Oracle Pool stats provider

Standalone API service providing:
- [x] list of known oracle pools

Pool specific data:
- [ ] creation date, settings, contract addresses
- [ ] total number of oracle tokens
- [ ] activated oracle tokens (used at least once)
- [x] timeseries of datapoints
- [ ] total funding and spending to date
- [x] number of datapoints posted to date
- [ ] timeseries of active oracles (oracles submitting a datapoint)
- [ ] timeseries of rejected datapoints
- [ ] timeseries of smartcontracts using a pool

Oracle specific data (for a given pool):
- [ ] first posting date
- [ ] last posting date
- [x] number of datapoints submitted to date
- [ ] number of rejected datapoints to date
- [ ] number of collections to date
- [ ] costs and rewards to date
- [x] timeseries of datapoints




