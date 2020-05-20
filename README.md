# GDPR Compliant Systems

- Run `./gen_data.py` to generate user shard csv files to be imported as databases
- Setup MySQL (grant user `tslilyai@localhost` with password `pass` all permissions to database)
- Run `go test --timeout 20m` to generate baseline and test timings for reads/updates/deletions in `[reads/updates/deletes]_[baseline/test].csv`
