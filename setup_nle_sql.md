## Cloud SQL

- Create and setup instance, as in [these instrucitons](https://cloud.google.com/sql/docs/postgres/quickstart?authuser=2)
  - Make sure to store the admin password somewhere
- create DB named "bagv2"
- Connect to instance, connect to DB, and install postgis, by running: `CREATE EXTENSION postgis;`
- Can verify by running `\dx`


## NLExtract

- relevant folder in NLE is 'bagv2'
- need to go to bag/v2/externals - and follow README.txt to download stetsl as submodule
- need to install all dependendcies: specifically gdal (which can be a pain):
  - First install dependencies (numpy and libgdal).
  - Verify which version of libgdal is installed: this can vary based on your OS.
  - Install via pip/poetry the same package version as the libgdal you have (for me, macOS 10/12/2021 this meant 3.3.0)
- main entry point is running `./NLExtract/bagv2/etl/stetl.sh`
- input arguments are in `NLExtract/bagv2/etl/options/common.args` and `NLExtract/bagv2/etl/options/default.args`

## Todo:

Manage to actually write data using NLE to the cloud instance. It could be simply inputting the right parameters in `default.args`, but a naive attempt failed. Possible reasons for failure:
1. Cloud SQL instance needs to be configured to receive data
2. The arguments that I tried inputting were not accurate
3. The arguments are not being handed down propely


- Next step is tracking these arguments from `./NLExtract/bagv2/etl/stetl.sh` to the actual command that uses them to make connection/write data, and see where it goes wrong.
- In parallel, research a bit on setting up Cloud SQL for external conenctions.
