# Right of Way (ROW) Reporting

Scripts that pull data from AMANDA and other data sources to enable the city to report on the work by the [Transportation & Public Works Department's Right of Way Management Division.](https://www.austintexas.gov/department/right-way-row-management)

![a flow diagram depicting data going from our source AMANDA DB to our destination city datahub.](docs/flow_diagram.png)

## AMANDA

AMANDA is the backend system that underlies the [Austin Build + Connect](https://abc.austintexas.gov/index) portal which the ROW division uses to manage permitting. 

`amanda_to_s3.py` is a script that allows us to run queries on the AMANDA read replica oracle DB and store the result as a .csv file in an AWS S3 bucket. To run this script, select one of the predefined queries in `queries.py` and provide the key as a parameter to the script.

`python amanda/amanda_to_s3.py --query applications_received`

### Queries

- `applications_received`: Gets the count of the number of right of way (ROW) permits received by day and folder type.
- `active_permits`: Gets the current number of active ROW permits by type.
- `issued_permits`: Gets the count of the number ROW permits issued by day and folder type.
- `review_time`: Gets a list of dates of different processes of a RW permit's review timeline.
- `ex_permits_issued`: Gets the list of EX permits and their indate and issuedate
- `license_agreements_timeline`: Gets a list of license reviews and a series of dates of review completion dates. 
- `lde_site_plan_revisions`: Gets a list of land development engineering reviews and key dates for reviews and their due dates.
- `row_inspector_permit_list`: Returns all active permits assigned to the Right of Way division.
- `row_inspector_segment_list`: Returns roadway segment IDs associated with active permits
- `tds_cases`: Returns the list of cases involving Transportation Development Services (TDS) and the cycle number.
- `tds_asmd_map`: SIF information related to Site Plan, Subdivision, and Zoning for TDS cases

## Smartsheet

[Smartsheet](https://www.smartsheet.com/) is an additional tool the ROW team uses to manage some types of permits. `smartsheet_to_s3.py` downloads all of the data from the predefined list of sheets in `sheets.py` and stores the data as a .csv file in an AWS S3 bucket. There are no parameters for this script.

`python smartsheet/smartsheet_to_s3.py`

## Metrics

This subdirectory stores the scripts that processes the data from AMANDA and/or smartsheet for reporting purposes.

### Quick Reporting

Quick reporting is enabled by setting up an entry in `socrata_config.py`, for a CSV from an AMANDA query that is run against the DB
in `amanda_to_s3.py`. Once a dataset has been created in Socrata along with the appropriate config, one can update a dataset with:

`python metrics/s3_to_scorata.py --dataset license_agreements_timeline`

### High-level ROW Metrics

`active_permits_logging.py` posts the current number of active permits to the [city's data hub](https://datahub.austintexas.gov/login). 

`python metrics/active_permits_logging.py`

`row_data_summary.py` totals up the count of the various types of permits that were requested and issued to [city's data hub](https://datahub.austintexas.gov/login). 

`python metrics/row_data_summary.py`

### Inspector Permit Prioritization

`roadway_segment_tagging.py` retrieves [Austin roadway segment data](https://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services/TRANSPORTATION_street_segment/FeatureServer),
then tags it with the appropriate ROW inspector zone and Downtown Project Coordination Zone (DAPCZ). Running this script 
will update a dataset with these relationships stored. 

`python metrics/roadway_segment_tagging.py`

`inspector_prioritization.py` "scores" permits based on several metrics (which includes data from the above street segments) 
to rank permits based on a prioritization for ROW inspectors. Loads data from csvs in S3 from `amanda_to_s3.py`.

`python metrics/inspector_prioritization.py`

![a diagram describing each of the components of the inspector scoring](docs/row_inspector_scoring.png)





## Docker

This repo can be used with a docker container. You can either build it yourself with:

`docker build . -t atddocker/dts-right-of-way-reporting:local`

or pull from our dockerhub account:

`docker pull atddocker/dts-right-of-way-reporting:production`

Then, provide the environment variables described in env_template to the docker image:

`docker run -it --env-file env_file -v "$(pwd):/app" atddocker/dts-right-of-way-reporting:production /bin/bash` 

Then, provide the command you would like to run.
