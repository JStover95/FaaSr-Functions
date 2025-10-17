# tutorial

Functions and workflow configurations for the basic FaaSr tutorial.

## Workflows

### Basic workflows

- tutorial.json: basic FaaSr tutorial workflow with two R functions (create_sample_data, compute_sum) and using GitHub Actions
- tutorialRpy.json: basic FaaSr tutorial workflow with one R function (create_sample_data) and one Python function (compute_sum) and using GitHub Actions

### Additional workflows
- tutorialLarger.json: a larger workflow graph consisting of multiple R and Python functions, including use of Arrow for R
- tutorialOW.json: basic FaaSr tutorial workflow using OpenWhisk for compute_sum function. Requires an OpenWhisk server and `OW_APIkey` secret

## Functions

- create_sample_data.R: creates two sample CSV files (e.g. sample1.csv and sample2.csv) and save to S3 bucket
- compute_sum.R: compute the sum of two CSV files (e.g. sample1.csv and sample2.csv) and saves the output to S3 bucket (e.g. sum.csv)
- compute_sum_arrow.R: compute the sum of two CSV files using the Arrow package
- compute_mult.R: compute the multiplication of two CSV files
- compute_div.R: compute the multiplication of two CSV files
