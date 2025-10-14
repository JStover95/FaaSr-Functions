# tutorial

Functions and workflow configurations for the basic FaaSr tutorial.

## Workflows

- tutorial.json: main FaaSr tutorial workflow

## Functions

- create_sample_data.R: creates two sample CSV files (e.g. sample1.csv and sample2.csv) and save to S3 bucket
- compute_sum.R: compute the sum of two CSV files (e.g. sample1.csv and sample2.csv) and saves the output to S3 bucket (e.g. sum.csv)
- compute_sum_arrow.R: compute the sum of two CSV files using the Arrow package
- compute_mult.R: compute the multiplication of two CSV files
- compute_div.R: compute the multiplication of two CSV files
