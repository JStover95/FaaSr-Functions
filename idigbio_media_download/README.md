# idigbio_media_download

This example downloads 10 media records from the [iDigBio](https://idigbio.org) biodiversity specimen record/media aggregator using its [ridigbio](https://cran.r-project.org/web/packages/ridigbio/index.html) API, queried by genus, and stores them in an S3 bucket

The name of the folder in the S3 bucket, and the genus to query, are both specified as values in the *idigbio_media_download.json* file; they default to *idigbio* and *acer*, respectively.



