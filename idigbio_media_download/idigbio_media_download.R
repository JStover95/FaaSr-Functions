idigbio_media_download <- function(folder, genus) {

  # Example function that downloads 10 media records from an iDigBio search (using ridigbio package)
  # then extracts URL from records, download, and save as images in S3 bucket, with name UUID.jpg
  # folder is the name of the S3 folder to store images in
  # genus is the genus to use in searching search

  # Use ridigbio to download media records into data frame df
  library('ridigbio')
  options(timeout=360)
  df <- idig_search_media(rq=list(genus=genus),
                        mq=list("data.ac:accessURI"=list("type"="exists")),
                        fields=c("uuid","data.ac:accessURI"), limit=10)

  # Iterate over 10 records
  for (x in 1:10) {
    # This is the name we'll save the image as
    fn = paste(df$`uuid`[x],".jpg",sep="")
    # Download image from provider's accessURI
    download.file(df$`data.ac:accessURI`[x],destfile=fn)
    # Save image into S3
    faasr_put_file(local_file=fn, remote_folder=folder, remote_file=fn)
  }

}