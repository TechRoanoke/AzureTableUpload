AzureTableUpload
================

This is a command line tool for uploading a CSV file into Azure Table Storage. 

* **Client-side validation**
The tool will do client-side validation of various Azure Tables rules, including validating that the Partition and Row Keys don't have illegal characters. This provides a much more user friendly experience than a worker role crashing with a random Storage SDK exception.
It will also provide statistics about the total rows and variance of partition keys. 

* **Local ingress**
It can upload the table from your local machine (which is much slower, since it's outside the datacenter). 
It will automatically batch by partition key. 
It will warn about illegal rows, but not abort the upload. 

* **Upload Service** 
Ingress from within a data-center can be 10x faster than ingress from your local machine.
This also includes a Worker Role service that can be deployed to a data-center. 
The client tool will zip your CSV (which can make it 5x smaller), upload the compressed file to blob, and then queue a message to the service to tell it to ingress the blob to a table. 

* **Progress Reporting** 
Since upload can take hours, the ingress tool will provide a live progress report including how many rows are updated and how long has been spent in IO.
The client can query progress while the server is working. 

* **Persistent resumable upload**
If the upload crashes (ie, the worker role is rebooted, or the local machine looses network connectivity), the ingressor can use the progress-reporting information to resume from where it last left. 
This also means you can begin an ingress of a large file locally, cancel it, and then kick off an ingress to the service. This way, you can still test against the table rows that are already uploaded.


