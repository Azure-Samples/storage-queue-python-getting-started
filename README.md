---
services: storage
platforms: python
author: dineshmurthy
---

# Azure Storage: Getting Started with Azure Storage in Python
Samples documenting basic operations with Azure Queue storage services in Python. 

## Running this sample
This sample can be run using either the Azure Storage Emulator (Windows) or by using your Azure Storage account name and key. Please update the config.py file with the appropriate properties.

To run the sample using the Storage Emulator:
1. Download and install the Azure Storage Emulator https://azure.microsoft.com/en-us/downloads/ 
2. Start the emulator (once only) by pressing the Start button or the Windows key and searching for it by typing "Azure Storage Emulator". Select it from the list of applications to start it.
3. Run the project. 

To run the sample using the Storage Service
1. Create a Storage Account through the Azure Portal and provide your STORAGE_CONNECTION_STRING in the config.py file. See https://azure.microsoft.com/en-us/documentation/articles/storage-create-storage-account/ for more information.
2. Set breakpoints and run the project. 

## Deploy this sample 

Either fork the sample to a local folder or download the zip file from https://github.com/Azure-Samples/storage-queue-python-getting-started/

To get the source code of the SDK via git, type:
git clone git://github.com/Azure-Samples/storage-queue-python-getting-started.git
cd .\storage-queue-python-getting-started

## Minimum Requirements
Python 2.7, 3.3, or 3.4.
To install Python, please go to https://www.python.org/downloads/

## More information
  - What is a Storage Account - https://azure.microsoft.com/en-us/documentation/articles/storage-whatis-account/  
  - Getting Started with Queues - https://azure.microsoft.com/en-us/documentation/articles/storage-python-how-to-use-queue-storage/
  - Queue Service Concepts - https://msdn.microsoft.com/en-us/library/dd179353.aspx
  - Queue Service REST API - https://msdn.microsoft.com/en-us/library/dd179363.aspx
  - Queue Service Python API - https://azure.github.io/azure-storage-python/ref/azure.storage.queue.html
  - Storage Emulator - https://azure.microsoft.com/en-us/documentation/articles/storage-use-emulator/
