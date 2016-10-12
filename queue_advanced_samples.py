#----------------------------------------------------------------------------------
# Microsoft Developer & Platform Evangelism
#
# Copyright (c) Microsoft Corporation. All rights reserved.
#
# THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, 
# EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES 
# OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
#----------------------------------------------------------------------------------
# The example companies, organizations, products, domain names,
# e-mail addresses, logos, people, places, and events depicted
# herein are fictitious.  No association with any real company,
# organization, product, domain name, email address, logo, person,
# places, or events is intended or should be inferred.
#----------------------------------------------------------------------------------

from azure.storage import CloudStorageAccount, AccessPolicy
from azure.storage.queue import Queue, QueueService, QueueMessage, QueuePermissions
from azure.storage.models import CorsRule, Logging, Metrics, RetentionPolicy
from azure.common import AzureException
import config
from random_data import RandomData
import datetime

# -------------------------------------------------------------
# <summary>
# Azure Queue Service Sample - The Queue Service provides reliable messaging for workflow processing and for communication 
# between loosely coupled components of cloud services. This sample demonstrates how to perform common tasks including 
# inserting, peeking, getting and deleting queue messages, as well as creating and deleting queues. 
# 
# Documentation References: 
# - What is a Storage Account - http://azure.microsoft.com/en-us/documentation/articles/storage-whatis-account/
# - Getting Started with Queues - https://azure.microsoft.com/en-us/documentation/articles/storage-python-how-to-use-queue-storage/
# - Queue Service Concepts - http://msdn.microsoft.com/en-us/library/dd179353.aspx
# - Queue Service REST API - http://msdn.microsoft.com/en-us/library/dd179363.aspx
# - Queue Service Python API - http://azure.github.io/azure-storage-python/ref/azure.storage.queue.html
# - Storage Emulator - http://msdn.microsoft.com/en-us/library/azure/hh403989.aspx
# </summary>
# -------------------------------------------------------------
 
class QueueAdvancedSamples():

    def __init__(self):
        self.random_data = RandomData()

    # Runs all samples for Azure Storage Queue service.
    # Input Arguments:
    # account - CloudStorageAccount to use for running the samples
    def run_all_samples(self, account):
        try:
            print('Azure Storage Advanced Queue samples - Starting.')

            # create a new queue service that can be passed to all methods
            queue_service = account.create_queue_service()

            print('\n\n* List queues *\n')
            self.list_queues(queue_service)
        
            print('\n\n* Set cors Rules *\n')
            self.set_cors_rules(queue_service)
        
            print('\n\n* ACL operations *\n')
            self.queue_acl_operations(queue_service)
            
            print('\n\n* Set service logging and metrics properties *\n')
            self.set_service_properties(queue_service)
            
            print('\n\n* Set queue metadata *\n')
            self.metadata_operations(queue_service)
                    
        except Exception as e:
            if (config.IS_EMULATED):
                print('Error occurred in the sample. Please make sure the Storage emulator is running.', e)
            else: 
                print('Error occurred in the sample. Please make sure the account name and key are correct.', e)
        finally:
            print('\nAzure Storage Advanced Queue samples - Completed\n')
    
    # Manage queues including, creating, listing and deleting
    def list_queues(self, queue_service):
        queue_prefix = "queuesample" + self.random_data.get_random_name(6)
        
        try:
            print('1. Create multiple queues with prefix: ', queue_prefix)
            
            for i in range(5):
                queue_service.create_queue(queue_prefix + str(i))
            
            print('2. List queues with prefix: ', queue_prefix)
            
            queues =  queue_service.list_queues(queue_prefix)
            
            for queue in queues:
                print('  Queue name:' + queue.name)

        finally:                
            print('3. Delete queues with prefix:' + queue_prefix) 
            for i in range(5):
                if queue_service.exists(queue_prefix + str(i)):
                    queue_service.delete_queue(queue_prefix + str(i))
    
        print("List queues sample completed")
    
    # Manage CORS rules
    def set_cors_rules(self, queue_service):

        cors_rule = CorsRule(
            allowed_origins=['*'], 
            allowed_methods=['POST', 'GET'],
            allowed_headers=['*'],
            exposed_headers=['*'],
            max_age_in_seconds=3600)

        try:
            print('1. Get Cors Rules')
            original_cors_rules = queue_service.get_queue_service_properties().cors
            
            print('2. Overwrite Cors Rules')
            queue_service.set_queue_service_properties(cors=[cors_rule])

        finally:        
            print('3. Revert Cors Rules back the original ones')
            #reverting cors rules back to the original ones
            queue_service.set_queue_service_properties(cors=original_cors_rules)
        
        print("CORS sample completed")
        
    # Manage properties of the Queue service, including logging and metrics settings, and the default service version.
    def set_service_properties(self, queue_service):

        try:
            print('1. Get Queue service properties')
            props = queue_service.get_queue_service_properties();

            retention = RetentionPolicy(enabled=True, days=5)
            logging = Logging(delete=True, read=False, write=True, retention_policy=retention)
            hour_metrics = Metrics(enabled=True, include_apis=True, retention_policy=retention)
            minute_metrics = Metrics(enabled=False)

            print('2. Ovewrite Queue service properties')
            queue_service.set_queue_service_properties(logging=logging, hour_metrics=hour_metrics, minute_metrics=minute_metrics)
        finally:
            print('3. Revert Queue service properties back to the original ones')
            queue_service.set_queue_service_properties(logging=props.logging, hour_metrics=props.hour_metrics, minute_metrics=props.minute_metrics)

        print('4. Set Queue service properties completed')
    
    # Manage metadata of a queue
    def metadata_operations(self, queue_service):
        queue_name = 'queue' + self.random_data.get_random_name(6)

        try:
            # Create a new queue
            print('1. Create a queue with custom metadata - ' + queue_name)
            queue_service.create_queue(queue_name, {'category':'azure-storage', 'type': 'queue-sample'})
            
            # Get all the queue metadata 
            print('2. Get queue metadata')

            metadata = queue_service.get_queue_metadata(queue_name)
            
            print('    Metadata:')

            for key in metadata:
                print('        ' + key + ':' + metadata[key])

        finally:
            # Delete the queue
            print("3. Delete Queue")
            if queue_service.exists(queue_name):
                queue_service.delete_queue(queue_name)
    
    # Manage access policy of a queue
    def queue_acl_operations(self, queue_service):
        queue_name = 'aclqueue' + self.random_data.get_random_name(6)

        try:        
            print('1. Create a queue with name - ' + queue_name)
            queue_service.create_queue(queue_name)
                
            print('2. Set access policy for queue')
            access_policy = AccessPolicy(permission=QueuePermissions.READ,
                                        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=1))
            identifiers = {'id': access_policy}
            queue_service.set_queue_acl(queue_name, identifiers)

            print('3. Get access policy from queue')
            acl = queue_service.get_queue_acl(queue_name)

            print('4. Clear access policy in queue')
            # Clear
            queue_service.set_queue_acl(queue_name)

        finally:
            print('5. Delete queue')
            if queue_service.exists(queue_name):
                queue_service.delete_queue(queue_name)
            
        print("Queue ACL operations sample completed")