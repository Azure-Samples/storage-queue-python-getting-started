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

from random_data import RandomData
from azure.storage.queue import QueueServiceClient

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
class QueueBasicSamples():

    def __init__(self):
        self.random_data = RandomData()

    # Runs all samples for Azure Storage Queue service.
    def run_all_samples(self, connection_string):
        try:
            print('Azure Storage Basic Queue samples - Starting.')
            
            # declare variables
            queuename = "queuesample" + self.random_data.get_random_name(6)
            queuename2 = "queuesample" + self.random_data.get_random_name(6)
            
            # create a new queue service that can be passed to all methods
            queue_service = QueueServiceClient.from_connection_string(conn_str=connection_string)

            # Basic queue operations such as creating a queue and listing all queues in your account
            print('\n\n* Basic queue operations *\n')
            self.basic_queue_operations(queue_service, queuename, queuename2)

            # Add a message to a queue in your account
            print('\n\n* Basic message operations *\n')
            self.basic_queue_message_operations(queue_service, queuename)

        except Exception as e: 
            print('Error occurred in the sample.', e)
        finally:
            # Delete the queues from your account
            self.delete_queue(queue_service, queuename)
            self.delete_queue(queue_service, queuename2)
            print('\nAzure Storage Basic Queue samples - Completed.\n')

    # Basic queue operations including creating and listing
    def basic_queue_operations(self, queue_service, queuename, queuename2):
        # Create a queue or leverage one if already exists
        print('Attempting create of queue: ', queuename)
        queue_service.create_queue(queuename)
        print('Successfully created queue: ', queuename)

        # Create a second queue or leverage one if already exists
        print('Attempting create of queue: ', queuename2)
        queue_service.create_queue(queuename2)
        print('Successfully created queue: ', queuename2)

        #List all queues with prefix "queuesample"
        print('Listing all queues with prefix "queuesample"')
        queues = queue_service.list_queues("queuesample")
        for queue in queues:
            print('\t', queue.name)
    
    # Basic queue operations on messages
    def basic_queue_message_operations(self, queue_service, queuename):
        # Add a number of messages to the queue.
        # if you do not specify time_to_live, the message will expire after 7 days
        # if you do not specify visibility_timeout, the message will be immediately visible
        messagename = "test message"
        queue_client = queue_service.get_queue_client(queuename)
        for i in range(1, 10):
            queue_client.send_message(messagename + str(i))
            print ('Successfully added message: ', messagename + str(i))

        # Get length of queue
        # Retrieve queue metadata which contains the approximate message count ie.. length. 
        # Note that this may not be accurate given dequeueing operations that could be happening in parallel
        queue_properties = queue_client.get_queue_properties()
        metadata = queue_properties.metadata
        length = queue_properties.approximate_message_count
        print('Approximate length of the queue: ', length)

        # Look at the first messages only without dequeueing it
        messages = queue_client.peek_messages()
        for message in messages:
            print('Peeked message content is: ', message.content)

        # Look at the first 5 messages only without any timeout without dequeueing it
        messages = queue_client.peek_messages(max_messages=5)
        for message in messages:
            print('Peeked message content is: ', message.content)

        # Update the visibility timeout of a message 
        # You can also use this operation to update the contents of a message.
        print('Update the visibility timeout of a message')
        messages = queue_client.receive_messages()
        message = next(messages)
        queue_client.update_message(message=message.id, pop_receipt=message.pop_receipt, visibility_timeout=300)

        # Dequeuing a message
        # First get the message, to read and process it.
        #  Specify num_messages to process a number of messages. If not specified, num_messages defaults to 1
        #  Specify visibility_timeout optionally to set how long the message is visible
        messages = queue_client.receive_messages()
        for message in messages:
            print('Message for dequeueing is: ', message.content)
            # Then delete it.
            # When queue is deleted all messages are deleted, here is done for demo purposes 
            # Deleting requires the message id and pop receipt (returned by get_messages)
            queue_client.delete_message(message=message.id, pop_receipt=message.pop_receipt)
            print('Successfully dequeued message')

        # Clear out all messages from the queue
        queue_client.clear_messages()
        print('Successfully cleared out all queue messages')

    # Delete the queue
    def delete_queue(self, queue_service, queuename):
        # Delete the queue. 
        # Warning: This will delete all the messages that are contained in it.
        print('Attempting delete of queue: ', queuename)
        queue_service.delete_queue(queuename)    
        print('Successfully deleted queue: ', queuename)