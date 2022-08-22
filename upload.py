import asyncio
import os, uuid
from azure.storage.blob import BlobBlock, ContainerClient, __version__, aio

class sample:

    # file generated with fallocate -l $(((4*1024*1024)+1)) FourMbPlus1.txt
    # in the terminal set the environment variable:
    # export AZURE_STORAGE_TEST_SAS_URI="https://<storageaccount>.blob.core.windows.net/<storage container name>?<access token portion>"

    def __init__(self) -> None:
        self.blocksize = 4*1024*1024

    def upload(self):
        try:
            self.connection_string = os.getenv("AZURE_STORAGE_TEST_SAS_URI")
            self.blob_name="FourMbPlus1.txt"

            print("Azure Blob Storage v" + __version__ + " " + self.connection_string)

            container_client = ContainerClient.from_container_url(self.connection_string)
            with open(self.blob_name, "rb") as data:
                blob_client = container_client.upload_blob(name=self.blob_name, data=data, overwrite=True)

            properties = blob_client.get_blob_properties()

            print("Stored size:" + str(properties.size))

        except Exception as ex:
            print('Exception:')
            print(ex)


    def block_upload(self):

        self.connection_string = os.getenv("AZURE_STORAGE_TEST_SAS_URI")
        self.blob_name="FourMbPlus1.txt"

        print("Azure Blob Storage v" + __version__ + " " + self.connection_string)

        try:

            container_client = ContainerClient.from_container_url(self.connection_string)

            destination_blob_client = container_client.get_blob_client("chunked_" + self.blob_name)
            
            block_list = []

            with open(self.blob_name, "rb") as data:
                # Read data in chunks to avoid loading all into memory at once
                chunk = data.read(self.blocksize)
            
                while len(chunk) > 0:
                    block_id = str(uuid.uuid4())
                    destination_blob_client.stage_block(block_id=block_id, data=chunk)
                    block_list.append(BlobBlock(block_id=block_id))
                    chunk = data.read(self.blocksize)

            destination_blob_client.commit_block_list(block_list)

            properties = destination_blob_client.get_blob_properties()

            print("Stored size:" + str(properties.size))

        except Exception as ex:
            print('Exception:')
            print(ex)

    async def block_upload_aio(self):

        self.connection_string = os.getenv("AZURE_STORAGE_TEST_SAS_URI")
        self.blob_name="FourMbPlus1.txt"

        print("Azure Blob Storage v" + __version__ + " " + self.connection_string)
        
        try:

            async with aio.ContainerClient.from_container_url(self.connection_string) as container_client:

                blobclient = container_client.get_blob_client("chunked_aio_" + self.blob_name)

                block_list = []

                async with blobclient as destination_blob_client:
                    with open(self.blob_name, "rb") as data:
                        
                        print('writing chunks')
                        chunk = data.read(self.blocksize)
                    
                        i = 1

                        while len(chunk) > 0:
                            print("Chunk: " + str(i))
                            block_id = str(uuid.uuid4())
                            
                            await destination_blob_client.stage_block(block_id=block_id, data=chunk)
                            block_list.append(BlobBlock(block_id=block_id))
                            chunk = data.read(self.blocksize)
                            i = i + 1

                    await destination_blob_client.commit_block_list(block_list)                            
                    properties = await blobclient.get_blob_properties()
                    print("Stored size:" + str(properties.size))

        except Exception as ex:
            print('Exception:')
            print(ex)


if __name__ == '__main__':
    sample = sample()
    sample.upload()
    sample.block_upload()
    asyncio.get_event_loop().run_until_complete(sample.block_upload_aio())
