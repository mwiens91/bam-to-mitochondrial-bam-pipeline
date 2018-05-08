"""The actual pipeline for bam-to-mt-bam-pipeline."""

from __future__ import print_function
import os
import azure.storage.blob
import pypeliner


def download_blob(azure_storage_account_name,
                  azure_storage_account_key,
                  storage_container_name,
                  output_file_name,
                  blob_name,):
    """Downloads a blob from Azure."""
    # This is used to interact with Azure blobs
    block_blob_service = azure.storage.blob.BlockBlobService(
                            account_name=azure_storage_account_name,
                            account_key=azure_storage_account_key,)

    # Download the blob
    block_blob_service.get_blob_to_path(container_name=storage_container_name,
                                        blob_name=blob_name,
                                        file_path=output_file_name)

def upload_blob(azure_storage_account_name,
                azure_storage_account_key,
                storage_container_name,
                input_file_name,
                blob_name,):
    """Uploads a file to an Azure blob."""
    # Gets the absolute path of the input file. I think this is required
    # by the Azure Storage Python library.
    input_file_path = os.path.abspath(input_file_name)

    # This is used to interact with Azure blobs
    block_blob_service = azure.storage.blob.BlockBlobService(
                            account_name=azure_storage_account_name,
                            account_key=azure_storage_account_key,)

    # Upload the file as a blob
    block_blob_service.create_blob_from_path(container_name=storage_container_name,
                                             blob_name=blob_name,
                                             file_path=input_file_path)

def create_bam_to_mt_bam_pipeline(source_account_name,
                                  source_account_key,
                                  source_storage_container_name,
                                  destination_account_name,
                                  destination_account_key,
                                  destination_storage_container_name,
                                  input_blob_name,
                                  output_blob_name,):
    """Creates the main pipeline.

    Creates a pipeline to download a cell's BAM files and extract their
    mitochondrial data.
    """
    # Create the main workflow
    workflow = pypeliner.workflow.Workflow()

    # Download the blob
    workflow.transform(
            name='download_blob',
            func=download_blob,
            args=(
                source_account_name,
                source_account_key,
                source_storage_container_name,
                input_blob_name,
                pypeliner.managed.TempOutputFile(input_blob_name),
            ),
    )

    # Get the mitochondrial data
    workflow.commandline(
            name='bam_to_mitochondrial_bam',
            args=(
                'samtools',
                'view',
                '-b',
                pypeliner.managed.TempInputFile(input_blob_name),
                'MT',
                '>',
                pypeliner.managed.TempOutputFile(output_blob_name),
            ),
    )

    # Upload the mitochondrial data to a blob
    workflow.transform(
            name='upload_blob',
            func=upload_blob,
            args=(
                destination_account_name,
                destination_account_key,
                destination_storage_container_name,
                pypeliner.managed.TempInputFile(output_blob_name),
                output_blob_name,
            ),
    )

    return workflow
