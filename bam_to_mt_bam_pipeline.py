"""The actual pipeline for bam-to-mt-bam-pipeline."""

from __future__ import print_function
import os
import subprocess
import azure.storage.blob
import pypeliner


def rename_input_to_output(name):
    """Renames the input file to an output file.

    Specifically, this adds '_MT' to the filename before the .bam
    extension.
    """
    return name[:-4] + '_MT.bam'

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

def get_mitochondrial_data(bam_file,
                           mitochondrial_bam_file):
    """Extracts mitochondrial data from a BAM file."""
    # Generate index file
    subprocess.check_call(['samtools', 'index', bam_file])

    # Extract the mitochondrial data
    subprocess.check_call(['samtools', 'view', '-b', bam_file, 'MT',
                     '>', mitochondrial_bam_file])

    # Clean up the index file
    os.remove(bam_file + '.bai')

def create_bam_to_mt_bam_pipeline(source_account_name,
                                  source_account_key,
                                  source_storage_container_name,
                                  destination_account_name,
                                  destination_account_key,
                                  destination_storage_container_name,
                                  input_blob_name):
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
                pypeliner.managed.TempOutputFile(input_blob_name),
                input_blob_name,
            ),
    )

    # Get the mitochondrial data
    workflow.transform(
            name='bam_to_mitochondrial_bam',
            func=get_mitochondrial_data,
            args=(
                pypeliner.managed.TempInputFile(input_blob_name),
                pypeliner.managed.TempOutputFile(
                    rename_input_to_output(input_blob_name)),
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
                pypeliner.managed.TempInputFile(
                    rename_input_to_output(input_blob_name)),
                rename_input_to_output(input_blob_name),
            ),
    )

    return workflow
