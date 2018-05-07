"""The actual pipeline for bam2MTbam pipeline."""

from __future__ import print_function
import pypeliner


def download_blob(azure_storage_account_name,
                  azure_storage_account_key,
                  storage_container_name,
                  blob_name,
                  output_file_name,):
    """Downloads a blob from Azure."""
    # This is used to interact with Azure blobs
    block_blob_service = azure.storage.blob.BlockBlobService(
                      account_name=settings_dict['AZURE_STORAGE_ACCOUNT_NAME'],
                      account_key=settings_dict['AZURE_STORAGE_ACCOUNT_KEY'],)

    # Download the blob
    block_blob_service.get_blob_to_path(container_name=storage_container_name,
                                        blob_name=blob_name,
                                        file_path=output_file_name)

def create_bam2MTbam_pipeline(azure_storage_account_name,
                              azure_storage_account_key,
                              storage_container_name,
                              blob_name,
                              output_file_name,):
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
                azure_storage_account_name,
                azure_storage_account_key,
                blob_name,
                pypeliner.managed.TempOutputFile(blob_name)
            ),
    )

    # Get the mitochondrial data
    workflow.commandline(
            name='bam_to_mitochondrial_bam',
            args=(
                'samtools',
                'view',
                '-b',
                pypeliner.managed.TempInputFile(blob_name),
                'MT',
                '>',
                pypeliner.managed.OutputFile(output_file_name)
            ),
    )

    return workflow
