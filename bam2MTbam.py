#!/usr/bin/env python
"""Main program for bam2MTbam pipeline."""

from __future__ import print_function
import sys
import azure.storage.blob
import pypeliner
import yaml
from bam2MTbam_pipeline import create_bam2MTbam_pipeline


# Contains Azure info; path is relative to script directory
SETTINGS_FILE = 'settings.yaml'

def main():
    """The main program for bam2MTbam_pypeline."""
    # Get Azure info from settings file
    try:
        with open('settings.yaml', 'r') as settingsfile:
            settings_dict = yaml.load(settingsfile)
    except IOError:
        # `settings.yaml` not found
        print("Please rename settings.yaml.example to settings.yaml"
              " and fill in the variables with your values."
              " Exiting now.")
        sys.exit(1)

    # This is used to interact with Azure blobs
    block_blob_service = azure.storage.blob.BlockBlobService(
                      account_name=settings_dict['AZURE_STORAGE_ACCOUNT_NAME'],
                      account_key=settings_dict['AZURE_STORAGE_ACCOUNT_KEY'],)

    # Get a list of blobs to download
    blob_list = []

    # Compile a list of all the BAM files for all of the chips we care
    # about.
    for cell_id in settings_dict['CHIP_IDS']:
        # Go through all files in the blob
        for blob in block_blob_service.list_blobs(
                        container_name=settings_dict['STORAGE_CONTAINER_NAME'],
                        prefix=settings_dict['CHIP_PATH_PREFIX']
                                + '/'
                                +  cell_id):
            # Filter out any non-BAM files
            if blob.name.endswith('.bam'):
                # Add the path of the blob
                blob_list.append(blob.name)

    # Now send all of the blobs down the pipeline
    pyp = pypeliner.app.Pypeline()

    for blobname in blob_list:
        # Add '_MT' to the filename before the .bam extension
        output_filename = blobname[:-4] + 'MT.bam'

        # Create the workflow
        thisworkflow = create_bam2MTbam_pipeline(
                account_name=settings_dict['AZURE_STORAGE_ACCOUNT_NAME'],
                account_key=settings_dict['AZURE_STORAGE_ACCOUNT_KEY'],
                storage_container_name=settings_dict['STORAGE_CONTAINER_NAME'],
                blob_name=blobname,
                output_filename=output_filename,)

        # Run the workflow
        pypa.sch.run(thisworkflow)


if __name__ == '__main__':
    # Run the program
    main()
