#!/usr/bin/env python
"""Main script for bam-to-mitochondrial-bam-pipeline."""

from __future__ import print_function
import sys
import azure.storage.blob
import pypeliner
import yaml
import bam_to_mt_bam_pipeline


# Contains Azure info; path is relative to script directory
SETTINGS_FILE = 'settings.yaml'

def main():
    """The main program for bam-to-mitochondrial-bam-pipeline."""
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
         account_name=settings_dict['AZURE_SOURCE_STORAGE_ACCOUNT_NAME'],
         account_key=settings_dict['AZURE_SOURCE_STORAGE_ACCOUNT_KEY'],)

    # Get a list of blobs to download
    blob_list = []

    # Compile a list of all the BAM files for all of the chips we care
    # about.
    for cell_id in settings_dict['SOURCE_CHIP_IDS']:
        # Go through all files in the blob
        for blob in block_blob_service.list_blobs(
                container_name=settings_dict['SOURCE_STORAGE_CONTAINER_NAME'],
                prefix=(settings_dict['SOURCE_CHIP_PATH_PREFIX']
                        + '/'
                        +  cell_id)):
            # Filter out any non-BAM files
            if blob.name.endswith('.bam'):
                # Add the path of the blob
                blob_list.append(blob.name)

    # Make the blob list a dict to use for the pipeline
    blob_dict = {i: blob_list[i] for i in range(len(blob_list))}

    # Now send all of the blobs down the pipeline
    pyp = pypeliner.app.Pypeline(modules=(bam_to_mt_bam_pipeline,),
                                 config={'sentinal_only': True,
                                         'loglevel': 'DEBUG',
                                         'nocleanup': False,
                                         'submit': 'local',
                                         'maxjobs': 64 * 2})

    # Make the workflow
    workflow = pypeliner.workflow.Workflow()

    # Set an object for parallelization
    workflow.setobj(obj=pypeliner.managed.TempOutputObj('blobname', 'blob'),
                    value=blob_dict)

    workflow.subworkflow(
            name='bam_to_mt_bam_pipeline',
            func=bam_to_mt_bam_pipeline.create_bam_to_mt_bam_pipeline,
            axes=('blob',),
            args=(
                settings_dict['AZURE_SOURCE_STORAGE_ACCOUNT_NAME'],
                settings_dict['AZURE_SOURCE_STORAGE_ACCOUNT_KEY'],
                settings_dict['SOURCE_STORAGE_CONTAINER_NAME'],
                settings_dict['AZURE_DESTINATION_STORAGE_ACCOUNT_NAME'],
                settings_dict['AZURE_DESTINATION_STORAGE_ACCOUNT_KEY'],
                settings_dict['DESTINATION_STORAGE_CONTAINER_NAME'],
                pypeliner.managed.TempInputObj('blobname', 'blob'),
            )
    )

    # Run it
    pyp.run(workflow)

if __name__ == '__main__':
    # Run the program
    main()
