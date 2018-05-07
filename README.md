# bam2MTbam-pipeline

This is a pipeline to fetch some BAM files from a Microsoft Azure Blob
Storage service container, and then to extract the parts relevant to
mitochondrial DNA and put them in (smaller) BAM files using Samtools
(http://www.htslib.org/).

## Usage

Fill in and rename `settings.yaml.example` to `settings.yaml`, then run
the main script `bam2MTbam.py`.

Make sure you have Samtools installed.
