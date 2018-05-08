# bam-to-mitochondrial-bam-pipeline

This is a script to

- fetch some BAM files given a list of sequencing chip IDs from an Azure
  Blob Storage container
- extract the parts of the BAM files relevant to mitochondrial DNA and
  put them into smaller BAM files using Samtools
- upload those files to an Azure Blob Storage container (possibly the
  same container as used previously, though not necessarily)

## Usage

**Important**. Fill in and rename `settings.yaml.example` to
`settings.yaml`. Also make sure you have Samtools installed
(http://www.htslib.org/).

Once you've done that run the main script with `./bam2MTbam.py`.
