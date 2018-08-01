![Python version](https://img.shields.io/badge/python-2-blue.svg)

# bam-to-mitochondrial-bam-pipeline

This pipeline script uses
[Pypeliner](https://pypeliner.readthedocs.io/en/latest/) to

- fetch some BAM files given a list of sequencing chip IDs from an Azure
  Blob Storage container
- extract the parts of the BAM files relevant to mitochondrial DNA and
  put them into smaller BAM files using Samtools
- upload those files to an Azure Blob Storage container (possibly the
  same container as used previously, though not necessarily)

## Setup

Fill in and rename [`settings.yaml.example`](settings.yaml.example) to
`settings.yaml`. Also [make sure you have Samtools
installed](http://www.htslib.org/).

## Usage

Run the main script with

```
./bam2MTbam.py
```
