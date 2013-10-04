#!/usr/bin/env python

import boto.glacier

from boto.glacier.layer1 import Layer1
from boto.glacier.vault import Vault
from boto.glacier.job import Job
from boto.glacier.concurrent import ConcurrentUploader
from boto.glacier.concurrent import ConcurrentDownloader


import sys
import os.path
import time

from datetime import date


access_key_id ="YOUR_KEY_ID_GOES_HERE"
secret_key = "YOUR_SECRET_KEY_GOES_HERE"

job_check_delay = 600


glacier_layer1 = None

def print_usage(program_name="glacier.py"):
    """Prints out standard help information."""
    print("Usage: %s <command> <options>"%program_name)
    print(" ")
    print("    upload datacenter:vault filename description")
    print("    download datacenter:vault archiveID outputfile")

def is_valid_vault(vault, datacenter='us-east-1', layer1=None):
    if layer1 is None:
        layer1 = Layer1(aws_access_key_id=access_key_id, aws_secret_access_key=secret_key, region_name=datacenter)
    
    vault_list = [x['VaultName'] for x in layer1.list_vaults()['VaultList']]

    return vault in vault_list

def getVaultTuple(vaultString):
    vault = vaultString
    datacenter = 'us-east-1'
    if ':' in vault:
        location_of_colon = vault.find(':')
        datacenter = vault[:location_of_colon]
        vault = vault[location_of_colon+1:]
    return (datacenter, vault)

def uploadCommand(arguments):
    filename = arguments[1]
    datacenter, vault = getVaultTuple(arguments[0])

    # Make a description, if no description is given just use the filename
    if(len(arguments) > 2):
        description = " ".join(arguments[2:])
    else:
        description = filename

    # Check to make sure the file to upload exists
    if not os.path.isfile(filename):
        print("Invalid file name.")
        sys.exit(1)

    # Create a layer1 glacier connection
    layer1 = Layer1(aws_access_key_id=access_key_id, aws_secret_access_key=secret_key, region_name=datacenter)

    # Check to make sure the vault exists
    if not is_valid_vault(vault, datacenter, layer1):
        print("Error: Not a valid vault name")
        sys.exit(1)

    archive_id = upload_file(filename, description, vault, layer1)  

    # Print out a csv line that matches the uploaded file.
    print("%s, %s, %s, %s, %s, %s"%(str(date.today()),datacenter, vault, archive_id, filename, description))

def downloadCommand(arguments):
    datacenter, vault = getVaultTuple(arguments[0])
    archiveID = arguments[1]

    outputfile = arguments[2]

    # Create a layer1 glacier connection
    layer1 = Layer1(aws_access_key_id=access_key_id, aws_secret_access_key=secret_key, region_name=datacenter)

    # Check to make sure the vault exists
    if not is_valid_vault(vault, datacenter, layer1):
        print("Error: Not a valid vault name")
        sys.exit(1)

    layer2 = boto.glacier.connect_to_region(region_name=datacenter, aws_access_key_id=access_key_id, aws_secret_access_key=secret_key)

    download_archive(archiveID, vault, outputfile, layer2)




def upload_file(filename, description, vault, layer1):
    uploader = ConcurrentUploader(layer1, vault, 32*1024*1024)
    archive_id = uploader.upload(filename, description)
    return archive_id

def download_archive(archiveID, vault, outputfile, layer2):
    
    vault_object = layer2.get_vault(vault)

    job = vault_object.retrieve_archive(archiveID, description="Retreiving archive")
    job_id = job.id

    print(job_id)
    
    while not job.completed:
        job = vault_object.get_job(job_id)
        time.sleep(job_check_delay)
    else:
        downloader = ConcurrentDownloader(job)
        downloader.download(outputfile)


def main(argv):

    if( len(argv) < 3 ):
        # A command is a least two arguments
        print_usage(argv[0])
        sys.exit(1)
    
    command = argv[1]


    if(command == "upload"):
        if( len(argv) < 5 ):
            # upload requires at least the vault and a filename
            print_usage(argv[0])
            sys.exit(1)
        uploadCommand(argv[2:])
    elif(command == "download"):
        if( len(argv) < 5 ):
            print_usage(argv[0])
            sys.exit(1);
        downloadCommand(argv[2:])
    else:
        print_usage(argv[0])
        sys.exit(1)

if __name__ == "__main__":
    main(sys.argv)
