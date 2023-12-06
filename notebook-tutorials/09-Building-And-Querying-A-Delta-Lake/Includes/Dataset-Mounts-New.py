# Databricks notebook source

ACCESS_KEY_ID = "AKIAJBRYNXGHORDHZB4A"
SECRET_ACCESS_KEY = "a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF" 

# COMMAND ----------

bucket = 'databricks-corp-training/common'
mount_folder = '/mnt/training'

try:
    dbutils.fs.ls(mount_folder)
    print('Datasets are already mounted')
except:
    dbutils.fs.mount("s3a://"+ ACCESS_KEY_ID + ":" + SECRET_ACCESS_KEY + "@" + bucket,mount_folder)
    print('Datasets are mounted')

# display(dbutils.fs.mounts())
