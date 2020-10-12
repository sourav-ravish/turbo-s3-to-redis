import boto3, json, redis, constants

def store_in_redis(redis, file_data_json):
    for hash_key in file_data_json:
        for map_key in file_data_json[hash_key]:
            redis.hset(hash_key, map_key, file_data_json[hash_key][map_key])
    return
    

def redis_connection():
    print("UEBA: Redis Connection Information : {}, {}".format(constants.HOST, constants.PORT))
    return redis.Redis(host=constants.HOST,
                        port=constants.PORT,
                        decode_responses=True)

def process_files(s3, redis, file_names):
    for file in file_names:
        print (file['Key'])
        if 'done/' not in file['Key']:        
            # get object
            file_content = s3.get_object(Bucket=constants.BUCKET, Key=file['Key'])
            
            # get file data in JSON
            file_data_json = json.loads(file_content['Body'].read().decode('utf-8'))

            # store in redis
            store_in_redis(redis, file_data_json)

            # copy file in done folder
            copy_source = {
                'Bucket': constants.BUCKET,
                'Key': file['Key']
            }
            file_name = file['Key'].split('/')[2]
            key_to_copy_to = 'ueba_test/done/'+file_name
            res = s3.copy_object(CopySource=copy_source, Bucket=constants.BUCKET, Key=key_to_copy_to)

            # delete/move the file
            status = s3.delete_object(Bucket=constants.BUCKET, Key=file["Key"])
            if res['ResponseMetadata']['HTTPStatusCode'] == 200 and status['DeleteMarker']:
                print('File {} processed and moved to {}'.format(file['Key'], key_to_copy_to))

def main():
    # initiate s3
    s3=boto3.client('s3')

    # create redis connection to redis server
    redis = redis_connection()

    # Get all file names.
    file_names = s3.list_objects_v2(Bucket=constants.BUCKET, StartAfter=constants.STARTAFTER, Prefix=constants.S3_PREFIX)['Contents']
    
    # Process the files. ie get file -> read data -> store in redis -> delete/move file to new folder
    process_files(s3, redis, file_names)

if __name__ == "__main__":
    main()