import boto3, json, redis, constants

def store_in_redis(redis, file_data_json):
    for hash_key in file_data_json:
        for map_key in file_data_json[hash_key]:
            print ('Consider key : {} set with value: {}'.format(map_key,file_data_json[hash_key][map_key]))
            # redis.hset(hash_key, map_key, file_data_json[hash_key][map_key])
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
    print ('I am in action!')

    # initiate s3
    s3 = boto3.client(
                        's3'
                        # aws_access_key_id='ASIASWRVTV73PX36M6K2',
                        # aws_secret_access_key='2R2whHqF4YoLNhR91fVNApoeXUdJK2ouitItJzFH',
                        # aws_session_token='FwoGZXIvYXdzEI///////////wEaDJ4Ym0tZ/kuZfzHS+CLiAiwTpcg5B/c78xl1lHKSXHT09xwXRH31+4Q2H7Ac7Y9/NdxW66DWGbpa9Dox8cTbEj4hZlxA/IVtb3HstuGhlzcP/AAEQ2fdmFTtzlHpeJ/ZPm/i4JJOmsj375jFlLtewsx0+TGjBWm5c3MZyQwB9zJnmU3mvXCH5LrZv809MLkwULWzoNwjWIgdiaKWVcq0Ptgt+u+lI84KmkLIoqAHjnAsI01FsdLBgl7XhZCnjyxEx+xChwBurnPd/RMY6kvc7mXWPZ5LsQ0rZmMADtiNTqW7K45Ux62tN+lL+8VwFP7JmE7VSsSEi1XBMliZZJGaXEbNnkRD+WgNclWWosb/FOROq4PDMHUjjJIijWOQ0swuVJ2jVXlAk49shYvbF0pD+vd073wvDl3+AaYQOLr8PAiwtbEaNp2YbKiK41hcN3zueRkXpZ7Zb95mbfVnbXW0parmA/s+ZwhQsY6++wAAz5sTiijDpJr8BTIjhmxwPDVXCHrf2y3+t70w7kdST/8C4PIdWjnorwhiIuQtvnk=+btyw98ecsbL4BKopaZU2TFrWmJaQz5aUZdI2u1oUua5GtwvLH+AINy6dZOEPnPNGG/lMHBSHewy+uM4UjmjrE60gS/DyIoN2wcxenYJ1/wTXrYkhDx9X4Yj/e5H3ohPX9PVKI1OiPMAy6env8OJ3qhFSL81BFPQ4i4M1xscyvJluwPzKPB26UFGjH5AMGz57jFGLE+ehJecKtjr53h1vkQ/wCpDKZaYNhStEIi9GuDcwZgBQIgClFVji1tQZwBpyOAHhgPF9OqUi4EtqTqVIwOmqlstDbYqCZkcgZ78NPC/h7L52r2aW4dEKfL1+IRGJH307Es0dvqsU/ySHI+VcAq/2LdggnpPIba+7hys3yIK3yx/tuFv2/Arxs8QhhXb5Y3xfxJJ2OD7R1xbtjODRTjSzxOcnH57u8jtse30UKPW/I/yiYjZb8BTIjkGiL4OwM7T+MahB6qBAmQcPe3HKKmruQesWvgvZsshXZqdY='
    )

    # create redis connection to redis server
    # redis = redis_connection()

    # Get all file names.
    files = s3.list_objects_v2(Bucket=constants.BUCKET, StartAfter=constants.STARTAFTER, Prefix=constants.S3_PREFIX)
    if 'Contents' not in files:
        print ('No files present at the location: ', constants.STARTAFTER)
        exit (1)
    file_names = files['Contents']
    
    # Process the files. ie get file -> read data -> store in redis -> delete/move file to new folder
    process_files(s3, redis, file_names)

if __name__ == "__main__":
    main()