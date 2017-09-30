import tinys3

def upload_csv_to_s3(s3_key, s3_secret, s3_endpoint, s3_bucket, local_csv_path, filetitle_s3):

    success = False
    tries = 0

    print('Uploading CSV file to S3...')

    while not success:
        tries += 1
        try:
            conn = tinys3.Connection(s3_key,
                                     s3_secret,
                                     tls=True,
                                     endpoint=s3_endpoint)

            file_to_upload = open('{0}'.format(local_csv_path), 'rb')
            print('{0}'.format(filetitle_s3))
            print('{0}\n{1}\n{2}'.format(filetitle_s3,
                                         file_to_upload,
                                         s3_bucket))
            conn.upload(filetitle_s3,
                        file_to_upload,
                        s3_bucket)
            print('Successfully uploaded CSV file to S3.')
            success = True
        except Exception as ex:
            print(ex)
            if tries >= 5:
                print('5 tries were unsuccessful, breaking process.')
                break
            else:
                print('Connection error. Trying again..')