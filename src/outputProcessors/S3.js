import _ from 'lodash';
import md5 from 'md5';
import Promise from 'bluebird';
import Path from 'path';
import OS from 'os';
import FS from 'fs';
import {ArgBuilder} from '../pipeline/PipelineBuilder';
import SplitWritable from '../writables/SplitWritable';
import * as FileOutput from './File';
import JsonToStringTransform from '../transforms/JsonToStringTransform';
import AWS from 'aws-sdk';

export const name = 'file';

export const supportsSplit = () => true;

export const defaultArgs = () => ({
    outputS3Bucket: new ArgBuilder('outputS3Bucket')
      .required()
      .description('S3 bucket')
      .build(),
    outputAwsAccessKeyId: new ArgBuilder('outputAwsAccessKeyId') // AWS_ACCESS_KEY_ID
      .description('AWS Access Key Id, can also be loaded via environment variable AWS_ACCESS_KEY_ID')
      .build(),
    outputAwsSecretAccessKey: new ArgBuilder('outputAwsSecretAccessKey') // AWS_SECRET_ACCESS_KEY
      .description('AWS Secret Access Key, can also be loaded via environment variable AWS_SECRET_ACCESS_KEY')
      .build(),
    outputAwsRegion: new ArgBuilder('outputAwsRegion') // AWS_SECRET_ACCESS_KEY
      .description('AWS Output Region, can also be loaded via environment variable AWS_REGION')
      .build(),
    outputFile: new ArgBuilder('outputFile')
      .required()
      .description('File path for output')
      .build(),
    outputJson: new ArgBuilder('outputJson')
      .boolean()
      .description('Whether JSON or TEXT output')
      .build()
});

function uploadFileToS3(inputFile, s3FileName, params) {
    return new Promise((resolve, reject) => {
        const config = {};
        if (params.outputAwsAccessKeyId && params.outputAwsSecretAccessKey) {
            config.credentials = new AWS.Credentials({
                accessKeyId: params.outputAwsAccessKeyId,
                secretAccessKey: params.outputAwsSecretAccessKey
            });
        }

        if (params.outputAwsRegion) {
            config.region = params.outputAwsRegion;
        }

        const s3Client = new AWS.S3({
            computeChecksums: true,
            maxRetries: 3
        });

        const uploadParams = {Bucket: params.outputS3Bucket, Key: s3FileName, Body: FS.createReadStream(inputFile)};
        const uploadOptions = {partSize: 10 * 1024 * 1024, queueSize: 1};
        s3Client.upload(uploadParams, uploadOptions, (err, data) => {
            console.log('Upload response: ', err, data);
            if (err) {
                reject(err);
                return;
            }

            resolve(data);
        });
    });
}

// TODO: support gzip
export function outputProcessor(key, stream, params) {
    if (!params.outputFile) {
        throw new Error(`S3 Output: At ${key} s3 output file not defined`);
    }

    if (!params.outputS3Bucket) {
        throw new Error(`S3 Output: At ${key} s3 output bucket not defined`);
    }

    let finalStream = null;

    // const s3UploadPromises = [];

    const fileUploads = [];

    if (params._splitter) {
        const splitWritable = new SplitWritable(key,
          !!params.outputJson,
          params.encoding || 'utf8',
          params._splitter,
          (splitId) => {
              const s3FileName = _.template(FileOutput.attachSplitId(params.outputFile, splitId))(FileOutput.fileNameTemplateContext(splitId));
              const tempFileName = Path.join(OS.tmpdir(), `tmp-${md5(s3FileName)}`);

              fileUploads.push({s3FileName, tempFileName});

              console.log(`For s3 file ${s3FileName} writing first to temporary file ${tempFileName}`);

              const writeStream = FS.createWriteStream(tempFileName, {flags: 'a', autoClose: true});

              // writeStream.on('finish', () => {
              //     console.log('Finishing write stream: ', s3FileName);
              //     s3UploadPromises.push(uploadFileToS3(tempFileName, s3FileName, params));
              // });

              if (params.outputJson) {
                  const inputStream = new JsonToStringTransform();

                  inputStream.pipe(writeStream);

                  return inputStream;
              }

              return writeStream;
          });

        finalStream = stream.pipe(splitWritable);
    } else {
        const s3FileName = _.template(params.outputFile)(FileOutput.fileNameTemplateContext());
        const tempFileName = Path.join(OS.tmpdir(), `tmp-${md5(s3FileName)}`);

        fileUploads.push({s3FileName, tempFileName});

        if (params.outputJson) {
            stream = stream.pipe(new JsonToStringTransform());
        }

        console.log(`For s3 file ${s3FileName} writing first to temporary file ${tempFileName}`);

        const writeStream = FS.createWriteStream(tempFileName, {flags: 'a', autoClose: true});

        finalStream = stream.pipe(writeStream);

        // writeStream.on('finish', () => {
        //     s3UploadPromises.push(uploadFileToS3(tempFileName, s3FileName, params));
        // });
    }

    finalStream.on('finish', () => {
        Promise.mapSeries(fileUploads, (fileUpload) => uploadFileToS3(fileUpload.tempFileName, fileUpload.s3FileName, params))
          .then(() => params.resolve(true));
    });

    return finalStream;
}

export function builder(buildKey, settings) {
    return {
        settings,
        outputProcessor
    };
}