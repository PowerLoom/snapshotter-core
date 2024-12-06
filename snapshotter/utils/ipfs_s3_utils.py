import uuid

import aioboto3
from botocore.exceptions import ClientError
from botocore.exceptions import ParamValidationError
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_exponential

from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.settings_model import IPFSS3Config


class S3UploadError(Exception):
    """Custom exception for S3 upload errors"""
    pass


def log_retry(retry_state):
    """Log retry attempts"""
    logger.warning(
        f'Retrying {retry_state.fn.__name__} (attempt {retry_state.attempt_number})',
    )


class S3Uploader:
    def __init__(self, config: IPFSS3Config):
        """
        Initialize S3Uploader with configuration

        Args:
            config (IPFSS3Config): Configuration dataclass containing S3 settings
        """
        self.config = config
        self.session = aioboto3.Session()
        self.client = None

    def _create_client(self):
        """Create a new S3 client"""
        try:
            return self.session.client(
                's3',
                endpoint_url=self.config.endpoint_url,
                aws_access_key_id=self.config.access_key,
                aws_secret_access_key=self.config.secret_key,
            )
        except Exception as e:
            logger.error('Failed to create S3 client: {}', str(e))
            raise S3UploadError(f'S3 client creation failed: {str(e)}')

    async def _ensure_client(self):
        """Ensure client exists and create if necessary"""
        if self.client is None:
            self.client = await self._create_client().__aenter__()
        return self.client

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((ClientError, ConnectionError)),
        reraise=True,
    )
    async def upload_file(
        self,
        data: bytes,
    ) -> str:
        """
        Upload file to S3 with retry logic

        Args:
            data: Dictionary data to upload
            bucket_name: Target S3 bucket
            headers: Optional metadata headers

        Returns:
            str: Content ID (CID) of uploaded file

        Raises:
            S3UploadError: If upload fails after retries
            ValueError: If input validation fails
        """
        try:
            client = await self._ensure_client()
            random_filename = f'{uuid.uuid4()}.json'
            response = await client.put_object(
                Bucket=self.config.bucket_name,
                Key=random_filename,
                Body=data,
                Metadata={
                    'cid-version': '1',
                },
            )

            cid = response['ResponseMetadata']['HTTPHeaders']['x-amz-meta-cid']
            logger.success('Successfully uploaded file {} with CID: {}', random_filename, cid)
            return cid

        except ParamValidationError as e:
            logger.error('Parameter validation error: {}', str(e))
            raise ValueError(f'Invalid parameters: {str(e)}')

        except (ClientError, ConnectionError) as e:
            logger.error('S3 operation failed: {}', str(e))
            self.client = None  # Reset client on error
            raise

        except Exception as e:
            logger.exception('Unexpected error during upload')
            self.client = None
            raise S3UploadError(f'Upload failed: {str(e)}')
