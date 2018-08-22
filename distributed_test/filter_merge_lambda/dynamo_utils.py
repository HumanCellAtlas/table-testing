"""Implement a Lock object using DynamoDB. Can be used to acquire locks in distributed
environments like multiple independent lambdas.
"""
import datetime
import time
import uuid

import boto3
import botocore


class Lock(object):
    """Implement a lock with DynamoDB."""

    timestamp_fmt = "%Y-%m-%dT%H:%M:%S.%fZ"

    def __init__(self, lock_table_name, lock_key, expiration_in_ms=None):
        """Init the lock object.

        Args:
          lock_table_name: Name of the table that manages the locks. This must
            be the same for all lock clients.
          lock_key: String representing the resource associated with the lock. For
            example, if you're locking an S3 object, this would be the bucket/key
            for the object.
          expiration_in_ms: Optionally set an expiration time for the lock. This
            is helpful the the acquirer dies gracelessly, for example.
        """
        self._lock_table_name = lock_table_name
        self._lock_key = lock_key
        self._expiration_in_ms = expiration_in_ms
        self._lock_id = str(uuid.uuid4())

    def __enter__(self):
        """Support `with DynamoLock(...)`"""
        self.acquire()
        return self

    def __exit__(self, *args):
        self.release()

    def _get_boto3_table(self):
        """Return the boto3 Table object for the lock table."""
        return boto3.resource("dynamodb").Table(self._lock_table_name)

    def _expiration_time(self):
        """Return a timestamp for the expiration of a lock."""
        if not self._expiration_in_ms:
            return "no expiration"

        return (datetime.datetime.utcnow() +
                datetime.timedelta(milliseconds=self._expiration_in_ms)).strftime(
                    self.timestamp_fmt)

    def _has_expired(self, expiration_timestamp):

        if expiration_timestamp == "no expiration":
            return False

        expiration_time = datetime.datetime.strptime(
            expiration_timestamp, self.timestamp_fmt)

        return expiration_time < datetime.datetime.utcnow()

    def acquire(self):
        """Acquire the lock."""

        lock_table = self._get_boto3_table()

        while True:

            db_response = lock_table.get_item(
                Key={"LockKey": self._lock_key},
                ConsistentRead=True
            )

            # If the lock key doesn't exist, then we're free to try to acquire
            # the lock.
            if "Item" not in db_response:
                try:
                    lock_table.put_item(
                        Item={
                            "LockKey": self._lock_key,
                            "LockHolder": self._lock_id,
                            "ExpirationTime": self._expiration_time()

                        },
                        ConditionExpression="attribute_not_exists(LockKey)"
                    )
                    return
                # If this didn't work, someone else got the lock first.
                except botocore.exceptions.ClientError as exc:
                    if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                        pass
                    else:
                        raise
            # If the lock has expired, we can also try to acquire it.
            elif self._has_expired(db_response["Item"]["ExpirationTime"]):
                try:
                    lock_table.update_item(
                        Key={"LockKey": self._lock_key},
                        UpdateExpression="SET LockHolder = :n, Expiration = :e",
                        ConditionExpression="LockHolder = :f",
                        ExpressionAttributeValues={":n": self._lock_id,
                                                   ":e": self._expiration_time(),
                                                   ":f": db_response["Item"]["LockHolder"]}
                    )
                    return
                except botocore.exceptions.ClientError as exc:
                    if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                        pass
                    else:
                        raise
            # Or maybe we hold the lock ourselves, then just return
            elif db_response["Item"]["LockHolder"] == self._lock_id:
                return

            # Chill out for a bit
            time.sleep(.5)
            print("Waiting to acquire lock")

    def release(self):
        """Release the lock.

        If it turns out we don't hold the lock, raise ClientError.
        """
        lock_table = self._get_boto3_table()

        lock_table.delete_item(
            Key={"LockKey": self._lock_key},
            ConditionExpression="LockHolder = :i",
            ExpressionAttributeValues={":i": self._lock_id}
        )


def increment_field(table_name, key_dict, field_name, increment_size):
    """Increment a value in a dynamo table safely.

    Makes sure distributed table updates don't clobber each other. For example,

    increment_field("my_table", {"id": id_}, "Counts", 5)

    will increment the Counts value in the item keyed by {"id": id_} in table
    "my_table" by 5.

    Args:
      table_name: Name of the dynamodb table with the value to update
      key_dict: Dict for the key in the table
      field_name: Name of the field to increment
      increment_size: Amount by which to increment the field.

    Returns:
      start_value, end_value: The values before and after incrementing
    """

    table = boto3.resource("dynamodb").Table(table_name)

    while True:
        db_response = table.get_item(
            Key=key_dict,
            ConsistentRead=True
        )

        if "Item" not in db_response:
            raise RuntimeError(f"Key {key_dict} not found in {table_name}.")

        item = db_response["Item"]
        try:
            start_value = item[field_name]
        except KeyError:
            raise RuntimeError(f"Item {item} has no field {field_name}")

        # If there's no increment, don't both updating the table
        if not increment_size:
            return start_value, start_value

        new_value = start_value + increment_size

        try:
            table.update_item(
                Key=key_dict,
                UpdateExpression=f"SET {field_name} = :n",
                ConditionExpression=f"{field_name} = :s",
                ExpressionAttributeValues={":n": new_value, ":s": start_value}
            )
            break  
        except botocore.exceptions.ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                pass
            else:
                raise
        time.sleep(.5)

    return start_value, new_value
