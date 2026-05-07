#!/bin/sh

# Below is encrypt command. It needs to input passphrase in promot.
# gpg --symmetric --cipher-algo AES256 snowflake.travis.json

# Decrypt the file
echo "Usage: decrypt_secret.sh output_file_name decrypted_file_name"
echo "       Note: environment variable SNOWFLAKE_TEST_CONFIG_SECRET should be set for descryption."

# --batch to prevent interactive command --yes to assume "yes" for questions
gpg --quiet --batch --yes --decrypt --passphrase="$SNOWFLAKE_TEST_CONFIG_SECRET" --output $1 $2

