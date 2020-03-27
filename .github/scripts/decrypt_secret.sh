#!/bin/sh

# Encrypt command and input passphrase
# gpg --symmetric --cipher-algo AES256 snowflake.travis.json

# Decrypt the file
# mkdir $HOME/secrets
echo Usage: decrypt_secret.sh output_file_name decrypted_file_name
# --batch to prevent interactive command --yes to assume "yes" for questions
gpg --quiet --batch --yes --decrypt --passphrase="$SNOWFLAKE_TEST_CONFIG_SECRET" --output $1 $2
