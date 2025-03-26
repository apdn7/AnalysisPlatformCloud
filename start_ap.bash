#!/bin/bash

REQUIREMENTS_DIR=requirements
OSS_REQUIREMENTS_FILE=$REQUIREMENTS_DIR/oss_prod.txt
DN_REQUIREMENTS_FILE=$REQUIREMENTS_DIR/prod.txt
VERSION_FILE=VERSION
if [ "$(sed -n '3p' $VERSION_FILE)" == "OSS" ]; then
  SOURCE=OSS
else
  SOURCE=DN
fi

# this should be set after running download_uv
UV_INSTALLATION_DIR=../.cache/uv-installer
UV_BINARY=$UV_INSTALLATION_DIR/uv

# translation japanese file
TRANSLATION_FILE=ap/translations/ja/LC_MESSAGES/messages.mo

# pip cache
CACHE_PIP_DIR=../.cache/pip

# python version
PYTHON_VERSION=3.9.21

# avoid conflict with current windows env if running in the same directory
ENV_DIR=../env-linux
PYTHON_DIR=$ENV_DIR/bin/python

# oracle portable
ORACLE_PORTABLE_URL=https://download.oracle.com/otn_software/linux/instantclient/2370000/instantclient-basic-linux.x64-23.7.0.25.01.zip
ORACLE_DIR=../Oracle-Portable
ORACLE_INSTANT_ZIP=../instantclient.zip
ORACLE_INSTANT_DIR=$ORACLE_DIR/instantclient_23_7

download_oracle() {
  echo "Download Oracle Instant Client 23.7.0.25.01"
  curl -LsSf -o $ORACLE_INSTANT_ZIP "https://download.oracle.com/otn_software/linux/instantclient/2370000/instantclient-basic-linux.x64-23.7.0.25.01.zip"
  echo "Download Completed"
  echo "Unzipping..."
  unzip -o $ORACLE_INSTANT_ZIP -d $ORACLE_DIR
  sh -c "echo $ORACLE_INSTANT_DIR > /etc/ld.so.conf.d/oracle-instantclient.conf"
  ldconfig
  export LD_LIBRARY_PATH=$ORACLE_INSTANT_DIR:$LD_LIBRARY_PATH
}

download_uv() {
  mkdir -p $UV_INSTALLATION_DIR

  curl -LsSf https://astral.sh/uv/install.sh | \
  env \
  UV_INSTALL_DIR=$UV_INSTALLATION_DIR \
  UV_UNMANAGED_INSTALL=$UV_INSTALLATION_DIR \
  INSTALLER_NO_MODIFY_PATH=1 \
  sh -s -- \
  --quiet

  echo "Installed $UV_BINARY"
}

download_python() {
  if ! [ -f $UV_BINARY ]; then
    download_uv
  fi
  $UV_BINARY python \
    --python-preference only-managed `# don't care system python` \
    install $PYTHON_VERSION
}

make_env() {
  # check oracle portable
  if [ ! -d "$ORACLE_INSTANT_DIR" ]; then
    download_oracle
  fi

  _=$($UV_BINARY python find $PYTHON_VERSION)
  # check if the previous command return error
  if [[ $? -ne 0 ]]; then
    download_python
  fi

  # check python virtual env
  if [ ! -d "$ENV_DIR/bin" ]; then
    echo "$ENV_DIR does not exist."
    $UV_BINARY venv $ENV_DIR --python $PYTHON_VERSION
    echo "Created $ENV_DIR"
  fi

  # set default python
  export PATH="$ENV_DIR/bin:${PATH}"
}

install_necessary_components() {
  if ! dpkg -l | grep -q "curl" || ! dpkg -l | grep -q "libaio1" || ! dpkg -l | grep -q "gcc" || ! dpkg -l | grep -q "libpq-dev"; then
    apt update
    apt install curl -y
    apt install unzip -y
    apt install libaio1 -y
    apt install gcc -y
    apt install libpq-dev -y
  fi
}

install_libraries() {
  make_env

  # install libraries
  $UV_BINARY pip install --upgrade pip --cache-dir=$CACHE_PIP_DIR
  if [ $SOURCE == OSS ]; then
    $UV_BINARY pip install -r $OSS_REQUIREMENTS_FILE --cache-dir=$CACHE_PIP_DIR
  else
    $UV_BINARY pip install -r $DN_REQUIREMENTS_FILE --cache-dir=$CACHE_PIP_DIR
  fi

  # activate python virtual env
  source $ENV_DIR/bin/activate
  echo "Activated $ENV_DIR."
}

setup_timezone() {
  # Set timezone
  if [ ! -z "$TZ" ]; then
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
    echo "Set $TZ timezone."
  fi
}

generate_i18n() {
  if ! [ -f $TRANSLATION_FILE ]; then
    pybabel update -i lang/message.pot -N --omit-header -d ap/translations
    pybabel compile -f -d ap/translations
    echo "Generated translations."
  fi
}

setup_timezone

install_necessary_components

install_libraries

generate_i18n

$PYTHON_DIR main.py
