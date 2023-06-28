ARG OWNER=jupyter
ARG BASE_CONTAINER=$OWNER/scipy-notebook
FROM $BASE_CONTAINER

# Fix: https://github.com/hadolint/hadolint/wiki/DL4006
# Fix: https://github.com/koalaman/shellcheck/wiki/SC3014
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER root

# Spark dependencies
# Default values can be overridden at build time
# (ARGS are in lower case to distinguish them from ENV)
ARG spark_version="3.4.1"
ARG hadoop_version="3"
ARG scala_version="2.13"
ARG openjdk_version="17"

ENV APACHE_SPARK_VERSION="${spark_version}" \
    HADOOP_VERSION="${hadoop_version}"

RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    "openjdk-${openjdk_version}-jre-headless" \
    ca-certificates-java && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Spark installation
WORKDIR /tmp

# You need to use https://archive.apache.org/dist/ website if you want to download old Spark versions
# But it seems to be slower, that's why we use recommended site for download
RUN if [ -z "${scala_version}" ]; then \
    wget --progress=dot:giga -O "spark.tgz" "https://dlcdn.apache.org/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"; \
  else \
    wget --progress=dot:giga -O "spark.tgz" "https://dlcdn.apache.org/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${scala_version}.tgz"; \
  fi && \
  tar xzf "spark.tgz" -C /usr/local --owner root --group root --no-same-owner && \
  rm "spark.tgz"

# Configure Spark
ENV SPARK_HOME=/usr/local/spark
ENV SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info" \
    PATH="${PATH}:${SPARK_HOME}/bin"

RUN if [ -z "${scala_version}" ]; then \
    ln -s "spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "${SPARK_HOME}"; \
  else \
    ln -s "spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${scala_version}" "${SPARK_HOME}"; \
  fi && \
  # Add a link in the before_notebook hook in order to source automatically PYTHONPATH && \
  mkdir -p /usr/local/bin/before-notebook.d && \
  ln -s "${SPARK_HOME}/sbin/spark-config.sh" /usr/local/bin/before-notebook.d/spark-config.sh

# Configure IPython system-wide
COPY ipython_kernel_config.py "/etc/ipython/"
RUN fix-permissions "/etc/ipython/"

USER ${NB_UID}

# Install pyarrow
RUN mamba install --yes \
    'pyarrow' && \
    mamba clean --all -f -y && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

WORKDIR "${HOME}"
EXPOSE 4040

# Install nb_conda_kernels https://z2jh.jupyter.org/en/stable/jupyterhub/customizing/user-environment.html#allow-users-to-create-their-own-conda-environments-for-notebooks
RUN mamba install --yes \
    'nb_conda_kernels' 'sparkmagic' && \
    mamba clean --all -f -y && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

RUN jupyter nbextension enable --py --sys-prefix widgetsnbextension 
RUN jupyter serverextension enable --py sparkmagic

COPY .condarc /home/jovyan/example-condarc
