FROM jupyter/all-spark-notebook

ARG dev_mode=false

USER root

# This is needed because requests-kerberos fails to install on debian due to missing linux headers
RUN conda install requests-kerberos -y

USER $NB_USER

RUN pip install --upgrade pip
RUN pip install --upgrade --ignore-installed setuptools
RUN pip3 install --user --upgrade jupyter boto3 aws-glue-sessions

COPY examples /home/jovyan/work

# Install sparkmagic - if DEV_MODE is set, use the one in the host directory.
# Otherwise, just install from pip.
COPY hdijupyterutils hdijupyterutils/
COPY autovizwidget autovizwidget/
COPY sparkmagic sparkmagic/

USER root
RUN chown -R $NB_USER .

USER $NB_USER
RUN if [ "$dev_mode" = "true" ]; then \
      cd hdijupyterutils && pip install -e . && cd ../ && \
      cd autovizwidget && pip install -e . && cd ../ && \
      cd sparkmagic && pip install -e . && cd ../ ; \
    else pip install sparkmagic ; fi

RUN mkdir /home/$NB_USER/.sparkmagic
COPY sparkmagic/example_config.json /home/$NB_USER/.sparkmagic/config.json
RUN sed -i 's/localhost/spark/g' /home/$NB_USER/.sparkmagic/config.json
RUN jupyter nbextension enable --py --sys-prefix widgetsnbextension
RUN jupyter-kernelspec install --user $(pip show sparkmagic | grep Location | cut -d" " -f2)/sparkmagic/kernels/sparkkernel
RUN jupyter-kernelspec install --user $(pip show sparkmagic | grep Location | cut -d" " -f2)/sparkmagic/kernels/pysparkkernel
RUN jupyter-kernelspec install --user $(pip show sparkmagic | grep Location | cut -d" " -f2)/sparkmagic/kernels/sparkrkernel

RUN jupyter serverextension enable --py sparkmagic

# https://aws.amazon.com/blogs/big-data/introducing-aws-glue-interactive-sessions-for-jupyter/
RUN jupyter-kernelspec install --user $(pip3 show aws-glue-sessions | grep Location | awk '{print $2}')/aws_glue_interactive_sessions_kernel/glue_pyspark
RUN jupyter-kernelspec install --user $(pip3 show aws-glue-sessions | grep Location | awk '{print $2}')/aws_glue_interactive_sessions_kernel/glue_spark

USER root
RUN chown $NB_USER /home/$NB_USER/.sparkmagic/config.json

CMD ["start-notebook.sh", "--NotebookApp.iopub_data_rate_limit=1000000000"]

USER $NB_USER
