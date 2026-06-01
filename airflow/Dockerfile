FROM astrocrpublic.azurecr.io/runtime:3.1-5

USER root
COPY --chown=astro:astro requirements.txt /requirements.txt
COPY --chown=astro:astro pipeline_observe-0.1.0-py3-none-any.whl /tmp/
RUN pip install --no-cache-dir -r /requirements.txt /tmp/pipeline_observe-0.1.0-py3-none-any.whl

USER astro
