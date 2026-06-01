FROM astrocrpublic.azurecr.io/runtime:3.1-5

# requirements.txt is installed by Astro Runtime automatically; we add
# the vendored observe wheel here so DAGs can import it.
USER root
COPY --chown=astro:astro requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER astro
