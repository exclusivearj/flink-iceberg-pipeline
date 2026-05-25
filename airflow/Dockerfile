FROM quay.io/astronomer/astro-runtime:10.5.0

# requirements.txt is installed by Astro Runtime automatically; we add
# the vendored sentinel wheel here so DAGs can import it.
USER root
COPY --chown=astro:astro requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER astro
