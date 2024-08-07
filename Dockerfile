FROM condaforge/mambaforge
RUN groupadd -g 1000 valankar
RUN useradd -u 1000 -g 1000 -d /home/valankar valankar
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata curl
RUN mamba update -n base --all -y
USER valankar
WORKDIR /home/valankar/code/accounts
COPY environment*.yml .
RUN mamba env create -f environment.yml -p ${HOME}/miniforge3/envs/investing
RUN mamba env create -f environment-ledger.yml -p ${HOME}/miniforge3/envs/ledger
RUN mamba clean -a -y
CMD ["mamba", "run", "-p", "/home/valankar/miniforge3/envs/investing", "--no-capture-output", "gunicorn", "dashboard:server", "-b", "0.0.0.0:8050", "-t", "60"]
EXPOSE 8050/tcp
