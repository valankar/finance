FROM condaforge/mambaforge
RUN groupadd -g 1000 valankar
RUN useradd -u 1000 -g 1000 -d /home/valankar valankar
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata
USER valankar
WORKDIR /home/valankar/code/accounts
COPY environment*.yml .
RUN mamba env create -f environment.yml -p ${HOME}/miniforge3/envs/investing
RUN mamba env create -f environment-firefox.yml -p ${HOME}/miniforge3/envs/firefox
RUN mamba env create -f environment-ledger.yml -p ${HOME}/miniforge3/envs/ledger
CMD ["mamba", "run", "-p", "/home/valankar/miniforge3/envs/investing", "--no-capture-output", "gunicorn", "dashboard:server", "-b", "0.0.0.0:8050", "-w", "2"]
EXPOSE 8050/tcp
